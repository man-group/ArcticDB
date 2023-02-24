/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <folly/gen/Base.h>
#ifndef ARCTICDB_MEMORY_STORAGE_H_
#error "This should only be included by memory_storage.hpp"
#endif

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/op_contexts.hpp>

namespace arcticdb::storage::memory {

    namespace fg = folly::gen;

    inline void MemoryStorage::do_write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(MemoryStorageWrite, 0)
        std::lock_guard lock{*mutex_};

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &kv : group.values()) {
                util::variant_match(kv.variant_key(),
                                    [&](const RefKey &key) {
                                        key_vec[key] = kv.segment();
                                    },
                                    [&](const AtomKey &key) {
                                        util::check(key_vec.find(key) == key_vec.end(),
                                                    "Cannot replace atom key in in-memory storage");

                                        key_vec[key] = kv.segment();
                                    }
                );
            }
        });
    }

    inline void MemoryStorage::do_update(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(MemoryStorageUpdate, 0)
        std::lock_guard lock{*mutex_};

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        op_ctx::OpContext<op_ctx::UpdateOpts> opts = op_ctx::OpContext<op_ctx::UpdateOpts>::get();

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &kv : group.values()) {
                auto it = key_vec.find(kv.variant_key());

                util::check_rte(opts->upsert_ || it != key_vec.end(), "update called with upsert=false but key does not exist");

                if(it != key_vec.end()) {
                    key_vec.erase(it);
                }
                key_vec.insert(std::make_pair(kv.variant_key(), kv.segment()));
            }
        });
    }

    template<class Visitor>
    void MemoryStorage::do_read(Composite<VariantKey>&& ks, Visitor &&visitor) {
        ARCTICDB_SAMPLE(MemoryStorageRead, 0)
        std::lock_guard lock{*mutex_};
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];
            for (auto &k : group.values()) {
                auto it = key_vec.find(k);

                if(it != key_vec.end()) {
                    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k), variant_key_view(k));
                    auto seg = it->second;
                    visitor(k, seg);
                } else {
                    throw KeyNotFoundException(std::move(ks));
                }
            }
        });
    }

inline bool MemoryStorage::do_key_exists(const VariantKey& key) {
        ARCTICDB_SAMPLE(MemoryStorageKeyExists, 0)
        std::lock_guard lock{*mutex_};
        auto& key_vec = data_[variant_key_type(key)];
        auto it = key_vec.find(key);
        return it != key_vec.end();
    }

    inline void MemoryStorage::do_remove(Composite<VariantKey>&& ks)
    {
        using namespace arcticdb::storage::op_ctx;

        ARCTICDB_SAMPLE(MemoryStorageRemove, 0)
        std::lock_guard lock{*mutex_};
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };
        auto flags = OpContext<RemoveOpts>::get();

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &k : group.values()) {
                auto it = key_vec.find(k);

                if(it != key_vec.end()) {
                    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}, with {} bytes of data", variant_key_type(k), variant_key_view(k));
                    key_vec.erase(it);
                } else if (!flags.ignores_missing_key) {
                    util::raise_rte("Failed to find segment for key {}",variant_key_view(k));
                }
            }
        });
    }

    bool MemoryStorage::do_fast_delete() {
        return false;
    }

    template<class Visitor>
    void MemoryStorage::do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &/*prefix*/) {
        ARCTICDB_SAMPLE(MemoryStorageItType, 0)
        std::lock_guard lock{*mutex_};
        auto& key_vec = data_[key_type];
        for(auto& key_value : key_vec) {
            auto key = key_value.first;
            visitor(std::move(key));
        }
    }
}
