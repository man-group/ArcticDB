/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/memory/memory_storage.hpp>


#include <folly/gen/Base.h>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>

namespace arcticdb::storage::memory {

    namespace fg = folly::gen;

    std::string MemoryStorage::name() const {
        return "memory_storage-0";
    }

    void MemoryStorage::do_write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(MemoryStorageWrite, 0)

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &kv : group.values()) {
                util::variant_match(kv.variant_key(),
                                    [&](const RefKey &key) {
                                        key_vec.erase(key);
                                        key_vec.try_emplace(key, kv.segment());
                                    },
                                    [&](const AtomKey &key) {
                                        key_vec.visit(key, [key](const auto&){
                                            throw DuplicateKeyException(key);
                                        });

                                        key_vec.try_emplace(key, kv.segment());
                                    }
                );
            }
        });
    }

    void MemoryStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
        ARCTICDB_SAMPLE(MemoryStorageUpdate, 0)

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &kv : group.values()) {
                auto kv_variant_key = kv.variant_key();
                key_vec.erase(kv_variant_key);

                key_vec.visit(kv_variant_key, [&kv_variant_key, &opts](const auto&){
                    if (!opts.upsert_) {
                        std::string err_message = fmt::format(
                                "do_update called with upsert=false on non-existent key(s): {}",
                                kv_variant_key
                        );
                        throw KeyNotFoundException(std::move(kv_variant_key), err_message);
                    }
                });

                key_vec.insert(std::make_pair(kv_variant_key, kv.segment()));
            }
        });
    }

    void MemoryStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts) {
        ARCTICDB_SAMPLE(MemoryStorageRead, 0)
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];
            for (auto &k : group.values()) {
                bool key_exists = false;
                key_vec.visit(k, [&visitor, &k, &key_exists](auto& pair){
                    auto & seg = pair.second;
                    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k), variant_key_view(k));
                    key_exists = true;
                    visitor(k, std::move(seg));
                });
                if (!key_exists) {
                    throw KeyNotFoundException(std::move(ks));
                }
            }
        });
    }

    bool MemoryStorage::do_key_exists(const VariantKey& key) {
        ARCTICDB_SAMPLE(MemoryStorageKeyExists, 0)
        bool key_exists = false;
        const auto& key_vec = data_[variant_key_type(key)];
        key_vec.visit(key, [&key_exists](const auto &){
            key_exists = true;
        });
        return key_exists;
    }

    void MemoryStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts)
    {
        ARCTICDB_SAMPLE(MemoryStorageRemove, 0)
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            auto& key_vec = data_[group.key()];

            for (auto &k : group.values()) {
                bool key_exists = false;
                key_vec.visit(k, [&key_exists](const auto&){
                    key_exists = true;
                });

                if (!key_exists and !opts.ignores_missing_key_) {
                        throw KeyNotFoundException(std::move(k));
                }

                ARCTICDB_DEBUG(log::storage(), "Removed key {}: {}", variant_key_type(k), variant_key_view(k));
                key_vec.erase(k);
            }
        });
    }

    bool MemoryStorage::do_fast_delete() {
        foreach_key_type([&] (KeyType key_type) {
            data_[key_type].clear();
        });
        return true;
    }

    void MemoryStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
        ARCTICDB_SAMPLE(MemoryStorageItType, 0)
        auto& key_vec = data_[key_type];
        auto prefix_matcher = stream_id_prefix_matcher(prefix);

        key_vec.visit_all([&prefix_matcher, &visitor](const auto& key_value) {
            auto key = key_value.first;

            if (prefix_matcher(variant_key_id(key))) {
                visitor(std::move(key));
            }
        });
    }

    MemoryStorage::MemoryStorage(const LibraryPath &library_path, OpenMode mode, const Config&) :
        Storage(library_path, mode) {
        arcticdb::entity::foreach_key_type([this](KeyType&& key_type) {
            data_[key_type];
        });
    }

}
