/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/variant_storage_factory.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/failure_simulation.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/composite.hpp>

#include <memory>
#include <vector>

namespace arcticdb::storage {

class Storages {
  public:
    Storages(const Storages&) = delete;
    Storages(Storages&&) = default;
    Storages& operator=(const Storages&) = delete;
    Storages& operator=(Storages&&) = delete;

    using StorageVector = std::vector<std::unique_ptr<VariantStorage>>;

    Storages(StorageVector&& variant_storages, OpenMode mode) :
        variant_storages_(std::move(variant_storages)), mode_(mode) {
    }

    void write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(StorageWrite, 0)
        primary().write(std::move(kvs));
    }

    void update(Composite<KeySegmentPair>&& kvs, storage::UpdateOpts opts) {
        ARCTICDB_SAMPLE(StorageUpdate, 0)
        primary().update(std::move(kvs), opts);
    }

    bool supports_prefix_matching() {
        return primary().supports_prefix_matching();
    }

    bool fast_delete() {
        return primary().fast_delete();
    }

    bool key_exists(const VariantKey& key) {
        return primary().key_exists(key);
    }

    template<class Visitor>
    auto read(Composite<VariantKey>&& ks, Visitor &&v, ReadKeyOpts opts, bool primary_only=true) {
        ARCTICDB_RUNTIME_SAMPLE(StorageRead, 0)
        if(primary_only)
            return primary().read(std::move(ks), std::forward<Visitor>(v), opts);

        if(auto rg = ks.as_range(); !std::all_of(std::begin(rg), std::end(rg), [] (const auto& vk) {
            return variant_key_type(vk) == KeyType::TABLE_DATA;
        })) {
            return primary().read(std::move(ks), std::forward<Visitor>(v), opts);
        }

        for(const auto& storage : variant_storages_) {
            auto visitor = std::forward<Visitor>(v);
            try {
                return storage->read(std::move(ks), Visitor{visitor}, opts);
            } catch (typename storage::KeyNotFoundException& ex) {
                ARCTICDB_DEBUG(log::version(), "Keys not found in storage, continuing to next storage");
                ks = std::move(ex.keys());
            }
        }
        throw storage::KeyNotFoundException(std::move(ks));
    }

    template<class Visitor>
    void iterate_type(KeyType key_type, Visitor &&v, const std::string &prefix=std::string{}, bool primary_only=true) {
        ARCTICDB_SAMPLE(StorageIterateType, RMTSF_Aggregate)
        if(primary_only) {
            primary().iterate_type(key_type, std::forward<Visitor>(v), prefix);
        } else {
            auto visitor = std::forward<Visitor>(v);
            for(const auto& storage : variant_storages_) {
                storage->iterate_type(key_type, std::decay_t<Visitor>{visitor}, prefix);
            }
        }
    }

    /** Calls VariantStorage::do_storage_specific on the primary storage. Remember to check the open mode. */
    template<class Visitor>
    void storage_specific(Visitor&& visitor) {
        primary().storage_specific(std::forward<Visitor>(visitor));
    }

    void remove(Composite<VariantKey>&& ks, storage::RemoveOpts opts) {
        primary().remove(std::move(ks), opts);
    }

    [[nodiscard]] OpenMode open_mode() const { return mode_; }

    void move_storage(KeyType key_type, timestamp horizon, size_t storage_index = 0) {
        util::check(storage_index + 1 < variant_storages_.size(), "Cannot move from storage {} to storage {} as only {} storages defined");
        auto& source = *variant_storages_[storage_index];
        auto& target = *variant_storages_[storage_index + 1];

        source.iterate_type(key_type, [&source, &target, horizon] (auto&& vk) {
            auto key = std::forward<VariantKey>(vk);
            if (to_atom(key).creation_ts() < horizon) {
                try {
                    auto key_seg = source.read(VariantKey{key}, ReadKeyOpts{});
                    target.write(std::move(key_seg));
                    source.remove(std::move(key), storage::RemoveOpts{});
                } catch (const std::exception& ex) {
                    log::storage().warn("Failed to move key to next storage: {}", ex.what());
                }
            } else {
                ARCTICDB_DEBUG(log::storage(), "Not moving key {} as it is too recent", key);
            }
        });
   }

  private:
    VariantStorage& primary() {
        util::check(!variant_storages_.empty(), "No storages configured");
        return *variant_storages_[0];
    }

    std::vector<std::unique_ptr<VariantStorage>> variant_storages_;
    OpenMode mode_;
};

inline std::shared_ptr<Storages> create_storages(const LibraryPath& library_path, OpenMode mode, const arcticdb::proto::storage::VariantStorage &storage_config) {
    using VariantVec = std::vector<std::unique_ptr<VariantStorage>>;
    VariantVec variants;
    variants.push_back(create_storage(library_path, mode, storage_config));

    return std::make_shared<Storages>(std::move(variants), mode);
}

inline std::shared_ptr<Storages> create_storages(const LibraryPath& library_path, OpenMode mode, const std::vector<arcticdb::proto::storage::VariantStorage> &storage_configs) {
    using VariantVec = std::vector<std::unique_ptr<VariantStorage>>;
    VariantVec variants;
    for (const auto& storage_config: storage_configs) {
        variants.push_back(create_storage(library_path, mode, storage_config));
    }
    return std::make_shared<Storages>(std::move(variants), mode);
}

} //namespace arcticdb::storage
