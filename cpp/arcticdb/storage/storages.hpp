/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/single_file_storage.hpp>
#include <arcticdb/storage/storage.hpp>

#include <memory>
#include <vector>

namespace arcticdb::storage {

/*
 * The Storages class abstracts over multiple physical stores and controls how ArcticDB read and write to multiple
 * ones.
 *
 * Possible future use-case for Storages:
 *  - Disaster recovery for Storages: if the first storage fails we can fall back to the second one, etc.
 *  - Tiered storage: recent data goes to a fast, comparatively expensive storage and then is gradually moved
 *  into a slower, cheaper one.
 */
class Storages {
public:
    Storages(const Storages&) = delete;
    Storages(Storages&&) = default;
    Storages& operator=(const Storages&) = delete;
    Storages& operator=(Storages&&) = delete;

    using StorageVector = std::vector<std::shared_ptr<Storage>>;

    Storages(StorageVector&& storages, OpenMode mode) :
        storages_(std::move(storages)), mode_(mode) {
    }

    void write(KeySegmentPair& key_seg) {
        ARCTICDB_SAMPLE(StoragesWrite, 0)
        primary().write(key_seg);
    }

    void write_if_none(KeySegmentPair& kv) {
        primary().write_if_none(kv);
    }

    void update(KeySegmentPair& key_seg, storage::UpdateOpts opts) {
        ARCTICDB_SAMPLE(StoragesUpdate, 0)
        primary().update(key_seg, opts);
    }

    [[nodiscard]] bool supports_prefix_matching() const {
        return primary().supports_prefix_matching();
    }

    [[nodiscard]] bool supports_atomic_writes() {
        return primary().supports_atomic_writes();
    }

    [[nodiscard]] bool supports_object_size_calculation() {
        return std::all_of(storages_.begin(), storages_.end(), [](const auto& storage) {return storage->supports_object_size_calculation();});
    }

    bool fast_delete() {
        return primary().fast_delete();
    }

    void cleanup() {
        primary().cleanup();
    }

    bool key_exists(const VariantKey& key) {
        return primary().key_exists(key);
    }

    [[nodiscard]] bool is_path_valid(const std::string_view path) const {
        return primary().is_path_valid(path);
    }

    void read_sync_fallthrough(const VariantKey& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
        for (const auto& storage : storages_) {
            try {
                return storage->read(VariantKey{variant_key}, visitor, opts);
            } catch (typename storage::KeyNotFoundException&) {
                ARCTICDB_DEBUG(log::version(), "Keys not found in storage, continuing to next storage");
            }
        }
        throw storage::KeyNotFoundException(variant_key);
    }

    KeySegmentPair read_sync_fallthrough(const VariantKey& variant_key) {
        for (const auto& storage : storages_) {
            try {
                return storage->read(VariantKey{variant_key}, ReadKeyOpts{});
            } catch (typename storage::KeyNotFoundException&) {
                ARCTICDB_DEBUG(log::version(), "Keys not found in storage, continuing to next storage");
            }
        }
        throw storage::KeyNotFoundException(variant_key);
    }

    void read_sync(const VariantKey& variant_key,
                   const ReadVisitor& visitor,
                   ReadKeyOpts opts,
                   bool primary_only = true) {
        ARCTICDB_RUNTIME_SAMPLE(StoragesRead, 0)
        if (primary_only || variant_key_type(variant_key) != KeyType::TABLE_DATA)
            return primary().read(VariantKey{variant_key}, visitor, opts);

        read_sync_fallthrough(variant_key, visitor, opts);
    }

    KeySegmentPair read_sync(const VariantKey& variant_key, ReadKeyOpts opts, bool primary_only = true) {
        ARCTICDB_RUNTIME_SAMPLE(StoragesRead, 0)
        if (primary_only || variant_key_type(variant_key) != KeyType::TABLE_DATA)
            return primary().read(VariantKey{variant_key}, opts);

        return read_sync_fallthrough(variant_key);
    }

    static folly::Future<folly::Unit> async_read(Storage& storage,
                                                 VariantKey&& variant_key,
                                                 const ReadVisitor& visitor,
                                                 ReadKeyOpts opts) {
        if (storage.has_async_api()) {
            return storage.async_api()->async_read(std::move(variant_key), visitor, opts);
        } else {
            storage.read(std::move(variant_key), visitor, opts);
            return folly::makeFuture();
        }
    }

    static folly::Future<KeySegmentPair> async_read(Storage& storage, VariantKey&& variant_key, ReadKeyOpts opts) {
        if (storage.has_async_api()) {
            return storage.async_api()->async_read(std::move(variant_key), opts);
        } else {
            auto key_seg = storage.read(std::move(variant_key), opts);
            return folly::makeFuture(std::move(key_seg));
        }
    }

    folly::Future<folly::Unit> read(VariantKey&& variant_key,
                                    const ReadVisitor& visitor,
                                    ReadKeyOpts opts,
                                    bool primary_only = true) {
        ARCTICDB_RUNTIME_SAMPLE(StoragesRead, 0)
        if (primary_only || variant_key_type(variant_key) != KeyType::TABLE_DATA)
            return async_read(primary(), std::move(variant_key), visitor, opts);

        // Not supporting async fall-through at the moment as we would need to ensure that
        // visitation was idempotent. Could be achieved with a mutex/call once wrapper around
        // the visitor for simultaneous async reads, or with a window of size 1 and cancellation
        // token.
        read_sync_fallthrough(variant_key, visitor, opts);
        return folly::makeFuture();
    }

    folly::Future<KeySegmentPair> read(VariantKey&& variant_key, ReadKeyOpts opts, bool primary_only = true) {
        ARCTICDB_RUNTIME_SAMPLE(StoragesRead, 0)
        if (primary_only || variant_key_type(variant_key) != KeyType::TABLE_DATA)
            return async_read(primary(), std::move(variant_key), opts);

        // Not supporting async fall-through at the moment as we would need to ensure that
        // visitation was idempotent. Could be achieved with a mutex/call once wrapper around
        // the visitor for simultaneous async reads, or with a window of size 1 and cancellation
        // token.
        auto res = read_sync_fallthrough(variant_key);
        return folly::makeFuture(std::move(res));
    }

    void iterate_type(KeyType key_type,
                      const IterateTypeVisitor& visitor,
                      const std::string& prefix = std::string{},
                      bool primary_only = true) {
        ARCTICDB_SAMPLE(StoragesIterateType, RMTSF_Aggregate)
        if (primary_only) {
            primary().iterate_type(key_type, visitor, prefix);
        } else {
            for (const auto& storage : storages_) {
                storage->iterate_type(key_type, visitor, prefix);
            }
        }
    }

    void visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor, bool primary_only = true) {
        if (primary_only) {
            primary().visit_object_sizes(key_type, prefix, visitor);
            return;
        }

        for (const auto& storage : storages_) {
            storage->visit_object_sizes(key_type, prefix, visitor);
        }
    }

    bool scan_for_matching_key(KeyType key_type, const IterateTypePredicate& predicate, bool primary_only = true) {
        if (primary_only) {
            return primary().scan_for_matching_key(key_type, predicate);
        }

        return std::any_of(std::begin(storages_), std::end(storages_),
                           [key_type, &predicate](const auto& storage) {
                               return storage->scan_for_matching_key(key_type, predicate);
                           });
    }

    /** Calls Storage::do_key_path on the primary storage. Remember to check the open mode. */
    [[nodiscard]] std::string key_path(const VariantKey& key) const {
        return primary().key_path(key);
    }

    void remove(VariantKey&& variant_key, storage::RemoveOpts opts) {
        primary().remove(std::move(variant_key), opts);
    }

    void remove(std::span<VariantKey> variant_keys, storage::RemoveOpts opts) {
        primary().remove(variant_keys, opts);
    }

    [[nodiscard]] OpenMode open_mode() const { return mode_; }

    void move_storage(KeyType key_type, timestamp horizon, size_t storage_index = 0) {
        util::check(storage_index + 1 < storages_.size(),
                    "Cannot move from storage {} to storage {} as only {} storages defined");
        auto& source = *storages_[storage_index];
        auto& target = *storages_[storage_index + 1];

        const IterateTypeVisitor& visitor = [&source, &target, horizon](VariantKey&& vk) {
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
        };

        source.iterate_type(key_type, visitor);
    }
    [[nodiscard]] std::optional<std::shared_ptr<SingleFileStorage>> get_single_file_storage() const {
        if (dynamic_cast<SingleFileStorage *>(storages_[0].get()) != nullptr) {
            return std::dynamic_pointer_cast<SingleFileStorage>(storages_[0]);
        } else {
            return std::nullopt;
        }
    }
    [[nodiscard]] std::string name() const {
        return primary().name();
    }

private:
    Storage& primary() {
        util::check(!storages_.empty(), "No storages configured");
        return *storages_[0];
    }

    [[nodiscard]] const Storage& primary() const {
        util::check(!storages_.empty(), "No storages configured");
        return *storages_[0];
    }

    std::vector<std::shared_ptr<Storage>> storages_;
    OpenMode mode_;
};

inline std::shared_ptr<Storages> create_storages(const LibraryPath& library_path,
                                                 OpenMode mode,
                                                 decltype(std::declval<arcticc::pb2::storage_pb2::LibraryConfig>().storage_by_id())& storage_configs,
                                                 const NativeVariantStorage& native_storage_config) {
    Storages::StorageVector storages;
    for (auto& [storage_id, storage_config] : storage_configs) {
        util::variant_match(native_storage_config.variant(),
                            [&storage_config, &storages, &library_path, mode](const s3::S3Settings& settings) {
                                util::check(storage_config.config().Is<arcticdb::proto::s3_storage::Config>(),
                                            "Only support S3 native settings");
                                arcticdb::proto::s3_storage::Config s3_storage;
                                storage_config.config().UnpackTo(&s3_storage);
                                storages.push_back(create_storage(library_path,
                                                                  mode,
                                                                  s3::S3Settings(settings).update(s3_storage)));
                            },
                            [&storage_config, &storages, &library_path, mode](const s3::GCPXMLSettings& settings) {
                                util::check(storage_config.config().Is<arcticdb::proto::gcp_storage::Config>(),
                                            "Only support GCP native settings");
                                arcticdb::proto::gcp_storage::Config gcp_storage;
                                storage_config.config().UnpackTo(&gcp_storage);
                                storages.push_back(create_storage(library_path,
                                                                  mode,
                                                                  s3::GCPXMLSettings(settings).update(gcp_storage)));
                            },
                            [&storage_config, &storages, &library_path, mode](const auto&) {
                                storages.push_back(create_storage(library_path, mode, storage_config));
                            }
        );
    }
    return std::make_shared<Storages>(std::move(storages), mode);
}

inline std::shared_ptr<Storages> create_storages(const LibraryPath& library_path,
                                                 OpenMode mode,
                                                 const std::vector<arcticdb::proto::storage::VariantStorage>& storage_configs) {
    Storages::StorageVector storages;
    for (const auto& storage_config : storage_configs) {
        storages.push_back(create_storage(library_path, mode, storage_config));
    }
    return std::make_shared<Storages>(std::move(storages), mode);
}

} //namespace arcticdb::storage