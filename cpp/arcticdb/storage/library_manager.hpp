/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/storage_override.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <folly/Range.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <boost/core/noncopyable.hpp>
#include <filesystem>

namespace arcticdb::storage {
    class LibraryManager {
    public:
        explicit LibraryManager(const std::shared_ptr<storage::Library>& library) :
            store_(std::make_shared<async::AsyncStore<util::SysClock>>(library, codec::default_lz4_codec(), encoding_version(library->config()))){
        }

        void write_library_config(const py::object& lib_cfg, const LibraryPath& path) const {
            SegmentInMemory segment;

            arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
            google::protobuf::Any output = {};
            python_util::pb_from_python(lib_cfg, lib_cfg_proto);
            output.PackFrom(lib_cfg_proto);
            segment.set_metadata(std::move(output));

            store_->write_sync(
                    entity::KeyType::                       LIBRARY_CONFIG,
                    StreamId(path.to_delim_path()),
                    std::move(segment)
            );
        }

        [[nodiscard]] py::object get_library_config(const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}) const {
            arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, storage_override);

            return arcticdb::python_util::pb_to_python(config);
        }

        void remove_library_config(const LibraryPath& path) const {
            store_->remove_key(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG}).wait();
        }

        [[nodiscard]] std::shared_ptr<Library> get_library(const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}) const {
            arcticdb::proto::storage::LibraryConfig config = get_config_internal(path, storage_override);

            std::vector<arcticdb::proto::storage::StorageConfig> storage_configs;
            for(const auto& storage: config.storage_by_id()){
                storage_configs.emplace_back(storage.second);
            }
            auto storages = create_storages(path, OpenMode::DELETE, storage_configs);

            return std::make_shared<Library>(path, std::move(storages), config.lib_desc().version());
        }

        [[nodiscard]] std::vector<LibraryPath> get_library_paths() const {
            std::vector<LibraryPath> ids;
            store_->iterate_type(entity::KeyType::LIBRARY_CONFIG, [&ids](const VariantKey &&key) {
                                     const auto& k = std::get<entity::RefKey>(key);
                                     const auto& lp = std::get<std::string>(k.id());
                                     ids.emplace_back(lp, '.');
                                 }
            );

            return ids;
        }

        [[nodiscard]] bool has_library(const LibraryPath& path) const {
            return store_->key_exists_sync(RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG});
        }

    private:
        static void apply_storage_override(const StorageOverride& storage_override, arcticdb::proto::storage::LibraryConfig& lib_cfg_proto) {
            util::variant_match(
                storage_override.variant(),
                [&lib_cfg_proto] (const S3CredentialsOverride& credentials_override) {
                    for(auto& storage: *lib_cfg_proto.mutable_storage_by_id()){
                        credentials_override.modify_storage_credentials(storage.second);
                    }
                },
                [] (const auto&) {
                    //std::monostate
                });
        }

        [[nodiscard]] arcticdb::proto::storage::LibraryConfig get_config_internal(const LibraryPath& path, const StorageOverride& storage_override) const {
            auto [key, segment_in_memory] = store_->read_sync(
                    RefKey{StreamId(path.to_delim_path()), entity::KeyType::LIBRARY_CONFIG}
            );

            auto any = segment_in_memory.metadata();
            arcticdb::proto::storage::LibraryConfig lib_cfg_proto;
            any->UnpackTo(&lib_cfg_proto);
            apply_storage_override(storage_override, lib_cfg_proto);
            return lib_cfg_proto;
        }

        std::shared_ptr<Store> store_;
    };
}
