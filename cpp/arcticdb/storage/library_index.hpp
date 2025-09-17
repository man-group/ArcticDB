/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_cache.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/protobuf_mappings.hpp>
#include <arcticdb/storage/storages.hpp>

namespace arcticdb::storage {

class LibraryIndex {
  public:
    LibraryIndex(const EnvironmentName& environment_name, const std::shared_ptr<ConfigResolver>& resolver) :
        library_cache_(),
        config_cache_(environment_name, resolver) {
        ARCTICDB_DEBUG(log::storage(), "Creating library index with resolver type {}", resolver->resolver_type());
    }

    std::vector<LibraryPath> list_libraries(std::string_view prefix) {
        std::lock_guard<std::mutex> lock{mutex_};
        return config_cache_.list_libraries(prefix);
    }

    bool has_library(const LibraryPath& path) const {
        return library_cache_.find(path) != library_cache_.end() || config_cache_.library_exists(path);
    }

    std::shared_ptr<Library> get_library(
            const LibraryPath& path, OpenMode mode, const UserAuth&, const NativeVariantStorage& native_storage_config
    ) {
        std::lock_guard<std::mutex> lock{mutex_};
        auto res = library_cache_.find(path);
        if (res != library_cache_.end())
            return res->second;

        return get_library_internal(path, mode, native_storage_config);
    }

  private:
    std::shared_ptr<Library> get_library_internal(
            const LibraryPath& path, OpenMode mode, const NativeVariantStorage& native_storage_config
    ) {
        auto desc = config_cache_.get_descriptor(path);
        LibraryDescriptor::VariantStoreConfig cfg;
        if (desc.has_value()) {
            cfg = desc->config_;
        }
        auto lib =
                std::make_shared<Library>(path, config_cache_.create_storages(path, mode, native_storage_config), cfg);
        if (auto&& [it, inserted] = library_cache_.try_emplace(path, lib); !inserted) {
            lib = it->second;
        }
        return lib;
    }

    std::unordered_map<LibraryPath, std::shared_ptr<Library>> library_cache_;
    ConfigCache config_cache_;
    std::mutex mutex_;
};

} // namespace arcticdb::storage