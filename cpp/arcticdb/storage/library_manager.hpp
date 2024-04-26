/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <vector>
#include <memory>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/storage_override.hpp>

namespace pybind11 {
    class object;
}

namespace arcticdb {
    class Store;
}

namespace arcticdb::storage {
    class Library;
    class LibraryManager {
    public:
        explicit LibraryManager(const std::shared_ptr<storage::Library>& library);

        ARCTICDB_NO_MOVE_OR_COPY(LibraryManager)

        void write_library_config(const pybind11::object& lib_cfg, const LibraryPath& path, const StorageOverride& storage_override,
                                  bool validate) const;

        [[nodiscard]] pybind11::object get_library_config(const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}) const;

        [[nodiscard]] bool is_library_config_ok(const LibraryPath& path, bool throw_on_failure) const;

        void remove_library_config(const LibraryPath& path) const;

        [[nodiscard]] std::shared_ptr<Library> get_library(
                const LibraryPath& path,
                const StorageOverride& storage_override,
                bool ignore_cache);

        void cleanup_library_if_open(const LibraryPath& path);

        [[nodiscard]] std::vector<LibraryPath> get_library_paths() const;

        [[nodiscard]] bool has_library(const LibraryPath& path) const;

    private:
        [[nodiscard]] arcticdb::proto::storage::LibraryConfig get_config_internal(const LibraryPath& path, const StorageOverride& storage_override) const;

        std::shared_ptr<Store> store_;
        std::unordered_map<LibraryPath, std::shared_ptr<Library>> open_libraries_;
        std::mutex open_libraries_mutex_;  // for open_libraries_
    };
}
