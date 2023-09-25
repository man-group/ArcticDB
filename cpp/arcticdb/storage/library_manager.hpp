/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <vector>
#include <memory>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/storage_override.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>


namespace arcticdb::storage {
    class LibraryManager {
    public:
        explicit LibraryManager(const std::shared_ptr<storage::Library>& library);

        void write_library_config(const py::object& lib_cfg, const LibraryPath& path, const StorageOverride& storage_override,
                                  bool validate) const;

        [[nodiscard]] py::object get_library_config(const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}) const;

        [[nodiscard]] bool is_library_config_ok(const LibraryPath& path, bool throw_on_failure) const;

        void remove_library_config(const LibraryPath& path) const;

        [[nodiscard]] std::shared_ptr<Library> get_library(const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}) const;

        [[nodiscard]] std::vector<LibraryPath> get_library_paths() const;

        [[nodiscard]] bool has_library(const LibraryPath& path) const;

    private:
        [[nodiscard]] arcticdb::proto::storage::LibraryConfig get_config_internal(const LibraryPath& path, const StorageOverride& storage_override) const;

        std::shared_ptr<Store> store_;
    };
}
