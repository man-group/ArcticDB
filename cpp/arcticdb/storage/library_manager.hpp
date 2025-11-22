/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <vector>
#include <memory>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/storage_override.hpp>
#include <arcticdb/util/lru_cache.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::storage {
enum class ModifiableLibraryOption {
    DEDUP = 1,
    ROWS_PER_SEGMENT = 2,
    COLUMNS_PER_SEGMENT = 3,
    RECURSIVE_NORMALIZERS = 4
};
enum class ModifiableEnterpriseLibraryOption { REPLICATION = 1, BACKGROUND_DELETION = 2 };
using LibraryOptionValue = std::variant<bool, int64_t>;

struct UnknownLibraryOption : UserInputException {
    UnknownLibraryOption(std::string msg) : UserInputException(msg) {}
};
struct UnsupportedLibraryOptionValue : UserInputException {
    UnsupportedLibraryOptionValue(std::string msg) : UserInputException(msg) {}
};

class LibraryManager {
  public:
    struct LibraryWithConfig {
        arcticdb::proto::storage::LibraryConfig config;
        std::shared_ptr<Library> library;
    };

    explicit LibraryManager(const std::shared_ptr<storage::Library>& library);

    ARCTICDB_NO_MOVE_OR_COPY(LibraryManager)

    void write_library_config(
            const py::object& lib_cfg, const LibraryPath& path, const StorageOverride& storage_override, bool validate
    ) const;

    void modify_library_option(
            const LibraryPath& path, std::variant<ModifiableLibraryOption, ModifiableEnterpriseLibraryOption> option,
            LibraryOptionValue new_value
    ) const;

    [[nodiscard]] py::object get_library_config(
            const LibraryPath& path, const StorageOverride& storage_override = StorageOverride{}
    ) const;

    // [get_unaltered_library_config] should be used solely for tests and debugging. Hence, it's separated from
    // [get_library_config] instead of making the [storage_override] optional.
    [[nodiscard]] py::object get_unaltered_library_config(const LibraryPath& path) const;

    [[nodiscard]] bool is_library_config_ok(const LibraryPath& path, bool throw_on_failure) const;

    void remove_library_config(const LibraryPath& path) const;

    [[nodiscard]] LibraryWithConfig get_library(
            const LibraryPath& path, const StorageOverride& storage_override, bool ignore_cache,
            const NativeVariantStorage& native_storage_config
    );

    void cleanup_library_if_open(const LibraryPath& path);

    [[nodiscard]] std::vector<LibraryPath> get_library_paths() const;

    [[nodiscard]] bool has_library(const LibraryPath& path) const;

  private:
    void write_library_config_internal(
            const arcticdb::proto::storage::LibraryConfig& lib_cfg_proto, const LibraryPath& path, bool validate
    ) const;
    [[nodiscard]] arcticdb::proto::storage::LibraryConfig get_config_internal(
            const LibraryPath& path, const std::optional<StorageOverride>& storage_override
    ) const;

    std::shared_ptr<Store> store_;
    LRUCache<LibraryPath, LibraryWithConfig> open_libraries_;
    mutable std::mutex open_libraries_mutex_; // for open_libraries_
};
} // namespace arcticdb::storage

namespace fmt {
template<>
struct formatter<arcticdb::storage::ModifiableLibraryOption> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::storage::ModifiableLibraryOption o, FormatContext& ctx) const {
        switch (o) {
        case arcticdb::storage::ModifiableLibraryOption::DEDUP:
            return fmt::format_to(ctx.out(), "DEDUP");
        case arcticdb::storage::ModifiableLibraryOption::ROWS_PER_SEGMENT:
            return fmt::format_to(ctx.out(), "ROWS_PER_SEGMENT");
        case arcticdb::storage::ModifiableLibraryOption::COLUMNS_PER_SEGMENT:
            return fmt::format_to(ctx.out(), "COLUMNS_PER_SEGMENT");
        case arcticdb::storage::ModifiableLibraryOption::RECURSIVE_NORMALIZERS:
            return fmt::format_to(ctx.out(), "RECURSIVE_NORMALIZERS");
        default:
            arcticdb::util::raise_rte("Unrecognized modifiable option {}", int(o));
        }
    }
};

template<>
struct formatter<arcticdb::storage::ModifiableEnterpriseLibraryOption> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::storage::ModifiableEnterpriseLibraryOption o, FormatContext& ctx) const {
        switch (o) {
        case arcticdb::storage::ModifiableEnterpriseLibraryOption::REPLICATION:
            return fmt::format_to(ctx.out(), "REPLICATION");
        case arcticdb::storage::ModifiableEnterpriseLibraryOption::BACKGROUND_DELETION:
            return fmt::format_to(ctx.out(), "BACKGROUND_DELETION");
        default:
            arcticdb::util::raise_rte("Unrecognized modifiable option {}", int(o));
        }
    }
};
} // namespace fmt
