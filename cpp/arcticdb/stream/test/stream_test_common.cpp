/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/test/stream_test_common.hpp>

#include <filesystem>

namespace arcticdb {

inline std::pair<storage::LibraryPath, arcticdb::proto::storage::LibraryConfig> test_config(const std::string& lib_name
) {
    auto unique_lib_name = fmt::format("{}_{}", lib_name, util::SysClock::nanos_since_epoch());
    arcticdb::proto::storage::LibraryConfig config;

    config.mutable_lib_desc()->set_name(unique_lib_name);
    auto temp_path = std::filesystem::temp_directory_path();
    // on windows, path is only implicitly converted to wstring, not string
    arcticdb::proto::storage::VariantStorage lmdb_config;
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path(temp_path.string());
    // 128 MiB - needs to be reasonably small else Windows build runs out of disk
    cfg.set_map_size(128ULL * (1ULL << 20));
    util::pack_to_any(cfg, *lmdb_config.mutable_config());

    auto library_path = storage::LibraryPath::from_delim_path(unique_lib_name);
    auto storage_id = fmt::format("{}_store", unique_lib_name);
    config.mutable_lib_desc()->add_storage_ids(storage_id);
    config.mutable_storage_by_id()->insert(
            google::protobuf::MapPair<std::decay_t<decltype(storage_id)>, std::decay_t<decltype(lmdb_config)>>(
                    storage_id, lmdb_config
            )
    );
    return std::make_pair(library_path, config);
}

inline std::shared_ptr<storage::Library> test_library_from_config(
        const storage::LibraryPath& lib_path, const arcticdb::proto::storage::LibraryConfig& lib_cfg
) {
    auto storage_cfg = lib_cfg.storage_by_id();
    auto vs_cfg = lib_cfg.lib_desc().has_version()
                          ? storage::LibraryDescriptor::VariantStoreConfig{lib_cfg.lib_desc().version()}
                          : std::monostate{};
    return std::make_shared<storage::Library>(
            lib_path,
            storage::create_storages(lib_path, storage::OpenMode::DELETE, storage_cfg, storage::NativeVariantStorage()),
            std::move(vs_cfg)
    );
}

/**
 * Uses the argument to create a LibraryConfig for LMDB storage and instantiate a Library with it.
 *
 * Note: the VariantStoreConfig will be monostate. If you need a version store with special config, then inline this
 * function and modify the config.
 */
inline std::shared_ptr<storage::Library> test_library(const std::string& lib_name) {
    auto [lib_path, config] = test_config(lib_name);
    return test_library_from_config(lib_path, config);
}

void TestStore::SetUp() {
    library_ = test_library(get_name());
    test_store_ = std::make_shared<version_store::PythonVersionStore>(library_);
}

void TestStore::TearDown() { library_->cleanup(); }
} // namespace arcticdb
