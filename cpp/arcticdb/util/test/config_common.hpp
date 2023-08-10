/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <string>

namespace arcticdb {

inline auto get_test_environment_config(
    const arcticdb::storage::LibraryPath& path,
    const arcticdb::storage::StorageName& storage_name,
    const arcticdb::storage::EnvironmentName& environment_name) {

    using namespace arcticdb::storage;
    using MemoryConfig = storage::details::InMemoryConfigResolver::MemoryConfig;
    MemoryConfig mem_config = {};

    arcticdb::proto::storage::StorageConfig storage_conf;
    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path("./"); //TODO local path is a bit annoying. TMPDIR?
    cfg.set_recreate_if_exists(true);
    util::pack_to_any(cfg, *storage_conf.mutable_config());
    mem_config.storage_configs_.insert(std::make_pair(storage_name, storage_conf));

    arcticdb::proto::storage::LibraryDescriptor library_descriptor;
    library_descriptor.add_storage_ids(storage_name.value);
    mem_config.libraries_.insert(std::make_pair(path, library_descriptor));

    std::vector <std::pair<std::string, MemoryConfig>> output;
    output.emplace_back(environment_name.value, mem_config);
    return output;
}
}