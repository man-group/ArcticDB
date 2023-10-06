/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/protobuf_mappings.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <fstream>
#include <string>
#include <string_view>

namespace arcticdb::storage::details {

std::optional<InMemoryConfigResolver::MemoryConfig> InMemoryConfigResolver::get_environment(const EnvironmentName& environment_name) const {
    auto env = environments_.find(environment_name);
    if(env == environments_.end())
        return std::nullopt;

    return env->second;
}

InMemoryConfigResolver::MemoryConfig& InMemoryConfigResolver::get_or_add_environment(const EnvironmentName& environment_name) {
    auto env = environments_.find(environment_name);
    if(env == environments_.end()) {
        env = environments_.insert(std::make_pair(environment_name, MemoryConfig())).first;
    }

    return env->second;
}

std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> InMemoryConfigResolver::get_libraries(const EnvironmentName &environment_name) const {
    auto config = get_environment(environment_name);
    std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> output;
    if(!config.has_value())
        return output;

    for(auto& pair : config.value().libraries_)
        output.emplace_back(pair);

    return output;
}

std::vector<std::pair<StorageName, StorageConfig>> InMemoryConfigResolver::get_storages(const EnvironmentName &environment_name) const {
    auto config = get_environment(environment_name);
    std::vector<std::pair<StorageName, StorageConfig>> output;
    if(!config.has_value())
        return output;

    for(auto& pair : config.value().storages_)
        output.emplace_back(pair);

    return output;
}

void InMemoryConfigResolver::add_library(const EnvironmentName& environment_name, const arcticdb::proto::storage::LibraryDescriptor& library_descriptor) {
    auto& config = get_or_add_environment(environment_name);
    config.libraries_.insert(std::make_pair(LibraryPath::from_delim_path(library_descriptor.name()), library_descriptor));
}

void InMemoryConfigResolver::add_storage(const EnvironmentName& environment_name, const StorageName& storage_name, const StorageConfig& storage) {
    auto& config = get_or_add_environment(environment_name);
    config.storages_.insert(std::make_pair(StorageName(storage_name), storage));
}

}
