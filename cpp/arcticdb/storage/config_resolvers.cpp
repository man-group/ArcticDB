/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/common.hpp>

namespace arcticdb::storage::details {

std::optional<InMemoryConfigResolver::MemoryConfig> InMemoryConfigResolver::get_environment(
        const EnvironmentName& environment_name
) const {
    auto env = environments_.find(environment_name);
    if (env == environments_.end())
        return std::nullopt;

    return env->second;
}

InMemoryConfigResolver::MemoryConfig& InMemoryConfigResolver::get_or_add_environment(
        const EnvironmentName& environment_name
) {
    auto env = environments_.find(environment_name);
    if (env == environments_.end()) {
        env = environments_.try_emplace(environment_name, MemoryConfig()).first;
    }

    return env->second;
}

std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> InMemoryConfigResolver::get_libraries(
        const EnvironmentName& environment_name
) const {
    auto config = get_environment(environment_name);
    std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> output;
    if (!config.has_value())
        return output;

    for (auto& pair : config->libraries_)
        output.emplace_back(pair);

    return output;
}

std::vector<std::pair<StorageName, arcticdb::proto::storage::VariantStorage>> InMemoryConfigResolver::get_storages(
        const EnvironmentName& environment_name
) const {
    auto config = get_environment(environment_name);
    std::vector<std::pair<StorageName, arcticdb::proto::storage::VariantStorage>> output;
    if (!config.has_value())
        return output;

    for (auto& pair : config->storages_)
        output.emplace_back(pair);

    return output;
}

void InMemoryConfigResolver::add_library(
        const EnvironmentName& environment_name, const arcticdb::proto::storage::LibraryDescriptor& library_descriptor
) {
    auto& config = get_or_add_environment(environment_name);
    config.libraries_.try_emplace(LibraryPath::from_delim_path(library_descriptor.name()), library_descriptor);
}

void InMemoryConfigResolver::add_storage(
        const EnvironmentName& environment_name, const StorageName& storage_name,
        const arcticdb::proto::storage::VariantStorage& storage
) {
    auto& config = get_or_add_environment(environment_name);
    config.storages_.try_emplace(StorageName(storage_name), storage);
}

} // namespace arcticdb::storage::details