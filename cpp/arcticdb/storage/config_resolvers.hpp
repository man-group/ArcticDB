/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/common.hpp>

namespace arcticdb::storage {
class ConfigResolver {
  public:
    virtual ~ConfigResolver() = default;

    // TODO nothing especially wrong with this method but what's the expected use case?
    // virtual std::vector<EnvironmentName> list_environments() const = 0;
    virtual std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> get_libraries(
            const EnvironmentName& environment_name
    ) const = 0;
    virtual std::vector<std::pair<StorageName, arcticdb::proto::storage::VariantStorage>> get_storages(
            const EnvironmentName& environment_name
    ) const = 0;
    virtual void add_library(
            const EnvironmentName& environment_name,
            const arcticdb::proto::storage::LibraryDescriptor& library_descriptor
    ) = 0;
    virtual void add_storage(
            const EnvironmentName& environment_name, const StorageName& storage_name,
            const arcticdb::proto::storage::VariantStorage& storage
    ) = 0;
    virtual void initialize_environment(const EnvironmentName& environment_name) = 0;
    virtual std::string_view resolver_type() const = 0;
};

template<class T>
std::shared_ptr<ConfigResolver> create_in_memory_resolver(const T& id_and_env_pairs);
} // namespace arcticdb::storage

namespace arcticdb::storage::details {

class InMemoryConfigResolver final : public ConfigResolver {
  public:
    typedef std::unordered_map<StorageName, arcticdb::proto::storage::VariantStorage> StorageMap;
    typedef std::unordered_map<LibraryPath, arcticdb::proto::storage::LibraryDescriptor> LibraryMap;

    struct MemoryConfig {
        StorageMap storages_;
        LibraryMap libraries_;
    };

    InMemoryConfigResolver() = default;

    template<class T>
    explicit InMemoryConfigResolver(const T& environments) : environments_() {
        for (auto&& [environment_name, env_storages] : environments) {
            environments_.emplace(environment_name, env_storages);
        }
    }

    std::vector<std::pair<LibraryPath, arcticdb::proto::storage::LibraryDescriptor>> get_libraries(
            const EnvironmentName& environment_name
    ) const override;
    std::vector<std::pair<StorageName, arcticdb::proto::storage::VariantStorage>> get_storages(
            const EnvironmentName& environment_name
    ) const override;

    void add_library(
            const EnvironmentName& environment_name,
            const arcticdb::proto::storage::LibraryDescriptor& library_descriptor
    ) override;
    void add_storage(
            const EnvironmentName& environment_name, const StorageName& storage_name,
            const arcticdb::proto::storage::VariantStorage& storage
    ) override;

    void initialize_environment(const EnvironmentName&) override {}
    std::string_view resolver_type() const override { return "in_mem"; }

  private:
    std::optional<MemoryConfig> get_environment(const EnvironmentName& environment_name) const;
    MemoryConfig& get_or_add_environment(const EnvironmentName& environment_name);
    std::unordered_map<EnvironmentName, MemoryConfig> environments_;
};

} // namespace arcticdb::storage::details

namespace arcticdb::storage {

template<class T>
std::shared_ptr<ConfigResolver> create_in_memory_resolver(const T& id_and_env_pairs) {
    return std::make_shared<details::InMemoryConfigResolver>(id_and_env_pairs);
}

} // namespace arcticdb::storage
