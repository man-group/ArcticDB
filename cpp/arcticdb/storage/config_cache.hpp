/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/protobuf_mappings.hpp>
#include <arcticdb/storage/variant_storage_factory.hpp>
#include <arcticdb/storage/storages.hpp>

#include <optional>

namespace arcticdb::storage {

//TODO cache invalidation
class ConfigCache {
  public:
    ConfigCache(const EnvironmentName &environment_name, const std::shared_ptr<ConfigResolver> &resolver) :
        environment_name_(environment_name), descriptor_map_(), config_resolver_(resolver) {
        refresh_config();
    }

    std::optional<LibraryDescriptor> get_descriptor(const LibraryPath &path) {
        std::lock_guard<std::mutex> lock{mutex_};
        auto descriptor = descriptor_map_.find(path);
        if (descriptor == descriptor_map_.end())
            return std::nullopt;

        return descriptor->second;
    }

    bool library_exists(const LibraryPath &path) const {
        std::lock_guard<std::mutex> lock{mutex_};
        return descriptor_map_.find(path) != descriptor_map_.end();
    }

    void add_library_config(const LibraryPath &path, const arcticdb::proto::storage::LibraryConfig lib_cfg) {
        add_library(path, decode_library_descriptor(lib_cfg.lib_desc()));
        for(const auto& storage: lib_cfg.storage_by_id())
            add_storage(StorageName{storage.first}, storage.second);
    }

    void add_library(const LibraryPath &path, const LibraryDescriptor &desc) {
        config_resolver_->add_library(environment_name_, encode_library_descriptor(desc));
        std::lock_guard<std::mutex> lock{mutex_};
        descriptor_map_.emplace(path, desc);
    }

    void add_storage(const StorageName& storage_name, const arcticdb::proto::storage::VariantStorage storage) {
        config_resolver_->add_storage(environment_name_, storage_name, storage);
    }

    std::vector<LibraryPath> list_libraries(const std::string_view &prefix) {
        std::lock_guard<std::mutex> lock{mutex_};
        std::vector<LibraryPath> res;
        for (auto &[lib, _] : descriptor_map_) {
            auto l = lib.to_delim_path();
            if (l.find(prefix) != std::string::npos) {
                res.push_back(lib);
            }
        }

        return res;
    }

    std::shared_ptr<Storages> create_storages(const LibraryPath &path, OpenMode mode) {
        auto maybe_descriptor = get_descriptor(path);
        if (!maybe_descriptor.has_value())
            throw std::runtime_error(fmt::format("Library {} not found", path));

        auto &descriptor = maybe_descriptor.value();

        util::check(!descriptor.storage_ids_.empty(), "Can't configure library with no storage ids");
        std::vector<std::unique_ptr<VariantStorage>> variants;
        for (const auto& storage_name : descriptor.storage_ids_) {
            auto factory = get_storage_factory(storage_name);
            variants.emplace_back(factory->create_storage(path, mode));
        }
        return std::make_shared<Storages>(std::move(variants), mode);
    }

  private:
    void refresh_config() {
        std::lock_guard<std::mutex> lock{mutex_};
        descriptor_map_.clear();
        storage_configs_.clear();

        auto libraries = config_resolver_->get_libraries(environment_name_);
        for (auto& [library_path, descriptor] : libraries) {
            descriptor_map_.emplace(library_path, decode_library_descriptor(descriptor));
        }
        auto storages = config_resolver_->get_storages(environment_name_);
        for(auto& [storage_name, config] : storages) {
            storage_configs_.emplace(StorageName(storage_name), config);
        }
        auto default_storages = config_resolver_->get_default_storages(environment_name_);
        for(auto& [storage_name, config] : default_storages) {
            if (storage_configs_.find(storage_name) == storage_configs_.end()) {
                config_resolver_->add_storage(environment_name_, storage_name, config);
                storage_configs_.emplace(StorageName(storage_name), config);
            }
        }
    }

    std::shared_ptr<VariantStorageFactory> get_storage_factory_internal(const StorageName &storage_name) {
        //See if we have previously cached the factory
        auto factory_it = storage_factories_.find(storage_name);
        if (factory_it != storage_factories_.end())
            return factory_it->second;

        //Otherwise see if we have the storage config
        auto storage_conf = storage_configs_.find(storage_name);
        if(storage_conf != storage_configs_.end())
            return create_storage_factory(storage_conf->second);


        //As a last resort, get the whole environment config from the resolver
        refresh_config();
        storage_conf = storage_configs_.find(storage_name);
        if(storage_conf != storage_configs_.end())
            return create_storage_factory(storage_conf->second);
        else
            return std::shared_ptr<VariantStorageFactory>();
    }


    std::shared_ptr<VariantStorageFactory> get_storage_factory(const StorageName &storage_name) {
        auto factory = get_storage_factory_internal(storage_name);
        util::check(factory != nullptr,
                    "cannot create storage factory for environment_name={}, storage_name={}",
                    environment_name_.value, storage_name.value);
        if (auto[it, inserted] = storage_factories_.insert(std::make_pair(storage_name, factory)); !inserted) {
            factory = it->second;
        }

        return factory;
    }

    EnvironmentName environment_name_;
    std::unordered_map<LibraryPath, LibraryDescriptor> descriptor_map_;
    std::unordered_map<StorageName, arcticdb::proto::storage::VariantStorage> storage_configs_;
    std::unordered_map<StorageName, std::shared_ptr<VariantStorageFactory>> storage_factories_;
    std::shared_ptr<ConfigResolver> config_resolver_;
    mutable std::mutex mutex_;
};

}