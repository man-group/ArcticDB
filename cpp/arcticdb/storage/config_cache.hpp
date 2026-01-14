/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/protobuf_mappings.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/storages.hpp>

#include <optional>

namespace arcticdb::storage {

// TODO cache invalidation
class ConfigCache {
  public:
    ConfigCache(const EnvironmentName& environment_name, const std::shared_ptr<ConfigResolver>& resolver) :
        environment_name_(environment_name),
        descriptor_map_(),
        config_resolver_(resolver) {
        refresh_config();
    }

    std::optional<LibraryDescriptor> get_descriptor(const LibraryPath& path) {
        std::lock_guard<std::mutex> lock{mutex_};
        auto descriptor = descriptor_map_.find(path);
        if (descriptor == descriptor_map_.end())
            return std::nullopt;

        return descriptor->second;
    }

    bool library_exists(const LibraryPath& path) const {
        std::lock_guard<std::mutex> lock{mutex_};
        return descriptor_map_.find(path) != descriptor_map_.end();
    }

    void add_library(const LibraryPath& path, const LibraryDescriptor& desc) {
        config_resolver_->add_library(environment_name_, encode_library_descriptor(desc));
        std::lock_guard<std::mutex> lock{mutex_};
        descriptor_map_.emplace(path, desc);
    }

    std::vector<LibraryPath> list_libraries(std::string_view prefix) {
        std::lock_guard<std::mutex> lock{mutex_};
        std::vector<LibraryPath> res;
        for (auto& [lib, _] : descriptor_map_) {
            auto l = lib.to_delim_path();
            if (l.find(prefix) != std::string::npos) {
                res.push_back(lib);
            }
        }

        return res;
    }

    std::shared_ptr<Storages> create_storages(
            const LibraryPath& path, OpenMode mode, const NativeVariantStorage& native_storage_config
    ) {
        auto maybe_descriptor = get_descriptor(path);
        if (!maybe_descriptor.has_value())
            throw std::runtime_error(fmt::format("Library {} not found", path));

        auto& descriptor = *maybe_descriptor;

        util::check(!descriptor.storage_ids_.empty(), "Can't configure library with no storage ids");
        std::vector<std::shared_ptr<Storage>> storages;
        for (const auto& storage_name : descriptor.storage_ids_) {
            // Otherwise see if we have the storage config.
            arcticdb::proto::storage::VariantStorage storage_conf;
            auto storage_conf_pos = storage_configs_.find(storage_name);
            if (storage_conf_pos != storage_configs_.end())
                storage_conf = storage_conf_pos->second;

            // As a last resort, get the whole environment config from the resolver.
            refresh_config();
            storage_conf_pos = storage_configs_.find(storage_name);
            if (storage_conf_pos != storage_configs_.end())
                storage_conf = storage_conf_pos->second;

            util::variant_match(
                    native_storage_config.variant(),
                    [&storage_conf, &storages, &path, mode](const s3::S3Settings& settings) {
                        util::check(
                                storage_conf.config().Is<arcticdb::proto::s3_storage::Config>(),
                                "Only support S3 native settings"
                        );
                        arcticdb::proto::s3_storage::Config s3_storage;
                        storage_conf.config().UnpackTo(&s3_storage);
                        storages.emplace_back(create_storage(path, mode, s3::S3Settings(settings).update(s3_storage)));
                    },
                    [&storage_conf, &storages, &path, mode](const s3::GCPXMLSettings& settings) {
                        util::check(
                                storage_conf.config().Is<arcticdb::proto::gcp_storage::Config>(),
                                "Only support GCP native settings"
                        );
                        arcticdb::proto::gcp_storage::Config gcp_storage;
                        storage_conf.config().UnpackTo(&gcp_storage);
                        storages.emplace_back(
                                create_storage(path, mode, s3::GCPXMLSettings(settings).update(gcp_storage))
                        );
                    },
                    [&storage_conf, &storages, &path, mode](const auto&) {
                        storages.emplace_back(create_storage(path, mode, storage_conf));
                    }
            );
        }
        return std::make_shared<Storages>(std::move(storages), mode);
    }

  private:
    void refresh_config() {
        std::lock_guard<std::mutex> lock{mutex_};
        descriptor_map_.clear();
        storage_configs_.clear();

        auto libraries = config_resolver_->get_libraries(environment_name_);
        for (auto& [library_path, descriptor] : libraries) {
            descriptor_map_.try_emplace(library_path, decode_library_descriptor(descriptor));
        }
        auto storages = config_resolver_->get_storages(environment_name_);
        for (auto& [storage_name, config] : storages) {
            storage_configs_.try_emplace(StorageName(storage_name), config);
        }
    }

    EnvironmentName environment_name_;
    std::unordered_map<LibraryPath, LibraryDescriptor> descriptor_map_;
    std::unordered_map<StorageName, arcticdb::proto::storage::VariantStorage> storage_configs_;
    std::shared_ptr<ConfigResolver> config_resolver_;
    mutable std::mutex mutex_;
};

} // namespace arcticdb::storage