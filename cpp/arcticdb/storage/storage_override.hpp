#pragma once

#include <variant>
#include <string>

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>

namespace arcticdb::storage {

class S3Override {
    std::string credential_name_;
    std::string credential_key_;
    std::string endpoint_;
    std::string bucket_name_;
    std::string region_;
    bool use_virtual_addressing_ = false;

public:
    std::string credential_name() const {
        return credential_name_;
    }

    void set_credential_name(std::string_view credential_name) {
        credential_name_ = credential_name;
    }

    std::string credential_key() const {
        return credential_key_;
    }

    void set_credential_key(std::string_view credential_key) {
        credential_key_ = credential_key;
    }

    std::string endpoint() const {
        return endpoint_;
    }

    void set_endpoint(std::string_view endpoint) {
        endpoint_ = endpoint;
    }

    std::string bucket_name() const {
        return bucket_name_;
    }

    void set_bucket_name(std::string_view bucket_name){
        bucket_name_ = bucket_name;
    }

    std::string region() const {
        return region_;
    }

    void set_region(std::string_view region){
        region_ = region;
    }

    bool use_virtual_addressing() const {
        return use_virtual_addressing_;
    }

    void set_use_virtual_addressing(bool use_virtual_addressing) {
        use_virtual_addressing_ = use_virtual_addressing;
    }

    void modify_storage_config(arcticdb::proto::storage::VariantStorage& storage) const {
        if(storage.config().Is<arcticdb::proto::s3_storage::Config>()) {
            arcticdb::proto::s3_storage::Config s3_storage;
            storage.config().UnpackTo(&s3_storage);

            s3_storage.set_bucket_name(bucket_name_);
            s3_storage.set_credential_name(credential_name_);
            s3_storage.set_credential_key(credential_key_);
            s3_storage.set_endpoint(endpoint_);
            s3_storage.set_region(region_);
            s3_storage.set_use_virtual_addressing(use_virtual_addressing_);

            util::pack_to_any(s3_storage, *storage.mutable_config());
        }
    }
};

class AzureOverride {
    std::string container_name_;
    std::string endpoint_;
    std::string ca_cert_path_;

public:
    std::string container_name() const {
        return container_name_;
    }

    void set_container_name(std::string_view container_name) {
        container_name_ = container_name;
    }

    std::string endpoint() const {
        return endpoint_;
    }

    void set_endpoint(std::string_view endpoint) {
        endpoint_ = endpoint;
    }

    std::string ca_cert_path() const {
        return ca_cert_path_;
    }

    void set_ca_cert_path(std::string_view ca_cert_path){
        ca_cert_path_ = ca_cert_path;
    }

    void modify_storage_config(arcticdb::proto::storage::VariantStorage& storage) const {
        if(storage.config().Is<arcticdb::proto::azure_storage::Config>()) {
            arcticdb::proto::azure_storage::Config azure_storage;
            storage.config().UnpackTo(&azure_storage);

            azure_storage.set_container_name(container_name_);
            azure_storage.set_endpoint(endpoint_);
            azure_storage.set_ca_cert_path(ca_cert_path_);

            util::pack_to_any(azure_storage, *storage.mutable_config());
        }
    }
};

class LmdbOverride {
    std::string path_;
    uint64_t map_size_;

public:

    [[nodiscard]] std::string path() const {
        return path_;
    }

    [[nodiscard]] uint64_t map_size() const {
        return map_size_;
    }

    void set_path(std::string path) {
        path_ = path;
    }

    void set_map_size(uint64_t map_size) {
        map_size_ = map_size;
    }

    void modify_storage_config(arcticdb::proto::storage::VariantStorage& storage) const {
        if(storage.config().Is<arcticdb::proto::lmdb_storage::Config>()) {
            arcticdb::proto::lmdb_storage::Config lmdb_storage;
            storage.config().UnpackTo(&lmdb_storage);

            lmdb_storage.set_path(path_);
            lmdb_storage.set_map_size(map_size_);

            util::pack_to_any(lmdb_storage, *storage.mutable_config());
        }
    }
};

using VariantStorageOverride = std::variant<std::monostate, S3Override, AzureOverride, LmdbOverride>;

class StorageOverride {
    VariantStorageOverride override_;

public:
    const VariantStorageOverride& variant() const {
        return override_;
    }

    void set_s3_override(const S3Override& storage_override) {
        override_ = storage_override;
    }

    void set_azure_override(const AzureOverride& storage_override) {
        override_ = storage_override;
    }

    void set_lmdb_override(const LmdbOverride& storage_override) {
        override_ = storage_override;
    }
};

} //namespace arcticdb::storage
