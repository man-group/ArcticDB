#pragma once

#include <variant>
#include <string>

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>

namespace arcticdb::storage {
class S3CredentialsOverride {
    std::string credential_name_;
    std::string credential_key_;
    std::string endpoint_;
    std::string bucket_;

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

    std::string bucket_name_() const {
        return bucket_;
    }

    void set_bucket_name(std::string_view bucket_name){
        bucket_name_ = bucket_name;
    }

    void modify_storage_credentials(arcticdb::proto::storage::VariantStorage& storage) const {
        if(storage.config().Is<arcticdb::proto::s3_storage::Config>()) {
            arcticdb::proto::s3_storage::Config s3_storage;
            storage.config().UnpackTo(&s3_storage);
            if(!credential_name_.empty())
                s3_storage.set_credential_name(credential_name_);

            if(!credential_key_.empty())
                s3_storage.set_credential_key(credential_key_);

            if(!endpoint_.empty())
                s3_storage.set_endpoint(endpoint_);

            if (!bucket_name_).empty())
                s3_storage.set_bucket_name(bucket_name_);

            util::pack_to_any(s3_storage, *storage.mutable_config());
        }
    }
};

using VariantStorageOverride = std::variant<std::monostate, S3CredentialsOverride>;

class StorageOverride {
    VariantStorageOverride override_;

public:
    const VariantStorageOverride& variant() const {
        return override_;
    }

    void set_override(const S3CredentialsOverride& credentials_override) {
        override_ = credentials_override;
    }
};
} //namespace arcticdb::storage