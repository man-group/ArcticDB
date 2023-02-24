/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/s3_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/storage/s3/s3_client_accessor.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>

namespace arcticdb::storage::nfs_backed {

class NfsBackedStorage final : public Storage<NfsBackedStorage> {
    using Parent = Storage<NfsBackedStorage>;
    friend Parent;

public:
    friend class S3TestForwarder<NfsBackedStorage>;
    friend class S3TestClientAccessor<NfsBackedStorage>;
    using Config = arcticdb::proto::nfs_backed_storage::Config;

    NfsBackedStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

protected:
    void do_write(Composite<KeySegmentPair>&& kvs);

    void do_update(Composite<KeySegmentPair>&& kvs);

    template<class Visitor>
    void do_read(Composite<VariantKey>&& ks, Visitor &&visitor);

    void do_remove(Composite<VariantKey>&& ks);

    template<class Visitor>
    void do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix);

    bool do_key_exists(const VariantKey& key);

    bool do_supports_prefix_matching() {
        return true;
    }

    bool do_fast_delete() {
        return false;
    }

private:
    auto& client() { return s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }

    std::shared_ptr<storage::s3::S3ApiInstance> s3_api_;
    Aws::S3::S3Client s3_client_;
    std::string root_folder_;
    std::string bucket_name_;
};

class NfsBackedStorageFactory final : public StorageFactory<NfsBackedStorageFactory> {
    using Parent = StorageFactory<NfsBackedStorageFactory>;
    friend Parent;

public:
    using Config = arcticdb::proto::nfs_backed_storage::Config;
    using StorageType = NfsBackedStorageFactory;

    NfsBackedStorageFactory(const Config &conf) :
        conf_(conf) {
    }
private:
    auto do_create_storage(const LibraryPath &lib, OpenMode mode) {
        return NfsBackedStorage(lib, mode, conf_);
    }

    Config conf_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string &bucket_name) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::nfs_backed_storage::Config cfg;
    cfg.set_bucket_name(bucket_name);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(
    const std::string &bucket_name,
    const std::string &credential_name,
    const std::string &credential_key,
    const std::string &endpoint
) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::nfs_backed_storage::Config cfg;
    cfg.set_bucket_name(bucket_name);
    cfg.set_credential_name(credential_name);
    cfg.set_credential_key(credential_key);
    cfg.set_endpoint(endpoint);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

} //namespace arcticdb::nfs_backed

#define ARCTICDB_NFS_BACKED_STORAGE_H_
#include <arcticdb/storage/s3/nfs_backed_storage-inl.hpp>