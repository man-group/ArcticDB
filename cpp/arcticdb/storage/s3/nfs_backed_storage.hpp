/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <aws/core/Aws.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>

#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>

namespace arcticdb::storage::nfs_backed {

class NfsBackedStorage final : public Storage {
public:
    using Config = arcticdb::proto::nfs_backed_storage::Config;

    NfsBackedStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    std::string name() const final;

private:
    void do_write(Composite<KeySegmentPair>&& kvs) final;

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

    void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

    void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return true;
    }

    bool do_fast_delete() final {
        return false;
    }

    std::string do_key_path(const VariantKey&) const final { return {}; };

    auto& client() { return s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }
    const std::string& region() const { return region_; }

    std::shared_ptr<storage::s3::S3ApiInstance> s3_api_;
    std::unique_ptr<storage::s3::S3ClientWrapper> s3_client_;
    std::string root_folder_;
    std::string bucket_name_;
    std::string region_;
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

struct NfsBucketizer {
    static std::string bucketize(const std::string& root_folder, const VariantKey& vk);
    static size_t bucketize_length(KeyType key_type);
};


} //namespace arcticdb::nfs_backed
