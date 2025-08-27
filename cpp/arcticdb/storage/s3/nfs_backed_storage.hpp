/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>

namespace arcticdb::storage::nfs_backed {

class NfsBackedStorage final : public Storage {
public:
    using Config = arcticdb::proto::nfs_backed_storage::Config;

    NfsBackedStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    std::string name() const final;

    bool supports_object_size_calculation() const final override;

private:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>("do_write_if_none not implemented for NFS backed storage");
    };

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) final;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string &prefix) final;

    void do_visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return true;
    }

    SupportsAtomicWrites do_supports_atomic_writes() const final {
        return SupportsAtomicWrites::NEEDS_TEST;
    }

    bool do_fast_delete() final {
        return false;
    }

    std::string do_key_path(const VariantKey&) const final;

    auto& client() { return s3_client_; }
    const std::string& bucket_name() const { return bucket_name_; }
    const std::string& root_folder() const { return root_folder_; }
    const std::string& region() const { return region_; }

    std::shared_ptr<s3::S3ApiInstance> s3_api_;
    std::unique_ptr<storage::s3::S3ClientInterface> s3_client_;
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
