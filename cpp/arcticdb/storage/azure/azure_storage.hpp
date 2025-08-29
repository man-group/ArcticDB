/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <string>

namespace arcticdb::storage::azure {

class AzureStorage final : public Storage {
  public:
    // friend class AzureTestClientAccessor<AzureStorage>;
    using Config = arcticdb::proto::azure_storage::Config;

    AzureStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    std::string name() const final;

  protected:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>("Atomic operations are only supported for s3 backend");
    };

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) final;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return true;
    }

    SupportsAtomicWrites do_supports_atomic_writes() const final {
        return SupportsAtomicWrites::NO;
    }

    bool do_fast_delete() final {
        return false;
    }

    std::string do_key_path(const VariantKey&) const final;

  private:
    std::unique_ptr<AzureClientWrapper> azure_client_;

    std::string root_folder_;
    std::string container_name_;
    unsigned int request_timeout_;
    Azure::Storage::Blobs::UploadBlockBlobFromOptions upload_option_;
    Azure::Storage::Blobs::DownloadBlobToOptions download_option_;
};

inline arcticdb::proto::storage::VariantStorage pack_config(const std::string &container_name) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::azure_storage::Config cfg;
    cfg.set_container_name(container_name);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

inline arcticdb::proto::storage::VariantStorage pack_config(
        const std::string &container_name,
        const std::string &endpoint
        ) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::azure_storage::Config cfg;
    cfg.set_container_name(container_name);
    cfg.set_endpoint(endpoint);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

template<typename ConfigType>
std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> get_azure_credentials(const ConfigType& conf) {
    return std::make_shared<Azure::Storage::StorageSharedKeyCredential>(conf.credential_name(), conf.credential_key());
}

} //namespace arcticdb::azure
