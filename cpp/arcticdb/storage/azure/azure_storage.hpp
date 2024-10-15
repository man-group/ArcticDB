/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/azure/azure_client_wrapper.hpp>
#include <arcticdb/storage/azure/azure_settings.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

namespace arcticdb::storage::azure {

class AzureStorage final : public Storage {
  public:
    AzureStorage(const LibraryPath &lib, OpenMode mode, const AzureSettings &conf);

    std::string name() const final;

  protected:
    void do_write(Composite<KeySegmentPair>&& kvs) final;

    void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

    void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string &prefix) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final {
        return true;
    }

    bool do_fast_delete() final {
        return false;
    }

    std::string do_key_path(const VariantKey&) const final { return {}; };

  private:
    std::unique_ptr<AzureClientWrapper> azure_client_;

    std::string root_folder_;
    std::string container_name_;
    unsigned int request_timeout_;
    Azure::Storage::Blobs::UploadBlockBlobFromOptions upload_option_;
    Azure::Storage::Blobs::DownloadBlobToOptions download_option_;
};
} //namespace arcticdb::azure
