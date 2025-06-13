/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/azure/azure_client_interface.hpp>

namespace arcticdb::storage::azure {
class RealAzureClient : public AzureClientWrapper {
  private:
    Azure::Storage::Blobs::BlobContainerClient container_client;

  public:
    explicit RealAzureClient(const Config& conf);

    static Azure::Storage::Blobs::BlobClientOptions get_client_options(const Config& conf);

    void write_blob(
            const std::string& blob_name, Segment& segment,
            const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option, unsigned int request_timeout
    ) override;

    Segment read_blob(
            const std::string& blob_name, const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
            unsigned int request_timeout
    ) override;

    void delete_blobs(const std::vector<std::string>& blob_names, unsigned int request_timeout) override;

    bool blob_exists(const std::string& blob_name) override;

    Azure::Storage::Blobs::ListBlobsPagedResponse list_blobs(const std::string& prefix) override;
};
} // namespace arcticdb::storage::azure
