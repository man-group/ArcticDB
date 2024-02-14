/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <azure/core/http/curl_transport.hpp>
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <arcticdb/storage/storage_utils.hpp>


namespace arcticdb::storage::azure {

static const size_t BATCH_SUBREQUEST_LIMIT = 256; //https://github.com/Azure/azure-sdk-for-python/blob/767facc39f2487504bcde4e627db16a79f96b297/sdk/storage/azure-storage-blob/azure/storage/blob/_container_client.py#L1608

    // An abstract class, which is responsible for sending the requests and parsing the responses from Azure.
    // It can be derived as either a real connection to Azure or a mock used for unit tests.
class AzureClientWrapper {
public:
    using Config = arcticdb::proto::azure_storage::Config;
    virtual void write_blob(
            const std::string& blob_name,
            Segment&& segment,
            const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option,
            unsigned int request_timeout) = 0;

    virtual Segment read_blob(
            const std::string& blob_name,
            const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
            unsigned int request_timeout) = 0;

    virtual void delete_blobs(
            const std::vector<std::string>& blob_names,
            unsigned int request_timeout) = 0;

    virtual Azure::Storage::Blobs::ListBlobsPagedResponse list_blobs(const std::string& prefix) = 0;

    virtual bool blob_exists(const std::string& blob_name) = 0;

    virtual ~AzureClientWrapper() = default;
};

}


