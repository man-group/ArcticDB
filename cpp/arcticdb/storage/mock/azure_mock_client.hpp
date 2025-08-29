/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <azure/core/http/http_status_code.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>

namespace arcticdb::storage::azure {

class MockAzureClient : public AzureClientWrapper {

public:
    void write_blob(
        const std::string& blob_name,
        Segment& segment,
        const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option,
        unsigned int request_timeout) override;

    Segment read_blob(
        const std::string& blob_name,
        const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
        unsigned int request_timeout) override;

    void delete_blobs(
        const std::vector<std::string>& blob_names,
        unsigned int request_timeout) override;

    bool blob_exists(const std::string& blob_name) override;

    Azure::Storage::Blobs::ListBlobsPagedResponse list_blobs(const std::string& prefix) override;

    static std::string get_failure_trigger(
        const std::string& blob_name,
        StorageOperation operation_to_fail,
        const std::string& error_code,
        Azure::Core::Http::HttpStatusCode error_to_fail_with);

private:
    // Stores a mapping from blob_name to a Segment.
    std::map<std::string, Segment> azure_contents;
};

}
