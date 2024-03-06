/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <azure/core/http/curl_transport.hpp>
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/azure/azure_client_wrapper.hpp>
#include <arcticdb/storage/azure/azure_mock_client.hpp>
#include <arcticdb/storage/object_store_utils.hpp>

namespace arcticdb::storage::azure {

std::string MockAzureClient::get_failure_trigger(
        const std::string& blob_name,
        StorageOperation operation_to_fail,
        const std::string& error_code,
        Azure::Core::Http::HttpStatusCode error_to_fail_with) {
    return fmt::format("{}#Failure_{}_{}_{}", blob_name, operation_to_string(operation_to_fail), error_code, static_cast<int>(error_to_fail_with));
}

Azure::Core::RequestFailedException get_exception(const std::string& message, const std::string& error_code, Azure::Core::Http::HttpStatusCode status_code) {
    auto rawResponse = std::make_unique<Azure::Core::Http::RawResponse>(0, 0, status_code, message);
    rawResponse->SetHeader("x-ms-error-code", error_code);
    auto exception = Azure::Core::RequestFailedException(rawResponse);
    exception.ErrorCode = error_code;

    return exception;
}

std::optional<Azure::Core::RequestFailedException> MockAzureClient::has_failure_trigger(const std::string& blob_name, StorageOperation operation) const {
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = blob_name.rfind(failure_string_for_operation);
    if (position == std::string::npos)
        return std::nullopt;

    try {
        auto start = position + failure_string_for_operation.size();
        auto error_code = blob_name.substr(start, blob_name.find_last_of('_') - start);
        auto status_code_string = blob_name.substr(blob_name.find_last_of('_') + 1);
        auto status_code = Azure::Core::Http::HttpStatusCode(std::stoi(status_code_string));
        auto error_message = fmt::format("Simulated Error, message: operation {}, error code {} statuscode {}",
                                         operation_to_string(operation), error_code, static_cast<int>(status_code));

        return get_exception(error_message, error_code, status_code);
    } catch (std::exception&) {
        return std::nullopt;
    }
}

Azure::Core::RequestFailedException MockAzureClient::missing_key_failure() const {
    auto error_code = AzureErrorCode_to_string(AzureErrorCode::BlobNotFound);
    std::string message = fmt::format("Simulated Error, message: Failed to find blob, {} {}", error_code,
                                      static_cast<int>(Azure::Core::Http::HttpStatusCode::NotFound));
    return get_exception(message, error_code, Azure::Core::Http::HttpStatusCode::NotFound);
}

template<typename Output>
void throw_if_error(StorageResult<Output, Azure::Core::RequestFailedException> result) {
    if (!result.is_success()) {
        throw result.get_error();
    }
}

bool MockAzureClient::matches_prefix(const std::string& blob_name, const std::string& prefix) const {
    return blob_name.find(prefix) == 0;
}

void MockAzureClient::write_blob(
        const std::string& blob_name,
        arcticdb::Segment&& segment,
        const Azure::Storage::Blobs::UploadBlockBlobFromOptions&,
        unsigned int) {
    throw_if_error(write_internal(blob_name, std::move(segment)));
}

Segment MockAzureClient::read_blob(
        const std::string& blob_name,
        const Azure::Storage::Blobs::DownloadBlobToOptions&,
        unsigned int) {
    auto result = read_internal(blob_name);
    throw_if_error(result);
    return result.get_output();
}

void MockAzureClient::delete_blobs(
        const std::vector<std::string>& blob_names,
        unsigned int) {
    throw_if_error(delete_internal(blob_names));
}

bool MockAzureClient::blob_exists(const std::string& blob_name) {
    auto result = exists_internal(blob_name);
    throw_if_error(result);
    return result.get_output();
}

Azure::Storage::Blobs::ListBlobsPagedResponse MockAzureClient::list_blobs(const std::string& prefix) {
    auto result = list_internal(prefix);
    throw_if_error(result);
    Azure::Storage::Blobs::ListBlobsPagedResponse output;
    for(auto& blob_name : result.get_output()) {
        Azure::Storage::Blobs::Models::BlobItem blobItem;
        blobItem.Name = blob_name;
        output.Blobs.push_back(blobItem);
    }
    return output;
}

}
