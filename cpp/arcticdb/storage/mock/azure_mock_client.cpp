/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/storage/mock/azure_mock_client.hpp>
#include <arcticdb/storage/object_store_utils.hpp>

#include <azure/core/http/curl_transport.hpp>
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

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

std::optional<Azure::Core::RequestFailedException> has_failure_trigger(const std::string& blob_name, StorageOperation operation) {
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = blob_name.rfind(failure_string_for_operation);
    if (position == std::string::npos)
        return std::nullopt;

    try {
        auto start = position + failure_string_for_operation.size();
        auto error_code = blob_name.substr(start, blob_name.find_last_of('_') - start);
        auto status_code_string = blob_name.substr(blob_name.find_last_of('_') + 1);
        auto status_code = Azure::Core::Http::HttpStatusCode(std::stoi(status_code_string));
        auto error_message = fmt::format("Simulated Error, message: operation {}, blob name {} error code {} statuscode {}",
                                         operation_to_string(operation), blob_name, error_code, static_cast<int>(status_code));

        return get_exception(error_message, error_code, status_code);
    } catch (std::exception&) {
        return std::nullopt;
    }
}

void MockAzureClient::write_blob(
        const std::string& blob_name,
        arcticdb::Segment&& segment,
        const Azure::Storage::Blobs::UploadBlockBlobFromOptions&,
        unsigned int) {

    auto maybe_exception = has_failure_trigger(blob_name, StorageOperation::WRITE);
    if (maybe_exception.has_value()) {
        throw *maybe_exception;
    }

    azure_contents.insert_or_assign(blob_name, std::move(segment));
}

Segment MockAzureClient::read_blob(
        const std::string& blob_name,
        const Azure::Storage::Blobs::DownloadBlobToOptions&,
        unsigned int) {

    auto maybe_exception = has_failure_trigger(blob_name, StorageOperation::READ);
    if (maybe_exception.has_value()) {
        throw *maybe_exception;
    }

    auto pos = azure_contents.find(blob_name);
    if (pos == azure_contents.end()) {
        auto error_code = AzureErrorCode_to_string(AzureErrorCode::BlobNotFound);
        std::string message = fmt::format("Simulated Error, message: Read failed {} {}", error_code, static_cast<int>(Azure::Core::Http::HttpStatusCode::NotFound));
        throw get_exception(message, error_code, Azure::Core::Http::HttpStatusCode::NotFound);
    }

    return std::move(pos->second);
}

void MockAzureClient::delete_blobs(
        const std::vector<std::string>& blob_names,
        unsigned int) {
    for (auto& blob_name : blob_names) {
        auto maybe_exception = has_failure_trigger(blob_name, StorageOperation::DELETE);
        if (maybe_exception.has_value()) {
            throw *maybe_exception;
        }
    }

    for (auto& blob_name : blob_names) {
        azure_contents.erase(blob_name);
    }
}

bool MockAzureClient::blob_exists(const std::string& blob_name) {
    auto maybe_exception = has_failure_trigger(blob_name, StorageOperation::EXISTS);
    if (maybe_exception.has_value()) {
        throw *maybe_exception;
    }

    return azure_contents.find(blob_name) != azure_contents.end();
}

Azure::Storage::Blobs::ListBlobsPagedResponse MockAzureClient::list_blobs(const std::string& prefix) {
    Azure::Storage::Blobs::ListBlobsPagedResponse output;
    for (auto& key : azure_contents){
        if (key.first.rfind(prefix, 0) == 0){
            auto blob_name = key.first;

            auto maybe_exception = has_failure_trigger(blob_name, StorageOperation::LIST);
            if (maybe_exception.has_value()) {
                throw *maybe_exception;
            }

            Azure::Storage::Blobs::Models::BlobItem blobItem;
            blobItem.Name = blob_name;
            output.Blobs.push_back(blobItem);
        }
    }

    return output;
}

}
