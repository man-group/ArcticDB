/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/**
 * Azure Transport Selection:
 *
 * This file implements platform-specific HTTP transport selection for Azure storage operations:
 *
 * - On Windows (_WIN32):
 *   - Always uses WinHTTP (native Windows HTTP client)
 *   - CA certificate settings are not supported as WinHTTP uses Windows certificate store
 *   - Custom CA certificates must be installed via Windows Certificate Manager (certmgr.msc)
 *   - This provides better performance and integration with Windows security
 *
 * - On macOS (__APPLE__):
 *   - Always uses libcurl as the HTTP transport
 *   - CA certificate settings are not supported
 *   - Custom CA certificates must be installed via Keychain Access
 *
 * - On Linux:
 *   - Always uses libcurl as the HTTP transport
 *   - Supports custom CA certificate configuration via ca_cert_path and ca_cert_dir
 *
 * The selection is done at compile time via preprocessor directives to ensure
 * optimal performance and minimal runtime overhead. The appropriate transport
 * header is included based on the platform, and the transport is configured
 * in get_client_options().
 */

#if defined(_WIN32)
#include <azure/core/http/win_http_transport.hpp>
#else
#include <azure/core/http/curl_transport.hpp>
#endif

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <arcticdb/storage/azure/azure_client_impl.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/util/error_code.hpp>

namespace arcticdb::storage {
using namespace object_store_utils;
namespace azure {
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

Azure::Core::Context get_context(unsigned int request_timeout) {
    Azure::Core::Context requestContext; // TODO: Maybe can be static but need to be careful with its shared_ptr and
                                         // ContextSharedState
    return requestContext.WithDeadline(std::chrono::system_clock::now() + std::chrono::milliseconds(request_timeout));
}

RealAzureClient::RealAzureClient(const Config& conf) :
    container_client(BlobContainerClient::CreateFromConnectionString(
            conf.endpoint(), conf.container_name(), get_client_options(conf)
    )) {}

Azure::Storage::Blobs::BlobClientOptions RealAzureClient::get_client_options(const Config& conf) {
    BlobClientOptions client_options;

#if defined(_WIN32)
    // On Windows, always use WinHTTP
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using WinHTTP transport");
    if (conf.ca_cert_path().empty() && conf.ca_cert_dir().empty()) {
        client_options.Transport.Transport = std::make_shared<Azure::Core::Http::WinHttpTransport>();
    } else {
        throw ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>(
            "CA certificate settings are not supported on Windows. "
            "Please use Windows Certificate Manager (certmgr.msc) to manage certificates:\n"
            "1. Open Certificate Manager (certmgr.msc)\n"
            "2. Navigate to 'Trusted Root Certification Authorities'\n"
            "3. Right-click and select 'All Tasks > Import'\n"
            "4. Select your certificate file and follow the import wizard\n"
            "5. Ensure 'Place all certificates in the following store' is selected\n"
            "6. Complete the import by clicking 'Next' and 'Finish'"
        );
    }
#elif defined(__APPLE__)
    // On macOS, always use libcurl but ignore CA cert paths
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using libcurl transport");
    if (conf.ca_cert_path().empty() && conf.ca_cert_dir().empty()) {
        client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>();
    } else {
        throw ArcticSpecificException<ErrorCode::E_INVALID_USER_ARGUMENT>(
            "CA certificate settings are not supported on macOS. "
            "Please use Keychain Access to manage certificates:\n"
            "1. Open Keychain Access (Applications > Utilities > Keychain Access)\n"
            "2. Select 'System' keychain from the left sidebar\n"
            "3. Click File > Import Items\n"
            "4. Select your certificate file\n"
            "5. Enter your keychain password if prompted\n"
            "6. The certificate will be added to the System keychain"
        );
    }
#else
    // On Linux, use libcurl with CA cert configuration
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using libcurl transport");
    Azure::Core::Http::CurlTransportOptions curl_transport_options;
    if (!conf.ca_cert_path().empty()) {
        curl_transport_options.CAInfo = conf.ca_cert_path();
    }
    if (!conf.ca_cert_dir().empty()) {
        curl_transport_options.CAPath = conf.ca_cert_dir();
    }
    client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>(curl_transport_options);
#endif

    return client_options;
}

void RealAzureClient::write_blob(
        const std::string& blob_name, Segment& segment,
        const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option, unsigned int request_timeout
) {

    auto [dst, write_size, buffer] = segment.serialize_header();
    ARCTICDB_SUBSAMPLE(AzureStorageUploadObject, 0)
    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Writing key '{}' with {} bytes of data", blob_name, write_size);
    blob_client.UploadFrom(dst, write_size, upload_option, get_context(request_timeout));
}

Segment RealAzureClient::read_blob(
        const std::string& blob_name, const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
        unsigned int request_timeout
) {

    ARCTICDB_DEBUG(log::storage(), "Looking for blob {}", blob_name);
    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    auto properties =
            blob_client.GetProperties(Azure::Storage::Blobs::GetBlobPropertiesOptions{}, get_context(request_timeout))
                    .Value;
    std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>(properties.BlobSize);
    blob_client.DownloadTo(buffer->data(), buffer->available(), download_option, get_context(request_timeout));
    ARCTICDB_SUBSAMPLE(AzureStorageVisitSegment, 0)

    return Segment::from_buffer(std::move(buffer));
}

void RealAzureClient::delete_blobs(const std::vector<std::string>& blob_names, unsigned int request_timeout) {

    util::check(
            blob_names.size() <= BATCH_SUBREQUEST_LIMIT,
            "Azure delete batch size {} exceeds maximum permitted batch size of {}",
            blob_names.size(),
            BATCH_SUBREQUEST_LIMIT
    );
    auto batch = container_client.CreateBatch();

    for (auto& blob_name : blob_names) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing azure blob with key {}", blob_names);
        batch.DeleteBlob(blob_name);
    }

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Submitting DeleteBlob batch");
    ARCTICDB_SUBSAMPLE(AzureStorageDeleteObjects, 0)
    container_client.SubmitBatch(
            batch, Azure::Storage::Blobs::SubmitBlobBatchOptions(), get_context(request_timeout)
    ); // To align with s3 behaviour, deleting non-exist objects is not an error, so not handling response
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Submitted DeleteBlob batch");
}

ListBlobsPagedResponse RealAzureClient::list_blobs(const std::string& prefix) {
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Searching for objects with prefix {}", prefix);
    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = prefix;
    return container_client.ListBlobs(options);
}

bool RealAzureClient::blob_exists(const std::string& blob_name) {
    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    auto properties = blob_client.GetProperties().Value;
    return properties.ETag.HasValue();
}

} // namespace azure

} // namespace arcticdb::storage
