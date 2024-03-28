/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <azure/core/http/curl_transport.hpp>

#include <arcticdb/storage/azure/azure_real_client.hpp>
#include <arcticdb/storage/azure/azure_client_wrapper.hpp>
#include <arcticdb/storage/object_store_utils.hpp>

namespace arcticdb::storage {
using namespace object_store_utils;
namespace azure {
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

Azure::Core::Context get_context(unsigned int request_timeout){
    Azure::Core::Context requestContext; //TODO: Maybe can be static but need to be careful with its shared_ptr and ContextSharedState
    return requestContext.WithDeadline(std::chrono::system_clock::now() + std::chrono::milliseconds(request_timeout));
}


RealAzureClient::RealAzureClient(const Config &conf) :
container_client(BlobContainerClient::CreateFromConnectionString(conf.endpoint(), conf.container_name(), get_client_options(conf))) { }

Azure::Storage::Blobs::BlobClientOptions RealAzureClient::get_client_options(const Config &conf) {
    BlobClientOptions client_options;
    if (!conf.ca_cert_path().empty()) {//WARNING: Setting ca_cert_path will force Azure sdk uses libcurl as backend support, instead of winhttp
        Azure::Core::Http::CurlTransportOptions curl_transport_options;
        curl_transport_options.CAInfo = conf.ca_cert_path();
        client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>(curl_transport_options);
    }
    return client_options;
}

void RealAzureClient::write_blob(
        const std::string& blob_name,
        Segment&& segment,
        const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option,
        unsigned int request_timeout) {

    std::shared_ptr<Buffer> tmp;
    auto hdr_size = segment.segment_header_bytes_size();
    auto [dst, write_size] = segment.try_internal_write(tmp, hdr_size);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + hdr_size + segment.buffer().bytes() <= write_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                hdr_size,
                segment.buffer().bytes(),
                write_size);
    ARCTICDB_SUBSAMPLE(AzureStorageUploadObject, 0)
    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Writing key '{}' with {} bytes of data",
                           blob_name,
                           segment.total_segment_size(hdr_size));
    blob_client.UploadFrom(dst, write_size, upload_option, get_context(request_timeout));
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Wrote key '{}' with {} bytes of data",
                           blob_name,
                           segment.total_segment_size(hdr_size));
}

Segment RealAzureClient::read_blob(
        const std::string& blob_name,
        const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
        unsigned int request_timeout) {

    ARCTICDB_DEBUG(log::storage(), "Looking for blob {}", blob_name);
    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    auto properties = blob_client.GetProperties(Azure::Storage::Blobs::GetBlobPropertiesOptions{}, get_context(request_timeout)).Value;
    std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>(properties.BlobSize);
    blob_client.DownloadTo(buffer->preamble(), buffer->available(), download_option, get_context(request_timeout));
    ARCTICDB_SUBSAMPLE(AzureStorageVisitSegment, 0)

    return Segment::from_buffer(std::move(buffer));
}

void RealAzureClient::delete_blobs(
        const std::vector<std::string>& blob_names,
        unsigned int request_timeout) {

    util::check(blob_names.size() <= BATCH_SUBREQUEST_LIMIT,
                "Azure delete batch size {} exceeds maximum permitted batch size of {}",
                blob_names.size(),
                BATCH_SUBREQUEST_LIMIT);
    auto batch = container_client.CreateBatch();

    for (auto& blob_name: blob_names) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing azure blob with key {}", blob_names);
        batch.DeleteBlob(blob_name);
    }

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Submitting DeleteBlob batch");
    ARCTICDB_SUBSAMPLE(AzureStorageDeleteObjects, 0)
    container_client.SubmitBatch(batch, Azure::Storage::Blobs::SubmitBlobBatchOptions(), get_context(request_timeout));//To align with s3 behaviour, deleting non-exist objects is not an error, so not handling response
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

}

}
