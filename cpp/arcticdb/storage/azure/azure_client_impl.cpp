/* Copyright 2026 Man Group Operations Limited
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
 *
 * Note, for conda-forge's builds, the distribution of `libcurl` uses CA certificates
 * provided by the `ca-certificates` package originated from the certifi python package.
 *
 * See:
 * https://github.com/conda-forge/ca-certificates-feedstock/blob/d13d63b3192ec707b514637930fd215d0776c604/recipe/meta.yaml#L8
 * https://github.com/conda-forge/curl-feedstock/blob/c6144ac9941ab00393a5a76954ddc19fab8005d1/recipe/build.sh#L19
 */

#if defined(_WIN32)
#include <azure/core/http/win_http_transport.hpp>
#else
#include <azure/core/http/curl_transport.hpp>
#endif

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <arcticdb/storage/azure/azure_client_impl.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/azure/azure_client_interface.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/error_code.hpp>

#include <chrono>

namespace arcticdb::storage {
using namespace object_store_utils;
namespace azure {
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

namespace {

std::string infer_azure_blob_operation(const Azure::Core::Http::Request& request) {
    const std::string method = request.GetMethod().ToString();
    const auto& query = request.GetUrl().GetQueryParameters();
    const auto comp_it = query.find("comp");
    const std::string_view comp = comp_it != query.end() ? std::string_view(comp_it->second) : std::string_view{};

    if (method == "GET") {
        if (comp == "list") {
            return "ListBlobs";
        }
        return "GetBlob";
    }
    if (method == "PUT") {
        if (comp == "block") {
            return "PutBlock";
        }
        if (comp == "blocklist") {
            return "PutBlockList";
        }
        return "PutBlob";
    }
    if (method == "HEAD") {
        return "GetBlobProperties";
    }
    if (method == "POST") {
        if (comp == "batch") {
            return "SubmitBatch";
        }
        return "POST";
    }
    if (method == "DELETE") {
        return "DeleteBlob";
    }
    return method;
}

query_stats::HttpStatusClass classify_azure_status(Azure::Core::Http::HttpStatusCode code_enum) noexcept {
    const int code = static_cast<int>(code_enum);
    if (code >= 200 && code < 300) {
        return query_stats::HttpStatusClass::Status2xx;
    }
    if (code >= 300 && code < 400) {
        return query_stats::HttpStatusClass::Status3xx;
    }
    if (code >= 400 && code < 500) {
        return query_stats::HttpStatusClass::Status4xx;
    }
    if (code >= 500 && code < 600) {
        return query_stats::HttpStatusClass::Status5xx;
    }
    return query_stats::HttpStatusClass::NetworkError;
}

struct AzureRequestTracker {
    std::string operation;
    query_stats::TimePoint last_attempt_end{};
    uint64_t attempts{0};
    bool has_prev_attempt_end{false};
    AzureRequestTracker* prev{nullptr};
};

thread_local AzureRequestTracker* tl_azure_req = nullptr;

class QueryStatsAzurePerOperationPolicy final : public Azure::Core::Http::Policies::HttpPolicy {
  public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
            Azure::Core::Http::Request& request, Azure::Core::Http::Policies::NextHttpPolicy nextPolicy,
            Azure::Core::Context const& context
    ) const override {
        if (!query_stats::is_active()) {
            return nextPolicy.Send(request, context);
        }

        const auto start = std::chrono::steady_clock::now();
        AzureRequestTracker tracker;
        tracker.operation = infer_azure_blob_operation(request);
        tracker.prev = tl_azure_req;
        tl_azure_req = &tracker;

        struct FinishGuard {
            AzureRequestTracker& tracker;
            query_stats::TimePoint start;
            ~FinishGuard() noexcept {
                tl_azure_req = tracker.prev;
                const auto total_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
                                .count()
                );
                query_stats::record_http_request_finish("azure_blob", tracker.operation, total_ns);
            }
        } finish_guard{tracker, start};

        return nextPolicy.Send(request, context);
    }

    std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> Clone() const override {
        return std::make_unique<QueryStatsAzurePerOperationPolicy>(*this);
    }
};

class QueryStatsAzurePerRetryPolicy final : public Azure::Core::Http::Policies::HttpPolicy {
  public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
            Azure::Core::Http::Request& request, Azure::Core::Http::Policies::NextHttpPolicy nextPolicy,
            Azure::Core::Context const& context
    ) const override {
        if (!query_stats::is_active()) {
            return nextPolicy.Send(request, context);
        }

        const auto attempt_start = std::chrono::steady_clock::now();
        auto* tracker = tl_azure_req;
        const std::string fallback_op = tracker != nullptr ? std::string{} : infer_azure_blob_operation(request);
        const std::string_view op = tracker != nullptr ? std::string_view(tracker->operation) : fallback_op;

        uint64_t backoff_ns = 0;
        if (tracker != nullptr && tracker->attempts > 0) {
            if (tracker->has_prev_attempt_end && attempt_start > tracker->last_attempt_end) {
                backoff_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(attempt_start - tracker->last_attempt_end)
                                .count()
                );
            }
            query_stats::record_http_retry("azure_blob", op, backoff_ns);
        }

        try {
            auto response = nextPolicy.Send(request, context);
            const auto attempt_end = std::chrono::steady_clock::now();
            const auto attempt_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(attempt_end - attempt_start).count()
            );
            const int status_code = response ? static_cast<int>(response->GetStatusCode()) : 0;
            const auto status_cls = response ? classify_azure_status(response->GetStatusCode())
                                             : query_stats::HttpStatusClass::NetworkError;
            const bool throttled = (status_code == 429 || status_code == 503);
            if (tracker != nullptr) {
                ++tracker->attempts;
                tracker->last_attempt_end = attempt_end;
                tracker->has_prev_attempt_end = true;
            }
            query_stats::record_http_attempt("azure_blob", op, status_cls, attempt_ns, 0, 0, throttled);
            return response;
        } catch (const Azure::Core::Http::TransportException&) {
            const auto attempt_end = std::chrono::steady_clock::now();
            const auto attempt_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(attempt_end - attempt_start).count()
            );
            if (tracker != nullptr) {
                ++tracker->attempts;
                tracker->last_attempt_end = attempt_end;
                tracker->has_prev_attempt_end = true;
            }
            query_stats::record_http_attempt(
                    "azure_blob", op, query_stats::HttpStatusClass::NetworkError, attempt_ns, 0, 0
            );
            throw;
        }
    }

    std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> Clone() const override {
        return std::make_unique<QueryStatsAzurePerRetryPolicy>(*this);
    }
};

} // namespace

void install_query_stats_azure_policies(Azure::Storage::Blobs::BlobClientOptions& options) {
    options.PerOperationPolicies.push_back(std::make_unique<QueryStatsAzurePerOperationPolicy>());
    options.PerRetryPolicies.push_back(std::make_unique<QueryStatsAzurePerRetryPolicy>());
}

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
    // Reusing a pooled connection that the server has already closed makes the next request hang for ~2 minutes
    // before it is retried. Azurite (node) closes idle connections after 5s, so tests hit this regularly.
    curl_transport_options.HttpKeepAlive = ConfigsMap::instance()->get_int("AzureStorage.HttpKeepAlive", 1) != 0;
    if (!conf.ca_cert_path().empty()) {
        curl_transport_options.CAInfo = conf.ca_cert_path();
    }
    if (!conf.ca_cert_dir().empty()) {
        curl_transport_options.CAPath = conf.ca_cert_dir();
    }
    client_options.Transport.Transport = std::make_shared<Azure::Core::Http::CurlTransport>(curl_transport_options);
#endif

    install_query_stats_azure_policies(client_options);
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
