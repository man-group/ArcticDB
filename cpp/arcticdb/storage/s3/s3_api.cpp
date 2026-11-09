/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_api.hpp>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/ec2_utils.hpp>
#include <algorithm>
#include <cstdarg>
#include <limits>
#include <vector>
#ifndef WIN32
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <arcticdb/toolbox/query_stats.hpp>
#include <chrono>
#include <optional>
#include <signal.h>
#endif

namespace arcticdb::storage::s3 {

namespace {
constexpr const char* EventLoopAllocationTag = "ArcticDBS3EventLoop";

spdlog::level::level_enum to_spdlog_level(Aws::Utils::Logging::LogLevel log_level) {
    switch (log_level) {
    case Aws::Utils::Logging::LogLevel::Fatal:
        return spdlog::level::critical;
    case Aws::Utils::Logging::LogLevel::Error:
        return spdlog::level::err;
    case Aws::Utils::Logging::LogLevel::Warn:
        return spdlog::level::warn;
    case Aws::Utils::Logging::LogLevel::Info:
        return spdlog::level::info;
    case Aws::Utils::Logging::LogLevel::Debug:
        return spdlog::level::debug;
    case Aws::Utils::Logging::LogLevel::Trace:
        return spdlog::level::trace;
    default:
        return spdlog::level::off;
    }
}
} // namespace

void SpdlogLogSystem::Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str, ...) {
    va_list args;
    va_start(args, format_str);
    vaLog(log_level, tag, format_str, args);
    va_end(args);
}

void SpdlogLogSystem::vaLog(
        Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str, va_list args
) {
    va_list args_copy;
    va_copy(args_copy, args);
    const int length = std::vsnprintf(nullptr, 0, format_str, args_copy);
    va_end(args_copy);
    if (length < 0) {
        return;
    }
    std::vector<char> buffer(static_cast<size_t>(length) + 1);
    std::vsnprintf(buffer.data(), buffer.size(), format_str, args);
    log::s3().log(to_spdlog_level(log_level), "[{}] {}", tag, buffer.data());
}

void SpdlogLogSystem::LogStream(
        Aws::Utils::Logging::LogLevel log_level, const char* tag, const Aws::OStringStream& message_stream
) {
    log::s3().log(to_spdlog_level(log_level), "[{}] {}", tag, message_stream.str());
}

void SpdlogLogSystem::Flush() { log::s3().flush(); }

#ifndef WIN32
namespace {
constexpr const char* ARCTIC_CURL_ALLOCATION_TAG = "ArcticCurlHttpClient";
} // namespace

bool dns_shuffle_addresses_enabled() {
    return ConfigsMap::instance()->get_int("S3Storage.DnsShuffleAddresses", 0) != 0;
}

ArcticCurlHttpClient::ArcticCurlHttpClient(const Aws::Client::ClientConfiguration& client_configuration) :
    Aws::Http::CurlHttpClient(client_configuration),
    dns_shuffle_addresses_enabled_(dns_shuffle_addresses_enabled()) {}

bool ArcticCurlHttpClient::should_shuffle_dns_addresses() const { return dns_shuffle_addresses_enabled_; }

namespace {
// Called by curl on the attempt's thread for each socket it opens, so not for an attempt that reuses a connection
int count_new_connection(void*, curl_socket_t, curlsocktype purpose) {
    if (purpose == CURLSOCKTYPE_IPCXN) {
        query_stats::add_to_current_calls(query_stats::StatType::NEW_CONNECTIONS, 1);
    }
    return CURL_SOCKOPT_OK;
}

std::optional<int64_t> request_metric(
        const Aws::Http::HttpRequest& request, Aws::Monitoring::HttpClientMetricsType metric_type
) {
    const auto& metrics = request.GetRequestMetrics();
    if (auto it = metrics.find(Aws::Monitoring::GetHttpClientMetricNameByType(metric_type)); it != metrics.end()) {
        return it->second;
    }
    return std::nullopt;
}

// Records the query stats of one HTTP attempt, see query_stats::StatType
void add_attempt_query_stats(
        const Aws::Http::HttpRequest& request, const Aws::Http::HttpResponse& response, int64_t attempt_time_ns
) {
    using query_stats::add_to_current_calls;
    using query_stats::StatType;
    add_to_current_calls(StatType::ATTEMPTS, 1);
    add_to_current_calls(StatType::ATTEMPT_TIME_MS, static_cast<uint64_t>(std::max<int64_t>(attempt_time_ns, 0)));
    // Set for curl errors (DNS, connect, timeouts, ...) and for an incomplete response body
    if (response.HasClientError()) {
        add_to_current_calls(StatType::NETWORK_ERRORS, 1);
        return;
    }
    const auto response_code = static_cast<int>(response.GetResponseCode());
    if (response_code >= 500 || response_code == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS)) {
        add_to_current_calls(StatType::SERVER_ERRORS, 1);
    }
    // The SDK names CURLINFO_STARTTRANSFER_TIME TimeToFirstByte and CURLINFO_PRETRANSFER_TIME TimeToConnect, both in ms
    using Aws::Monitoring::HttpClientMetricsType;
    if (auto ttfb_ms = request_metric(request, HttpClientMetricsType::TimeToFirstByte); ttfb_ms && *ttfb_ms > 0) {
        add_to_current_calls(StatType::TTFB_MS, static_cast<uint64_t>(*ttfb_ms));
    }
    if (auto connect_ms = request_metric(request, HttpClientMetricsType::TimeToConnect);
        connect_ms && *connect_ms > 0) {
        add_to_current_calls(StatType::CONNECT_MS, static_cast<uint64_t>(*connect_ms));
    }
}
} // namespace

std::shared_ptr<Aws::Http::HttpResponse> ArcticCurlHttpClient::MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* read_limiter,
        Aws::Utils::RateLimits::RateLimiterInterface* write_limiter
) const {
    if (!query_stats::QueryStats::instance()->is_enabled()) {
        return Aws::Http::CurlHttpClient::MakeRequest(request, read_limiter, write_limiter);
    }
    const auto start = std::chrono::steady_clock::now();
    auto response = Aws::Http::CurlHttpClient::MakeRequest(request, read_limiter, write_limiter);
    const auto attempt_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
    if (response) {
        add_attempt_query_stats(*request, *response, attempt_time_ns);
    }
    return response;
}

void ArcticCurlHttpClient::OverrideOptionsOnConnectionHandle(CURL* connection_handle) const {
    if (should_shuffle_dns_addresses()) {
        curl_easy_setopt(connection_handle, CURLOPT_DNS_SHUFFLE_ADDRESSES, 1L);
    }
    // The SDK resets the handle before reusing it, so this is set for every attempt
    if (query_stats::QueryStats::instance()->is_enabled()) {
        curl_easy_setopt(connection_handle, CURLOPT_SOCKOPTFUNCTION, count_new_connection);
    }
}

ArcticCurlHttpClientFactory::ArcticCurlHttpClientFactory(bool init_and_cleanup_curl, bool install_sigpipe_handler) :
    init_and_cleanup_curl_(init_and_cleanup_curl),
    install_sigpipe_handler_(install_sigpipe_handler) {}

std::shared_ptr<Aws::Http::HttpClient> ArcticCurlHttpClientFactory::CreateHttpClient(
        const Aws::Client::ClientConfiguration& client_configuration
) const {
    return Aws::MakeShared<ArcticCurlHttpClient>(ARCTIC_CURL_ALLOCATION_TAG, client_configuration);
}

std::shared_ptr<Aws::Http::HttpRequest> ArcticCurlHttpClientFactory::CreateHttpRequest(
        const Aws::String& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory
) const {
    return CreateHttpRequest(Aws::Http::URI(uri), method, stream_factory);
}

std::shared_ptr<Aws::Http::HttpRequest> ArcticCurlHttpClientFactory::CreateHttpRequest(
        const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory
) const {
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(ARCTIC_CURL_ALLOCATION_TAG, uri, method);
    request->SetResponseStreamFactory(stream_factory);
    return request;
}

void ArcticCurlHttpClientFactory::InitStaticState() {
    if (init_and_cleanup_curl_) {
        Aws::Http::CurlHttpClient::InitGlobalState();
    }
    if (install_sigpipe_handler_) {
        ::signal(SIGPIPE, [](int) {});
    }
}

void ArcticCurlHttpClientFactory::CleanupStaticState() {
    if (init_and_cleanup_curl_) {
        Aws::Http::CurlHttpClient::CleanupGlobalState();
    }
}
#endif // WIN32

uint16_t event_loop_thread_count_from_config() {
    const auto configured = ConfigsMap::instance()->get_int("AWS.EventLoopThreads", 0);
    return static_cast<uint16_t>(std::clamp<int64_t>(configured, 0, std::numeric_limits<uint16_t>::max()));
}

ClientBootstrapFactory make_client_bootstrap_factory(uint16_t event_loop_thread_count) {
    // Mirrors Aws::InitAPI's own default construction, apart from the explicit thread count.
    return [event_loop_thread_count]() {
        Aws::Crt::Io::EventLoopGroup event_loop_group(event_loop_thread_count);
        Aws::Crt::Io::DefaultHostResolver host_resolver(event_loop_group, 8, 30);
        auto client_bootstrap =
                Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(EventLoopAllocationTag, event_loop_group, host_resolver);
        client_bootstrap->EnableBlockingShutdown();
        return client_bootstrap;
    };
}

S3ApiInstance::S3ApiInstance(
        Aws::Utils::Logging::LogLevel log_level, bool log_to_file, uint16_t event_loop_thread_count
) :
    log_level_(log_level),
    options_() {
    // Use correct URI encoding rather than legacy compat one in AWS SDK. PURE S3 needs this to handle symbol names
    // that have special characters (eg ':').
    options_.httpOptions.compliantRfc3986Encoding = true;

    options_.ioOptions.clientBootstrap_create_fn = make_client_bootstrap_factory(event_loop_thread_count);

    if (log_level_ > Aws::Utils::Logging::LogLevel::Off) {
        if (log_to_file) {
            Aws::Utils::Logging::InitializeAWSLogging(
                    Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>("v", log_level, "aws_sdk_")
            );
        } else {
            Aws::Utils::Logging::InitializeAWSLogging(Aws::MakeShared<SpdlogLogSystem>("v", log_level));
        }
    }
#ifndef WIN32
    {
        const bool init_and_cleanup_curl = options_.httpOptions.initAndCleanupCurl;
        const bool install_sigpipe_handler = options_.httpOptions.installSigPipeHandler;
        options_.httpOptions.httpClientFactory_create_fn = [init_and_cleanup_curl, install_sigpipe_handler] {
            return Aws::MakeShared<ArcticCurlHttpClientFactory>(
                    ARCTIC_CURL_ALLOCATION_TAG, init_and_cleanup_curl, install_sigpipe_handler
            );
        };
    }
#endif
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Begin initializing AWS API");
    Aws::InitAPI(options_);
    // A workaround for https://github.com/aws/aws-sdk-cpp/issues/1410.
    if (is_running_inside_aws_fast()) {
        return;
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Does not appear to be using AWS. Will set AWS_EC2_METADATA_DISABLED");
#ifdef WIN32
    _putenv_s("AWS_EC2_METADATA_DISABLED", "true");
#else
    setenv("AWS_EC2_METADATA_DISABLED", "true", true);
#endif
}

S3ApiInstance::~S3ApiInstance() {
    if (log_level_ > Aws::Utils::Logging::LogLevel::Off)
        Aws::Utils::Logging::ShutdownAWSLogging();

    // Aws::ShutdownAPI(options_); This causes a crash on shutdown in Aws::CleanupMonitoring
}

void S3ApiInstance::init() {
    auto log_level = ConfigsMap::instance()->get_int("AWS.LogLevel", 0);
    auto log_to_file = ConfigsMap::instance()->get_int("AWS.LogToFile", 0) != 0;
    S3ApiInstance::instance_ = std::make_shared<S3ApiInstance>(
            Aws::Utils::Logging::LogLevel(log_level), log_to_file, event_loop_thread_count_from_config()
    );
}

std::shared_ptr<S3ApiInstance> S3ApiInstance::instance() {
    std::call_once(S3ApiInstance::init_flag_, &S3ApiInstance::init);
    return instance_;
}

void S3ApiInstance::destroy_instance() { S3ApiInstance::instance_.reset(); }

std::shared_ptr<S3ApiInstance> S3ApiInstance::instance_;
std::once_flag S3ApiInstance::init_flag_;

} // namespace arcticdb::storage::s3
