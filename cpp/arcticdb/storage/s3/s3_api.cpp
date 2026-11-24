/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/ec2_utils.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <aws/core/monitoring/CoreMetrics.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/monitoring/MonitoringFactory.h>
#include <aws/core/monitoring/MonitoringInterface.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>

#include <algorithm>
#include <chrono>
#include <cstdarg>
#include <limits>
#include <string>
#include <vector>
#ifndef WIN32
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <curl/curl.h>
#include <signal.h>
#endif

namespace arcticdb::storage::s3 {

namespace {
constexpr const char* EventLoopAllocationTag = "ArcticDBS3EventLoop";
constexpr const char* MonitoringAllocationTag = "ArcticDBQueryStatsMonitor";

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

#ifndef WIN32
struct CurlAttemptTelemetry {
    bool tracking{false};
    bool valid{false};
    query_stats::TimePoint start_tp{};
    uint64_t duration_ns{0};
    uint64_t new_connections{0};
};

thread_local CurlAttemptTelemetry tl_curl_attempt{};

int sockopt_count_new_connection(void*, curl_socket_t, curlsocktype purpose) {
    if (purpose == CURLSOCKTYPE_IPCXN && tl_curl_attempt.tracking) {
        ++tl_curl_attempt.new_connections;
    }
    return CURL_SOCKOPT_OK;
}
#endif

struct ActiveAwsOpSlot {
    std::string request_name;
    ActiveAwsOpSlot* prev{nullptr};
};

thread_local ActiveAwsOpSlot* tl_active_aws_op = nullptr;

int extract_http_status_code(const Aws::Client::HttpResponseOutcome& outcome) noexcept {
    if (outcome.IsSuccess() && outcome.GetResult() != nullptr) {
        return static_cast<int>(outcome.GetResult()->GetResponseCode());
    }
    return static_cast<int>(outcome.GetError().GetResponseCode());
}

query_stats::HttpStatusClass classify_http_status_code(int code) noexcept {
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

bool is_throttled_http_status(int code) noexcept { return code == 429 || code == 503; }

struct AwsMonitorRequestContext {
    query_stats::TimePoint request_start;
    query_stats::TimePoint attempt_start;
    query_stats::TimePoint last_attempt_end;
    uint64_t total_attempt_ns{0};
    uint64_t measured_backoff_ns{0};
    uint64_t attempts{0};
    uint64_t retries{0};
    bool has_prev_attempt_end{false};
    ActiveAwsOpSlot op_slot;
};

class QueryStatsAwsMonitor final : public Aws::Monitoring::MonitoringInterface {
  public:
    void*
    OnRequestStarted(const Aws::String&, const Aws::String& requestName, const std::shared_ptr<const Aws::Http::HttpRequest>&)
            const override {
        if (!query_stats::is_active()) {
            return nullptr;
        }
        const auto now = std::chrono::steady_clock::now();
        auto* ctx = new AwsMonitorRequestContext{
                .request_start = now,
                .attempt_start = now,
                .last_attempt_end = now,
                .total_attempt_ns = 0,
                .measured_backoff_ns = 0,
                .attempts = 0,
                .retries = 0,
                .has_prev_attempt_end = false,
                .op_slot = ActiveAwsOpSlot{std::string(requestName.c_str(), requestName.size()), tl_active_aws_op}
        };
        tl_active_aws_op = &ctx->op_slot;
        return ctx;
    }

    void OnRequestSucceeded(
            const Aws::String& serviceName, const Aws::String& requestName,
            const std::shared_ptr<const Aws::Http::HttpRequest>&, const Aws::Client::HttpResponseOutcome& outcome,
            const Aws::Monitoring::CoreMetricsCollection& metricsFromCore, void* context
    ) const override {
        record_attempt(serviceName, requestName, outcome, metricsFromCore, context);
    }

    void OnRequestFailed(
            const Aws::String& serviceName, const Aws::String& requestName,
            const std::shared_ptr<const Aws::Http::HttpRequest>&, const Aws::Client::HttpResponseOutcome& outcome,
            const Aws::Monitoring::CoreMetricsCollection& metricsFromCore, void* context
    ) const override {
        record_attempt(serviceName, requestName, outcome, metricsFromCore, context);
    }

    void OnRequestRetry(
            const Aws::String& serviceName, const Aws::String& requestName,
            const std::shared_ptr<const Aws::Http::HttpRequest>&, void* context
    ) const override {
        auto* ctx = static_cast<AwsMonitorRequestContext*>(context);
        if (ctx == nullptr) {
            return;
        }
        ++ctx->retries;
        ctx->attempt_start = std::chrono::steady_clock::now();
        query_stats::record_http_retry(
                std::string_view(serviceName.c_str(), serviceName.size()),
                std::string_view(requestName.c_str(), requestName.size()),
                0
        );
    }

    void OnFinish(
            const Aws::String& serviceName, const Aws::String& requestName,
            const std::shared_ptr<const Aws::Http::HttpRequest>&, void* context
    ) const override {
        auto* ctx = static_cast<AwsMonitorRequestContext*>(context);
        if (ctx == nullptr) {
            return;
        }
        if (tl_active_aws_op == &ctx->op_slot) {
            tl_active_aws_op = ctx->op_slot.prev;
        }
        const auto total_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                            std::chrono::steady_clock::now() - ctx->request_start
        )
                                                            .count());
        // On non-curl platforms (e.g. Windows), fall back to residual time when retries occurred.
        uint64_t extra_backoff_ns = 0;
        if (ctx->retries > 0 && ctx->measured_backoff_ns == 0 && total_ns > ctx->total_attempt_ns) {
            extra_backoff_ns = total_ns - ctx->total_attempt_ns;
        }
        query_stats::record_http_request_finish(
                std::string_view(serviceName.c_str(), serviceName.size()),
                std::string_view(requestName.c_str(), requestName.size()),
                total_ns,
                extra_backoff_ns
        );
        delete ctx;
    }

  private:
    static void record_attempt(
            const Aws::String& serviceName, const Aws::String& requestName,
            const Aws::Client::HttpResponseOutcome& outcome,
            const Aws::Monitoring::CoreMetricsCollection& metricsFromCore, void* context
    ) {
        auto* ctx = static_cast<AwsMonitorRequestContext*>(context);
        if (ctx == nullptr) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        uint64_t attempt_ns = 0;
        uint64_t new_conns = 0;
        uint64_t backoff_ns = 0;
        bool used_curl = false;

#ifndef WIN32
        if (tl_curl_attempt.valid) {
            attempt_ns = tl_curl_attempt.duration_ns;
            new_conns = tl_curl_attempt.new_connections;
            if (ctx->has_prev_attempt_end && tl_curl_attempt.start_tp > ctx->last_attempt_end) {
                backoff_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                           tl_curl_attempt.start_tp - ctx->last_attempt_end
                )
                                                           .count());
                ctx->measured_backoff_ns += backoff_ns;
            }
            tl_curl_attempt.valid = false;
            used_curl = true;
        }
#endif
        if (!used_curl) {
            const auto latency_key = Aws::Monitoring::GetHttpClientMetricNameByType(
                    Aws::Monitoring::HttpClientMetricsType::RequestLatency
            );
            if (auto it = metricsFromCore.httpClientMetrics.find(latency_key);
                it != metricsFromCore.httpClientMetrics.end() && it->second >= 0) {
                attempt_ns = static_cast<uint64_t>(it->second) * 1'000'000ULL;
            } else {
                attempt_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(now - ctx->attempt_start).count()
                );
            }
            const auto reused_key = Aws::Monitoring::GetHttpClientMetricNameByType(
                    Aws::Monitoring::HttpClientMetricsType::ConnectionReused
            );
            if (auto it = metricsFromCore.httpClientMetrics.find(reused_key);
                it != metricsFromCore.httpClientMetrics.end() && it->second == 0) {
                new_conns = 1;
            }
        }

        ++ctx->attempts;
        ctx->total_attempt_ns += attempt_ns;
        ctx->last_attempt_end = now;
        ctx->has_prev_attempt_end = true;

        static const auto dns_key =
                Aws::Monitoring::GetHttpClientMetricNameByType(Aws::Monitoring::HttpClientMetricsType::DnsLatency);
        static const auto tcp_key =
                Aws::Monitoring::GetHttpClientMetricNameByType(Aws::Monitoring::HttpClientMetricsType::TcpLatency);
        static const auto ssl_key =
                Aws::Monitoring::GetHttpClientMetricNameByType(Aws::Monitoring::HttpClientMetricsType::SslLatency);
        static const auto ttfb_key =
                Aws::Monitoring::GetHttpClientMetricNameByType(Aws::Monitoring::HttpClientMetricsType::TimeToFirstByte);

        query_stats::HttpAttemptTimings timings{};
        const auto& core_metrics = metricsFromCore.httpClientMetrics;
        if (auto it = core_metrics.find(dns_key); it != core_metrics.end() && it->second >= 0) {
            timings.dns_ns = static_cast<uint64_t>(it->second) * 1'000'000ULL;
            timings.has_timings = true;
        }
        if (auto it = core_metrics.find(tcp_key); it != core_metrics.end() && it->second >= 0) {
            timings.tcp_connect_ns = static_cast<uint64_t>(it->second) * 1'000'000ULL;
            timings.has_timings = true;
        }
        if (auto it = core_metrics.find(ssl_key); it != core_metrics.end() && it->second >= 0) {
            // Upstream aws-sdk-cpp CurlHttpClient bug (libcurl >= 7.61.0): CURLINFO_APPCONNECT_TIME_T
            // returns microseconds, yet aws-sdk-cpp multiplies by 1000 as if it were seconds, emitting
            // nanoseconds instead of milliseconds in SslLatency. Detect and handle both units safely.
            const uint64_t raw = static_cast<uint64_t>(it->second);
            const uint64_t scaled = raw * 1'000'000ULL;
            timings.tls_handshake_ns =
                    (attempt_ns > 0 && scaled > attempt_ns * 2ULL && raw <= attempt_ns * 2ULL) ? raw : scaled;
            timings.has_timings = true;
        }
        if (auto it = core_metrics.find(ttfb_key); it != core_metrics.end() && it->second >= 0) {
            timings.ttfb_ns = static_cast<uint64_t>(it->second) * 1'000'000ULL;
            timings.transfer_ns = attempt_ns > timings.ttfb_ns ? (attempt_ns - timings.ttfb_ns) : 0ULL;
            timings.has_timings = true;
        }

        const int status_code = extract_http_status_code(outcome);
        query_stats::record_http_attempt(
                std::string_view(serviceName.c_str(), serviceName.size()),
                std::string_view(requestName.c_str(), requestName.size()),
                classify_http_status_code(status_code),
                attempt_ns,
                new_conns,
                backoff_ns,
                is_throttled_http_status(status_code),
                timings
        );
    }
};

class QueryStatsAwsMonitorFactory final : public Aws::Monitoring::MonitoringFactory {
  public:
    Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> CreateMonitoringInstance() const override {
        return Aws::MakeUnique<QueryStatsAwsMonitor>(MonitoringAllocationTag);
    }
};

} // namespace

std::string_view current_aws_operation() noexcept {
    if (tl_active_aws_op != nullptr) {
        return tl_active_aws_op->request_name;
    }
    return {};
}

bool ContextPropagatingAwsExecutor::SubmitToThread(std::function<void()>&& fn) {
    if (!query_stats::is_active()) {
        return Aws::Utils::Threading::DefaultExecutor::SubmitToThread(std::move(fn));
    }
    auto ctx = query_stats::capture_context();
    if (!ctx) {
        return Aws::Utils::Threading::DefaultExecutor::SubmitToThread(std::move(fn));
    }
    return Aws::Utils::Threading::DefaultExecutor::SubmitToThread([ctx = std::move(ctx), fn = std::move(fn)]() mutable {
        query_stats::RestoredContextGuard guard(ctx);
        fn();
    });
}

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

std::shared_ptr<Aws::Http::HttpResponse> ArcticCurlHttpClient::MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter
) const {
    if (!query_stats::is_active()) {
        return Aws::Http::CurlHttpClient::MakeRequest(request, readLimiter, writeLimiter);
    }

    tl_curl_attempt.tracking = true;
    tl_curl_attempt.valid = false;
    tl_curl_attempt.new_connections = 0;
    tl_curl_attempt.start_tp = std::chrono::steady_clock::now();

    struct ResetTracking {
        ~ResetTracking() noexcept { tl_curl_attempt.tracking = false; }
    } reset_guard;

    auto response = Aws::Http::CurlHttpClient::MakeRequest(request, readLimiter, writeLimiter);
    tl_curl_attempt.duration_ns =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now() - tl_curl_attempt.start_tp
            )
                                          .count());
    tl_curl_attempt.valid = true;
    return response;
}

void ArcticCurlHttpClient::OverrideOptionsOnConnectionHandle(CURL* connection_handle) const {
    if (should_shuffle_dns_addresses()) {
        curl_easy_setopt(connection_handle, CURLOPT_DNS_SHUFFLE_ADDRESSES, 1L);
    }
    curl_easy_setopt(connection_handle, CURLOPT_SOCKOPTFUNCTION, &sockopt_count_new_connection);
    curl_easy_setopt(connection_handle, CURLOPT_SOCKOPTDATA, nullptr);
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
    const auto configured = ConfigsMap::instance()->get_int("AWS.EventLoopThreads", 1);
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

    // Left unset, Aws::InitAPI builds a ClientBootstrap with one AWS CRT event-loop thread per two logical
    // processors, which ArcticDB's S3Client has no use for - its HTTP transport is never CRT-backed.
    options_.ioOptions.clientBootstrap_create_fn = make_client_bootstrap_factory(event_loop_thread_count);

    // Register cross-platform SDK monitoring for query_stats HTTP telemetry (S3 vs STS, retries, latency, status).
    options_.monitoringOptions.customizedMonitoringFactory_create_fn.emplace_back([]() {
        return Aws::MakeUnique<QueryStatsAwsMonitorFactory>(MonitoringAllocationTag);
    });

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
