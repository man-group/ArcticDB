/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#ifndef WIN32
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#endif
#include <atomic>
#include <memory>
#include <mutex>

namespace arcticdb::storage::s3 {

// Routes AWS SDK log messages into ArcticDB's `s3` spdlog logger so they are ordered with, and formatted like, the
// rest of ArcticDB's logging. Implementing LogSystemInterface directly (rather than deriving from FormattedLogSystem)
// keeps each message's AWS severity on every path, including the printf-style Log/vaLog path where vaLog receives the
// level. The severity is mapped to the equivalent spdlog level.
class SpdlogLogSystem : public Aws::Utils::Logging::LogSystemInterface {
  public:
    explicit SpdlogLogSystem(Aws::Utils::Logging::LogLevel log_level) : log_level_(log_level) {}

    Aws::Utils::Logging::LogLevel GetLogLevel() const override { return log_level_.load(); }
    void Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str, ...) override;
    void vaLog(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str, va_list args) override;
    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag, const Aws::OStringStream& message_stream)
            override;
    void Flush() override;

  private:
    std::atomic<Aws::Utils::Logging::LogLevel> log_level_;
};

#ifndef WIN32
// Whether to set CURLOPT_DNS_SHUFFLE_ADDRESSES.
bool dns_shuffle_addresses_enabled();

// Custom client that allows setting specific cUrl options.
class ArcticCurlHttpClient : public Aws::Http::CurlHttpClient {
  public:
    explicit ArcticCurlHttpClient(const Aws::Client::ClientConfiguration& client_configuration);

    bool should_shuffle_dns_addresses() const;

  protected:
    void OverrideOptionsOnConnectionHandle(CURL* connection_handle) const override;

  private:
    const bool dns_shuffle_addresses_enabled_;
};

class ArcticCurlHttpClientFactory : public Aws::Http::HttpClientFactory {
  public:
    // Defaults mirror Aws::HttpOptions
    explicit ArcticCurlHttpClientFactory(bool init_and_cleanup_curl = true, bool install_sigpipe_handler = false);

    std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& client_configuration
    ) const override;
    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
            const Aws::String& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory
    ) const override;
    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
            const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory
    ) const override;
    void InitStaticState() override;
    void CleanupStaticState() override;

  private:
    bool init_and_cleanup_curl_;
    bool install_sigpipe_handler_;
};
#endif // WIN32

class S3ApiInstance {
  public:
    S3ApiInstance(
            Aws::Utils::Logging::LogLevel log_level = Aws::Utils::Logging::LogLevel::Off, bool log_to_file = false
    );
    ~S3ApiInstance();

    static std::shared_ptr<S3ApiInstance> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<S3ApiInstance> instance();
    static void destroy_instance();

  private:
    Aws::Utils::Logging::LogLevel log_level_;
    Aws::SDKOptions options_;
};

} // namespace arcticdb::storage::s3