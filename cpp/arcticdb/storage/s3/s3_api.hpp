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