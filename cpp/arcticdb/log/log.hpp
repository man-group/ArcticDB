/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>
#include <memory>
#ifdef DEBUG_BUILD
#define ARCTICDB_DEBUG(logger, ...) logger.debug(__VA_ARGS__)
#define ARCTICDB_TRACE(logger, ...) logger.trace(__VA_ARGS__)
#else
#define ARCTICDB_DEBUG(logger, ...) (void)0
#define ARCTICDB_TRACE(logger, ...) (void)0
#endif

#define ARCTICDB_INFO(logger, ...) logger.info(__VA_ARGS__)
#define ARCTICDB_RUNTIME_DEBUG(logger, ...) logger.debug(__VA_ARGS__)

namespace arcticc::pb2::logger_pb2 {
class LoggersConfig;
}

namespace arcticdb::proto {
namespace logger = arcticc::pb2::logger_pb2;
}

namespace spdlog {
class logger;
namespace level {
enum level_enum : int;
}
namespace sinks {
class sink;
}
} // namespace spdlog

namespace arcticdb::log {

struct SpdlogWrapperImpl;

/**
 * Hide spdlog loggers from calling code for the sake of compilation time. spdlog.h transitively
 * includes a great deal, while at the same time logging is a near-universal concern in
 * ArcticDB TUs and of low performance concern.
 */
class SpdlogWrapper {
  public:
    template<typename... Args>
    void trace(fmt::format_string<Args...> fmt, Args&&... args) {
        debug(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void trace(const std::string& msg);

    template<typename... Args>
    void debug(fmt::format_string<Args...> fmt, Args&&... args) {
        debug(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void debug(const std::string& msg);

    template<typename... Args>
    void info(fmt::format_string<Args...> fmt, Args&&... args) {
        info(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void info(const std::string& msg);

    template<typename... Args>
    void warn(fmt::format_string<Args...> fmt, Args&&... args) {
        warn(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void warn(const std::string& msg);

    template<typename... Args>
    void error(fmt::format_string<Args...> fmt, Args&&... args) {
        info(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void error(const std::string& msg);

    template<typename... Args>
    void critical(fmt::format_string<Args...> fmt, Args&&... args) {
        critical(fmt::format(fmt, std::forward<Args>(args)...));
    }
    void critical(const std::string& msg);

    bool should_log(spdlog::level::level_enum level);
    void set_level(spdlog::level::level_enum level);
    spdlog::level::level_enum level();
    void flush();

    // If you need an API the wrapper doesn't expose and are willing to include spdlog.h, go for it.
    spdlog::logger& get() const noexcept;

  private:
    friend class Loggers;

    SpdlogWrapper(SpdlogWrapperImpl&);

    SpdlogWrapperImpl& impl_;
};

class Loggers {
  public:
    Loggers();
    ~Loggers();

    static void init();
    static Loggers& instance();
    static void destroy_instance();

    /**
     * Configure the loggers instance.
     * If called multiple times will ignore subsequent calls and return false
     * @param conf
     * @return true if configuration occurred
     */
    bool configure(const arcticdb::proto::logger::LoggersConfig& conf, bool force = false);

    SpdlogWrapper storage();
    SpdlogWrapper inmem();
    SpdlogWrapper codec();
    SpdlogWrapper root();
    SpdlogWrapper memory();
    SpdlogWrapper version();
    SpdlogWrapper timings();
    SpdlogWrapper lock();
    SpdlogWrapper schedule();
    SpdlogWrapper message();
    SpdlogWrapper symbol();
    SpdlogWrapper snapshot();
    SpdlogWrapper s3();

    void flush_all();

  private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

SpdlogWrapper storage();
SpdlogWrapper inmem();
SpdlogWrapper codec();
SpdlogWrapper root();
SpdlogWrapper version();
SpdlogWrapper memory();
SpdlogWrapper timings();
SpdlogWrapper lock();
SpdlogWrapper schedule();
SpdlogWrapper message();
SpdlogWrapper symbol();
SpdlogWrapper snapshot();
SpdlogWrapper s3();

/// Console sink for stdout/stderr: ConsoleSink (console_sink.hpp) on Windows, spdlog's own stdout/stderr sinks
/// everywhere else, where they already write through the FILE* and so already follow a dup2 of fd 1/2.
std::shared_ptr<spdlog::sinks::sink> make_console_sink(bool std_err, bool color);

} // namespace arcticdb::log
