/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <spdlog/spdlog.h>
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

namespace arcticdb::log {
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

    spdlog::logger& storage();
    spdlog::logger& inmem();
    spdlog::logger& codec();
    spdlog::logger& root();
    spdlog::logger& memory();
    spdlog::logger& version();
    spdlog::logger& timings();
    spdlog::logger& lock();
    spdlog::logger& schedule();
    spdlog::logger& message();
    spdlog::logger& symbol();
    spdlog::logger& snapshot();

    void flush_all();

  private:
    struct Impl;

    std::unique_ptr<Impl> impl_;
};

spdlog::logger& storage();
spdlog::logger& inmem();
spdlog::logger& codec();
spdlog::logger& root();
spdlog::logger& version();
spdlog::logger& memory();
spdlog::logger& timings();
spdlog::logger& lock();
spdlog::logger& schedule();
spdlog::logger& message();
spdlog::logger& symbol();
spdlog::logger& snapshot();

} // namespace arcticdb::log
