/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once


#include <spdlog/spdlog.h>


#ifdef DEBUG_BUILD
#define ARCTICDB_DEBUG(logger, ...) logger.debug(__VA_ARGS__)
#define ARCTICDB_TRACE(logger, ...) logger.trace(__VA_ARGS__)
#else
#define ARCTICDB_DEBUG(logger, ...) (void)0
#define ARCTICDB_TRACE(logger, ...) (void)0
#endif

#define ARCTICDB_RUNTIME_DEBUG(logger, ...) logger.debug(__VA_ARGS__)

namespace arcticc::pb2::logger_pb2 {
    // to use these types where needed in user code: #include <logger.pb.h>
    class LoggerConfig;
    class LoggersConfig;
}

namespace arcticdb::proto {
    namespace logger = arcticc::pb2::logger_pb2;
}

namespace arcticdb::log {
class Loggers : public std::enable_shared_from_this<Loggers> {
  public:
    Loggers();
    ~Loggers();

    static std::shared_ptr<Loggers> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<Loggers> instance();
    static void destroy_instance();

    /**
     * Configure the loggers instance.
     * If called multiple times will ignore subsequent calls and return false
     * @param conf
     * @return true if configuration occurred
     */
    bool configure(const arcticdb::proto::logger::LoggersConfig &conf, bool force=false);

    spdlog::logger &storage();
    spdlog::logger &inmem();
    spdlog::logger &codec();
    spdlog::logger &root();
    spdlog::logger &memory();
    spdlog::logger &version();
    spdlog::logger &timings();
    spdlog::logger &lock();
    spdlog::logger &schedule();
    spdlog::logger &message();
    spdlog::logger &symbol();
    spdlog::logger &snapshot();

    void flush_all();

  private:

    void configure_logger(const arcticdb::proto::logger::LoggerConfig &conf,
                          const std::string &name,
                          std::unique_ptr<spdlog::logger> &logger);

    spdlog::logger &logger_ref(std::unique_ptr<spdlog::logger> &src);

    struct Impl;
    std::unique_ptr<Impl> impl_;

};

// N.B. If you add a new logger type here you need to add it to the dict of python loggers
// in log.py
spdlog::logger &storage();
spdlog::logger &inmem();
spdlog::logger &codec();
spdlog::logger &root();
spdlog::logger &version();
spdlog::logger &memory();
spdlog::logger &timings();
spdlog::logger &lock();
spdlog::logger &schedule();
spdlog::logger &message();
spdlog::logger &symbol();
spdlog::logger &snapshot();


} //namespace arcticdb::log
