/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>

#include <memory>
#include <mutex>

#ifdef DEBUG_BUILD
#define ARCTICDB_DEBUG(logger, ...) logger.debug(__VA_ARGS__)
#define ARCTICDB_TRACE(logger, ...) logger.trace(__VA_ARGS__)
#else
#define ARCTICDB_DEBUG(logger, ...) (void)0
#define ARCTICDB_TRACE(logger, ...) (void)0
#endif

#define ARCTICDB_RUNTIME_INFO(logger, ...) logger.info(__VA_ARGS__)
#define ARCTICDB_RUNTIME_DEBUG(logger, ...) logger.debug(__VA_ARGS__)
#define ARCTICDB_RUNTIME_TRACE(logger, ...) logger.trace(__VA_ARGS__)

namespace arcticdb::log {
class Loggers : public std::enable_shared_from_this<Loggers> {
public:
    Loggers();

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

    void flush_all();

private:
    void configure_logger(const arcticdb::proto::logger::LoggerConfig& conf,
        const std::string& name,
        std::unique_ptr<spdlog::logger>& logger);

    spdlog::logger& logger_ref(std::unique_ptr<spdlog::logger>& src);

    std::mutex config_mutex_;
    std::unordered_map<std::string, spdlog::sink_ptr> sink_by_id_;
    std::unique_ptr<spdlog::logger> unconfigured_;
    std::unique_ptr<spdlog::logger> root_;
    std::unique_ptr<spdlog::logger> storage_;
    std::unique_ptr<spdlog::logger> inmem_;
    std::unique_ptr<spdlog::logger> memory_;
    std::unique_ptr<spdlog::logger> codec_;
    std::unique_ptr<spdlog::logger> version_;
    std::unique_ptr<spdlog::logger> timings_;
    std::unique_ptr<spdlog::logger> lock_;
    std::unique_ptr<spdlog::logger> schedule_;
    std::unique_ptr<spdlog::logger> message_;
    std::shared_ptr<spdlog::details::thread_pool> thread_pool_;
    std::unique_ptr<spdlog::details::periodic_worker> periodic_worker_;
};

// N.B. If you add a new logger type here you need to add it to the dict of python loggers
// in log.py
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

inline std::unordered_map<std::string, spdlog::logger*> get_loggers_by_name()
{
    return {{"root", &root()},
        {"storage", &storage()},
        {"inmem", &inmem()},
        {"codec", &codec()},
        {"version", &version()},
        {"memory", &memory()},
        {"timings", &timings()},
        {"lock", &lock()},
        {"schedule", &schedule()},
        {"message", &message()}};
}

inline void set_levels(spdlog::level::level_enum default_level,
    std::unordered_map<std::string, spdlog::level::level_enum> specific_log_levels)
{
    auto log_cfgs = arcticdb::proto::logger::LoggersConfig{};

    auto sink_config = arcticdb::proto::logger::SinkConfig{};
    sink_config.mutable_console()->set_std_err(true);
    log_cfgs.mutable_sink_by_id()->insert({std::string{"console"}, sink_config});

    auto logger_by_name = get_loggers_by_name();
    for (const auto& entry : logger_by_name) {
        const auto& logger_name = entry.first;
        auto it = specific_log_levels.find(logger_name);
        auto level_to_set = (it == specific_log_levels.end() ? default_level : it->second);
        auto logger = arcticdb::proto::logger::LoggerConfig{};
        sink_config.mutable_console()->set_std_err(true);
        logger.set_level(arcticdb::proto::logger::LoggerConfig_Level(int(level_to_set)));
        auto logger_id =
            fmt::format("{}", logger_name != "root" ? fmt::format("arcticdb.{}", logger_name) : "arcticdb");
        logger.set_pattern(fmt::format("%Y%m%d_%H%M%S.%f %t %L %n %P | %v", logger_id));
        log_cfgs.mutable_logger_by_id()->insert({logger_name, logger});
    }

    Loggers::instance()->configure(log_cfgs, true);
}

} //namespace arcticdb::log