/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preprocess.hpp>

#include <arcticdb/util/pb_util.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/details/periodic_worker.h>
#include <arcticdb/util/configs_map.hpp>
#include <filesystem>

namespace arcticdb::log {

static const char* DefaultLogPattern = "%Y%m%d %H:%M:%S.%f %t %L %n | %v";

constexpr auto get_default_log_level() {
    return spdlog::level::debug;
}

spdlog::logger &storage() {
    return Loggers::instance()->storage();
}

spdlog::logger &inmem() {
    return Loggers::instance()->inmem();
}

spdlog::logger &codec() {
    return Loggers::instance()->codec();
}

spdlog::logger &root() {
    return Loggers::instance()->root();
}

spdlog::logger &memory() {
    return Loggers::instance()->memory();
}

spdlog::logger &version() {
    return Loggers::instance()->version();
}

spdlog::logger &timings() {
    return Loggers::instance()->timings();
}

spdlog::logger &lock() {
    return Loggers::instance()->lock();
}

spdlog::logger &schedule() {
    return Loggers::instance()->schedule();
}

spdlog::logger &message() {
    return Loggers::instance()->message();
}

namespace fs = std::filesystem;

using SinkConf = arcticdb::proto::logger::SinkConfig;

Loggers::Loggers() : config_mutex_(), sink_by_id_(),
                     unconfigured_(std::make_unique<spdlog::logger>("arcticdb",
                                                                    std::make_shared<spdlog::sinks::stderr_sink_mt>())),
                     root_(), storage_(), inmem_(), codec_(), version_(), thread_pool_(), periodic_worker_() {
    unconfigured_->set_level(get_default_log_level());
}

spdlog::logger &Loggers::logger_ref(std::unique_ptr<spdlog::logger>  &src) {
    if (ARCTICDB_LIKELY(bool(src))) return *src;
    return *unconfigured_;
}

spdlog::logger &Loggers::storage() {
    return logger_ref(storage_);
}

spdlog::logger &Loggers::inmem() {
    return logger_ref(inmem_);
}

spdlog::logger &Loggers::codec() {
    return logger_ref(codec_);
}

spdlog::logger &Loggers::version() {
    return logger_ref(version_);
}

spdlog::logger &Loggers::memory() {
    return logger_ref(memory_);
}

spdlog::logger &Loggers::timings() {
    return logger_ref(timings_);
}

spdlog::logger &Loggers::lock() {
    return logger_ref(lock_);
}

spdlog::logger &Loggers::schedule() {
    return logger_ref(schedule_);
}

spdlog::logger &Loggers::message() {
    return logger_ref(message_);
}

spdlog::logger &Loggers::root() {
    return logger_ref(root_);
}

void Loggers::flush_all() {
    root().flush();
    storage().flush();
    inmem().flush();
    codec().flush();
    version().flush();
    memory().flush();
    timings().flush();
    lock().flush();
    schedule().flush();
    message().flush();
}


std::shared_ptr<Loggers> Loggers::instance() {
    std::call_once(Loggers::init_flag_, &Loggers::init);
    return instance_;
}

void Loggers::destroy_instance() {
    Loggers::instance_.reset();
}

void Loggers::init() {
    Loggers::instance_ = std::make_shared<Loggers>();
}


std::shared_ptr<Loggers> Loggers::instance_;
std::once_flag Loggers::init_flag_;

namespace {
std::string make_parent_dir(const std::string &p_str, const std::string_view &def_p_str) {
    fs::path p;
    if (p_str.empty()) {
        p = fs::path(def_p_str);
    } else {
        p = fs::path(p_str);
    }
    if (!fs::exists(p.parent_path())) {
        fs::create_directories(p.parent_path());
    }
    return p.generic_string();
}
}
bool Loggers::configure(const arcticdb::proto::logger::LoggersConfig &conf, bool force) {
    auto lock = std::scoped_lock(config_mutex_);
    if (!force && root_)
        return false;

    // Configure async behavior
    if (conf.has_async()) {
        thread_pool_ = std::make_shared<spdlog::details::thread_pool>(
            util::as_opt(conf.async().queue_size()).value_or(8192),
            util::as_opt(conf.async().thread_pool_size()).value_or(1)
        );
    }

    // Configure the sinks
    for (auto &&[sink_id, sink_conf] : conf.sink_by_id()) {
        switch (sink_conf.sink_case()) {
            case SinkConf::kConsole:
                if (sink_conf.console().has_color()) {
                    if (sink_conf.console().std_err()) {
                        sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
                    } else {
                        sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
                    }
                } else {
                    if (sink_conf.console().std_err()) {
                        sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::stderr_sink_mt>());
                    } else {
                        sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::stdout_sink_mt>());
                    }
                }
                break;
            case SinkConf::kFile:
                sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                    make_parent_dir(sink_conf.file().path(), "./arcticdb.basic.log")
                ));
                break;
            case SinkConf::kRotFile:
                sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                    make_parent_dir(sink_conf.rot_file().path(), "./arcticdb.rot.log"),
                    util::as_opt(sink_conf.rot_file().max_size_bytes()).value_or(64ULL* (1ULL<< 20)),
                    util::as_opt(sink_conf.rot_file().max_file_count()).value_or(8)
                ));
                break;
            case SinkConf::kDailyFile:
                sink_by_id_.emplace(sink_id, std::make_shared<spdlog::sinks::daily_file_sink_mt>(
                    make_parent_dir(sink_conf.daily_file().path(), "./arcticdb.daily.log"),
                    util::as_opt(sink_conf.daily_file().utc_rotation_hour()).value_or(0),
                    util::as_opt(sink_conf.daily_file().utc_rotation_minute()).value_or(0)
                ));
                break;
            default:util::raise_error_msg("Unsupported sink_conf {}", sink_conf);
        }
    }

    // Now associate loggers with sinks
    auto check_and_configure = [&](
        const std::string &name,
        const std::string &fallback,
        std::unique_ptr<spdlog::logger> &logger) {
        if (auto it = conf.logger_by_id().find(name); it != conf.logger_by_id().end()) {
            configure_logger(it->second, name, logger);
        } else {
            if (fallback.empty())
                throw std::invalid_argument(fmt::format(
                    "missing conf for logger {} without fallback", name));
            configure_logger(conf.logger_by_id().find(fallback)->second, name, logger);
        }
    };

    check_and_configure("root", std::string(), root_);
    check_and_configure("storage", "root", storage_);
    check_and_configure("inmem", "root", inmem_);
    check_and_configure("codec", "root", codec_);
    check_and_configure("version", "root", version_);
    check_and_configure("memory", "root", memory_);
    check_and_configure("timings", "root", timings_);
    check_and_configure("lock", "root", lock_);
    check_and_configure("schedule", "root", schedule_);
    check_and_configure("message", "root", message_);

    auto flush_sec = util::as_opt(conf.flush_interval_seconds()).value_or(1);
    if (flush_sec != 0) {
        periodic_worker_ = std::make_unique<typename decltype(periodic_worker_)::element_type>(
            [loggers = weak_from_this()]() {
                if (auto l = loggers.lock()) {
                    l->flush_all();
                }
            }, std::chrono::seconds(flush_sec));
    }
    return true;
}

void Loggers::configure_logger(
        const arcticdb::proto::logger::LoggerConfig &conf,
        const std::string &name,
        std::unique_ptr<spdlog::logger> &logger) {
    std::vector<spdlog::sink_ptr> sink_ptrs;
    for (const auto& sink_id : conf.sink_ids()) {
        if (auto it = sink_by_id_.find(sink_id); it != sink_by_id_.end()) {
            sink_ptrs.push_back(it->second);
        } else {
            throw std::invalid_argument(fmt::format("invalid sink_id {} for logger {}", sink_id, name));
        }
    }
    auto fq_name = fmt::format("arcticdb.{}", name);
    if (thread_pool_) {
        // async logger
        logger = std::make_unique<spdlog::async_logger>(fq_name, sink_ptrs.begin(), sink_ptrs.end(),
                                                        thread_pool_, spdlog::async_overflow_policy::block);
    } else {
        logger = std::make_unique<spdlog::logger>(fq_name, sink_ptrs.begin(), sink_ptrs.end());
    }

    if (!conf.pattern().empty()) {
        logger->set_pattern(conf.pattern());
    }
    else {
        logger->set_pattern(DefaultLogPattern);
    }
    
    if (conf.level() != 0) {
        logger->set_level(static_cast<spdlog::level::level_enum>(conf.level() - 1));
    } else {
        logger->set_level(get_default_log_level());
    }
}

}
