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
#include <filesystem>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/async.h>
#include <logger.pb.h>

#include <memory>
#include <mutex>
#include <optional>

namespace arcticdb::log {

static const char* DefaultLogPattern = "%Y%m%d %H:%M:%S.%f %t %L %n | %v";

namespace {
std::shared_ptr<Loggers> loggers_instance_;
std::once_flag loggers_init_flag_;
} // namespace

struct Loggers::Impl {
    std::mutex config_mutex_;
    std::unordered_map<std::string, spdlog::sink_ptr> sink_by_id_;
    std::unique_ptr<spdlog::logger> unconfigured_ = std::make_unique<spdlog::logger>("arcticdb",
        std::make_shared<spdlog::sinks::stderr_sink_mt>());
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
    std::unique_ptr<spdlog::logger> symbol_;
    std::unique_ptr<spdlog::logger> snapshot_;
    std::shared_ptr<spdlog::details::thread_pool> thread_pool_;
    std::optional<spdlog::details::periodic_worker> periodic_worker_;


    void configure_logger(const arcticdb::proto::logger::LoggerConfig& conf,
        const std::string& name,
        std::unique_ptr<spdlog::logger>& logger);

    spdlog::logger& logger_ref(std::unique_ptr<spdlog::logger>& src);
};

constexpr auto get_default_log_level() {
    return spdlog::level::info;
}

spdlog::logger &storage() {
    return Loggers::instance().storage();
}

spdlog::logger &inmem() {
    return Loggers::instance().inmem();
}

spdlog::logger &codec() {
    return Loggers::instance().codec();
}

spdlog::logger &root() {
    return Loggers::instance().root();
}

spdlog::logger &memory() {
    return Loggers::instance().memory();
}

spdlog::logger &version() {
    return Loggers::instance().version();
}

spdlog::logger &timings() {
    return Loggers::instance().timings();
}

spdlog::logger &lock() {
    return Loggers::instance().lock();
}

spdlog::logger &schedule() {
    return Loggers::instance().schedule();
}

spdlog::logger &message() {
    return Loggers::instance().message();
}

spdlog::logger &symbol() {
    return Loggers::instance().symbol();
}

spdlog::logger &snapshot() {
    return Loggers::instance().snapshot();
}

namespace fs = std::filesystem;

using SinkConf = arcticdb::proto::logger::SinkConfig;

Loggers::Loggers()
        : impl_(std::make_unique<Impl>()) {
    impl_->unconfigured_->set_level(get_default_log_level());
    impl_->unconfigured_->set_pattern(DefaultLogPattern);
}

Loggers::~Loggers() = default;

Loggers& Loggers::instance() {
    std::call_once(loggers_init_flag_, &Loggers::init);
    return *loggers_instance_;
}

spdlog::logger &Loggers::storage() {
    return impl_->logger_ref(impl_->storage_);
}

spdlog::logger &Loggers::inmem() {
    return impl_->logger_ref(impl_->inmem_);
}

spdlog::logger &Loggers::codec() {
    return impl_->logger_ref(impl_->codec_);
}

spdlog::logger &Loggers::version() {
    return impl_->logger_ref(impl_->version_);
}

spdlog::logger &Loggers::memory() {
    return impl_->logger_ref(impl_->memory_);
}

spdlog::logger &Loggers::timings() {
    return impl_->logger_ref(impl_->timings_);
}

spdlog::logger &Loggers::lock() {
    return impl_->logger_ref(impl_->lock_);
}

spdlog::logger &Loggers::schedule() {
    return impl_->logger_ref(impl_->schedule_);
}

spdlog::logger &Loggers::message() {
    return impl_->logger_ref(impl_->message_);
}

spdlog::logger &Loggers::symbol() {
    return impl_->logger_ref(impl_->symbol_);
}

spdlog::logger &Loggers::snapshot() {
    return impl_->logger_ref(impl_->snapshot_);
}

spdlog::logger &Loggers::root() {
    return impl_->logger_ref(impl_->root_);
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
    symbol().flush();
    snapshot().flush();
}

void Loggers::destroy_instance() {
    loggers_instance_.reset();
}

void Loggers::init() {
    loggers_instance_ = std::make_shared<Loggers>();
}

namespace {
std::string make_parent_dir(const std::string &p_str, std::string_view def_p_str) {
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

spdlog::logger& Loggers::Impl::logger_ref(std::unique_ptr<spdlog::logger>& src) {
    if (ARCTICDB_LIKELY(bool(src)))
        return *src;

    return *unconfigured_;
}

bool Loggers::configure(const arcticdb::proto::logger::LoggersConfig &conf, bool force) {
    auto lock = std::scoped_lock(impl_->config_mutex_);
    if (!force && impl_->root_)
        return false;

    // Configure async behavior
    if (conf.has_async()) {
        impl_->thread_pool_ = std::make_shared<spdlog::details::thread_pool>(
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
                        impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
                    } else {
                        impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
                    }
                } else {
                    if (sink_conf.console().std_err()) {
                        impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::stderr_sink_mt>());
                    } else {
                        impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::stdout_sink_mt>());
                    }
                }
                break;
            case SinkConf::kFile:
                impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                    make_parent_dir(sink_conf.file().path(), "./arcticdb.basic.log")
                ));
                break;
            case SinkConf::kRotFile:
                impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                    make_parent_dir(sink_conf.rot_file().path(), "./arcticdb.rot.log"),
                    util::as_opt(sink_conf.rot_file().max_size_bytes()).value_or(64ULL* (1ULL<< 20)),
                    util::as_opt(sink_conf.rot_file().max_file_count()).value_or(8)
                ));
                break;
            case SinkConf::kDailyFile:
                impl_->sink_by_id_.try_emplace(sink_id, std::make_shared<spdlog::sinks::daily_file_sink_mt>(
                    make_parent_dir(sink_conf.daily_file().path(), "./arcticdb.daily.log"),
                    util::as_opt(sink_conf.daily_file().utc_rotation_hour()).value_or(0),
                    util::as_opt(sink_conf.daily_file().utc_rotation_minute()).value_or(0)
                ));
                break;
            default:util::raise_rte("Unsupported sink_conf {}", sink_conf.DebugString());
        }
    }

    // Now associate loggers with sinks
    auto check_and_configure = [&](
        const std::string &name,
        const std::string &fallback,
        auto &logger) {
        const auto& logger_by_id = conf.logger_by_id();
        if (auto it = logger_by_id.find(name); it != logger_by_id.end()) {
            impl_->configure_logger(it->second, name, logger);
        } else {
            if (fallback.empty())
                throw std::invalid_argument(fmt::format(
                    "missing conf for logger {} without fallback", name));
            impl_->configure_logger(logger_by_id.find(fallback)->second, name, logger);
        }
    };

    check_and_configure("root", std::string(), impl_->root_);
    check_and_configure("storage", "root", impl_->storage_);
    check_and_configure("inmem", "root", impl_->inmem_);
    check_and_configure("codec", "root", impl_->codec_);
    check_and_configure("version", "root", impl_->version_);
    check_and_configure("memory", "root", impl_->memory_);
    check_and_configure("timings", "root", impl_->timings_);
    check_and_configure("lock", "root", impl_->lock_);
    check_and_configure("schedule", "root", impl_->schedule_);
    check_and_configure("message", "root", impl_->message_);
    check_and_configure("symbol", "root", impl_->symbol_);
    check_and_configure("snapshot", "root", impl_->snapshot_);

    if (auto flush_sec = util::as_opt(conf.flush_interval_seconds()).value_or(1); flush_sec != 0) {
        impl_->periodic_worker_.emplace(
            [loggers = std::weak_ptr(loggers_instance_)]() {
                if (auto l = loggers.lock()) {
                    l->flush_all();
                }
            }, std::chrono::seconds(flush_sec));
    }
    return true;
}

void Loggers::Impl::configure_logger(
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

    if (!conf.pattern().empty())
        logger->set_pattern(conf.pattern());
    else
        logger->set_pattern(DefaultLogPattern);

    if (conf.level() != 0)
        logger->set_level(static_cast<spdlog::level::level_enum>(conf.level() - 1));
    else
        logger->set_level(get_default_log_level());
}

}
