/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/log/log.hpp>
#ifdef _WIN32
#include <arcticdb/log/console_sink.hpp>
#endif
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/details/periodic_worker.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/async_logger.h>
#include <spdlog/async.h>
#include <logger.pb.h>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>

namespace arcticdb::log {

static const char* DefaultLogPattern = "%Y%m%d %H:%M:%S.%f %t %L %n | %v";

namespace {
std::shared_ptr<Loggers> loggers_instance_;
std::once_flag loggers_init_flag_;
} // namespace

// The cached-HANDLE bug this works around is Windows-only, so only Windows gets the replacement sink; elsewhere
// spdlog's own sinks are kept, unchanged. They already fwrite() to the FILE*, so they already follow a dup2 of
// fd 1/2 - there was nothing to fix off Windows.
std::shared_ptr<spdlog::sinks::sink> make_console_sink(bool std_err, bool color) {
#ifdef _WIN32
    // Not the colour sink either: spdlog's Windows colour sink caches the same HANDLE
    (void)color;
    return std::make_shared<ConsoleSinkMt>(std_err ? stderr : stdout);
#else
    if (color) {
        if (std_err)
            return std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
        return std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    }
    if (std_err)
        return std::make_shared<spdlog::sinks::stderr_sink_mt>();
    return std::make_shared<spdlog::sinks::stdout_sink_mt>();
#endif
}

struct SpdlogWrapperImpl {
    std::unique_ptr<spdlog::logger> logger_;
};

SpdlogWrapper::SpdlogWrapper(SpdlogWrapperImpl& impl) : impl_(impl) {}
void SpdlogWrapper::trace(const std::string& msg) { impl_.logger_->trace(msg); }
void SpdlogWrapper::debug(const std::string& msg) { impl_.logger_->debug(msg); }
void SpdlogWrapper::info(const std::string& msg) { impl_.logger_->info(msg); }
void SpdlogWrapper::warn(const std::string& msg) { impl_.logger_->warn(msg); }
void SpdlogWrapper::error(const std::string& msg) { impl_.logger_->error(msg); }
void SpdlogWrapper::critical(const std::string& msg) { impl_.logger_->critical(msg); }
bool SpdlogWrapper::should_log(spdlog::level::level_enum level) { return impl_.logger_->should_log(level); }
void SpdlogWrapper::set_level(spdlog::level::level_enum level) { impl_.logger_->set_level(level); };
spdlog::level::level_enum SpdlogWrapper::level() { return impl_.logger_->level(); }
void SpdlogWrapper::flush() { return impl_.logger_->flush(); }
spdlog::logger& SpdlogWrapper::get() const noexcept { return *impl_.logger_; }

struct Loggers::Impl {
    std::mutex config_mutex_;
    std::unordered_map<std::string, spdlog::sink_ptr> sink_by_id_;
    SpdlogWrapperImpl unconfigured_ = SpdlogWrapperImpl(
                                                        std::make_unique<spdlog::logger>("arcticdb", make_console_sink(true, false))
    );
    SpdlogWrapperImpl root_;
    SpdlogWrapperImpl storage_;
    SpdlogWrapperImpl inmem_;
    SpdlogWrapperImpl memory_;
    SpdlogWrapperImpl codec_;
    SpdlogWrapperImpl version_;
    SpdlogWrapperImpl timings_;
    SpdlogWrapperImpl lock_;
    SpdlogWrapperImpl schedule_;
    SpdlogWrapperImpl message_;
    SpdlogWrapperImpl symbol_;
    SpdlogWrapperImpl snapshot_;
    SpdlogWrapperImpl s3_;

    std::shared_ptr<spdlog::details::thread_pool> thread_pool_;
    std::optional<spdlog::details::periodic_worker> periodic_worker_;

    void configure_logger(
            const arcticdb::proto::logger::LoggerConfig& conf, const std::string& name, SpdlogWrapperImpl& logger
    );

    SpdlogWrapperImpl& logger_ref(SpdlogWrapperImpl& src);
};

constexpr auto get_default_log_level() { return spdlog::level::info; }

SpdlogWrapper storage() { return Loggers::instance().storage(); }

SpdlogWrapper inmem() { return Loggers::instance().inmem(); }

SpdlogWrapper codec() { return Loggers::instance().codec(); }

SpdlogWrapper root() { return Loggers::instance().root(); }

SpdlogWrapper memory() { return Loggers::instance().memory(); }

SpdlogWrapper version() { return Loggers::instance().version(); }

SpdlogWrapper timings() { return Loggers::instance().timings(); }

SpdlogWrapper lock() { return Loggers::instance().lock(); }

SpdlogWrapper schedule() { return Loggers::instance().schedule(); }

SpdlogWrapper message() { return Loggers::instance().message(); }

SpdlogWrapper symbol() { return Loggers::instance().symbol(); }

SpdlogWrapper snapshot() { return Loggers::instance().snapshot(); }

SpdlogWrapper s3() { return Loggers::instance().s3(); }

namespace fs = std::filesystem;

using SinkConf = arcticdb::proto::logger::SinkConfig;

Loggers::Loggers() : impl_(std::make_unique<Impl>()) {
    impl_->unconfigured_.logger_->set_level(get_default_log_level());
    impl_->unconfigured_.logger_->set_pattern(DefaultLogPattern);
}

Loggers::~Loggers() = default;

Loggers& Loggers::instance() {
    std::call_once(loggers_init_flag_, &Loggers::init);
    return *loggers_instance_;
}

SpdlogWrapper Loggers::storage() { return impl_->logger_ref(impl_->storage_); }

SpdlogWrapper Loggers::inmem() { return impl_->logger_ref(impl_->inmem_); }

SpdlogWrapper Loggers::codec() { return impl_->logger_ref(impl_->codec_); }

SpdlogWrapper Loggers::version() { return impl_->logger_ref(impl_->version_); }

SpdlogWrapper Loggers::memory() { return impl_->logger_ref(impl_->memory_); }

SpdlogWrapper Loggers::timings() { return impl_->logger_ref(impl_->timings_); }

SpdlogWrapper Loggers::lock() { return impl_->logger_ref(impl_->lock_); }

SpdlogWrapper Loggers::schedule() { return impl_->logger_ref(impl_->schedule_); }

SpdlogWrapper Loggers::message() { return impl_->logger_ref(impl_->message_); }

SpdlogWrapper Loggers::symbol() { return impl_->logger_ref(impl_->symbol_); }

SpdlogWrapper Loggers::snapshot() { return impl_->logger_ref(impl_->snapshot_); }

SpdlogWrapper Loggers::s3() { return impl_->logger_ref(impl_->s3_); }

SpdlogWrapper Loggers::root() { return impl_->logger_ref(impl_->root_); }

void Loggers::flush_all() {
    root().impl_.logger_->flush();
    storage().impl_.logger_->flush();
    inmem().impl_.logger_->flush();
    codec().impl_.logger_->flush();
    version().impl_.logger_->flush();
    memory().impl_.logger_->flush();
    timings().impl_.logger_->flush();
    lock().impl_.logger_->flush();
    schedule().impl_.logger_->flush();
    message().impl_.logger_->flush();
    symbol().impl_.logger_->flush();
    snapshot().impl_.logger_->flush();
    s3().impl_.logger_->flush();
}

void Loggers::destroy_instance() { loggers_instance_.reset(); }

void Loggers::init() { loggers_instance_ = std::make_shared<Loggers>(); }

namespace {
std::string make_parent_dir(const std::string& p_str, std::string_view def_p_str) {
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
} // namespace

SpdlogWrapperImpl& Loggers::Impl::logger_ref(SpdlogWrapperImpl& src) {
    if (ARCTICDB_LIKELY(bool(src.logger_)))
        return src;

    return unconfigured_;
}

bool Loggers::configure(const arcticdb::proto::logger::LoggersConfig& conf, bool force) {
    auto lock = std::scoped_lock(impl_->config_mutex_);
    if (!force && impl_->root_.logger_)
        return false;

    // Configure async behavior
    if (conf.has_async()) {
        impl_->thread_pool_ = std::make_shared<spdlog::details::thread_pool>(
                util::as_opt(conf.async().queue_size()).value_or(8192),
                util::as_opt(conf.async().thread_pool_size()).value_or(1)
        );
    }

    // Configure the sinks
    for (auto&& [sink_id, sink_conf] : conf.sink_by_id()) {
        switch (sink_conf.sink_case()) {
        case SinkConf::kConsole:
            impl_->sink_by_id_.try_emplace(
                    sink_id, make_console_sink(sink_conf.console().std_err(), sink_conf.console().has_color())
            );
            break;
        case SinkConf::kFile:
            impl_->sink_by_id_.try_emplace(
                    sink_id,
                    std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                            make_parent_dir(sink_conf.file().path(), "./arcticdb.basic.log")
                    )
            );
            break;
        case SinkConf::kRotFile:
            impl_->sink_by_id_.try_emplace(
                    sink_id,
                    std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                            make_parent_dir(sink_conf.rot_file().path(), "./arcticdb.rot.log"),
                            util::as_opt(sink_conf.rot_file().max_size_bytes()).value_or(64ULL * (1ULL << 20)),
                            util::as_opt(sink_conf.rot_file().max_file_count()).value_or(8)
                    )
            );
            break;
        case SinkConf::kDailyFile:
            impl_->sink_by_id_.try_emplace(
                    sink_id,
                    std::make_shared<spdlog::sinks::daily_file_sink_mt>(
                            make_parent_dir(sink_conf.daily_file().path(), "./arcticdb.daily.log"),
                            util::as_opt(sink_conf.daily_file().utc_rotation_hour()).value_or(0),
                            util::as_opt(sink_conf.daily_file().utc_rotation_minute()).value_or(0)
                    )
            );
            break;
        default:
            util::raise_rte("Unsupported sink_conf {}", sink_conf.DebugString());
        }
    }

    // Now associate loggers with sinks
    auto check_and_configure = [&](const std::string& name, const std::string& fallback, auto& logger) {
        const auto& logger_by_id = conf.logger_by_id();
        if (auto it = logger_by_id.find(name); it != logger_by_id.end()) {
            impl_->configure_logger(it->second, name, logger);
        } else {
            if (fallback.empty())
                throw std::invalid_argument(fmt::format("missing conf for logger {} without fallback", name));
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
    check_and_configure("s3", "root", impl_->s3_);

    if (auto flush_sec = util::as_opt(conf.flush_interval_seconds()).value_or(1); flush_sec != 0) {
        impl_->periodic_worker_.emplace(
                [loggers = std::weak_ptr(loggers_instance_)]() {
                    if (auto l = loggers.lock()) {
                        l->flush_all();
                    }
                },
                std::chrono::seconds(flush_sec)
        );
    }
    return true;
}

void Loggers::Impl::configure_logger(
        const arcticdb::proto::logger::LoggerConfig& conf, const std::string& name, SpdlogWrapperImpl& logger
) {
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
        logger.logger_ = make_unique<spdlog::async_logger>(
                fq_name, sink_ptrs.begin(), sink_ptrs.end(), thread_pool_, spdlog::async_overflow_policy::block
        );
    } else {
        logger.logger_ = std::make_unique<spdlog::logger>(fq_name, sink_ptrs.begin(), sink_ptrs.end());
    }

    if (!conf.pattern().empty())
        logger.logger_->set_pattern(conf.pattern());
    else
        logger.logger_->set_pattern(DefaultLogPattern);

    if (conf.level() != 0)
        logger.logger_->set_level(static_cast<spdlog::level::level_enum>(conf.level() - 1));
    else
        logger.logger_->set_level(get_default_log_level());
}

} // namespace arcticdb::log
