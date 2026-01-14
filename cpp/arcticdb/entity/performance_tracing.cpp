/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#if defined(USE_REMOTERY)

#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/util/storage_lock.hpp>

std::shared_ptr<RemoteryInstance> RemoteryInstance::instance() {
    std::call_once(RemoteryInstance::init_flag_, &RemoteryInstance::init);
    return RemoteryInstance::instance_;
}

std::shared_ptr<RemoteryInstance> RemoteryInstance::instance_;
std::once_flag RemoteryInstance::init_flag_;

std::shared_ptr<RemoteryConfigInstance> RemoteryConfigInstance::instance() {
    std::call_once(RemoteryConfigInstance::init_flag_, &RemoteryConfigInstance::init);
    return RemoteryConfigInstance::instance_;
}

std::shared_ptr<RemoteryConfigInstance> RemoteryConfigInstance::instance_;
std::once_flag RemoteryConfigInstance::init_flag_;

RemoteryInstance::RemoteryInstance() {
    auto cfg = RemoteryConfigInstance::instance()->config;
    auto* settings = rmt_Settings();
    settings->port = cfg.port() ? cfg.port() : 17815;
    settings->reuse_open_port = !cfg.do_not_reuse_open_port();
    settings->limit_connections_to_localhost = cfg.limit_connections_to_localhost();
    auto set_if = [](auto* dst, auto src) {
        if (src) {
            *dst = src;
        }
    };
    set_if(&settings->msSleepBetweenServerUpdates, cfg.ms_sleep_btw_server_updates());
    set_if(&settings->messageQueueSizeInBytes, cfg.message_queue_size_in_bytes());
    set_if(&settings->maxNbMessagesPerUpdate, cfg.max_nb_messages_per_update());
    auto rc = rmt_CreateGlobalInstance(&rmt_);
    if (rc) {
        ARCTICDB_DEBUG(
                arcticdb::log::version(),
                "Remotery creation with settings '{}' failed, rc={}",
                arcticdb::util::format(cfg),
                rc
        );
        rmt_ = nullptr;
    } else {
        ARCTICDB_DEBUG(arcticdb::log::version(), "Remotery created with settings {}", arcticdb::util::format(cfg));
    }
}

RemoteryInstance::~RemoteryInstance() {
    if (rmt_) {
        rmt_DestroyGlobalInstance(rmt_);
        rmt_ = nullptr;
    }
}

namespace arcticdb::detail {
struct ThreadNameCache {
    std::unordered_map<const char*, std::string> fqn_by_task_name_;
    std::string thread_name_;

    ThreadNameCache() : fqn_by_task_name_(), thread_name_(fmt::format("{}", arcticdb::get_thread_id())) {}

    std::string_view get_thread_name(const char* task_name) {
        if (auto fqn_it = fqn_by_task_name_.find(task_name); fqn_it != fqn_by_task_name_.end()) {
            return fqn_it->second;
        }
        auto [fqn_it, inserted] = fqn_by_task_name_.emplace(task_name, fmt::format("{}/{}", thread_name_, task_name));
        return fqn_it->second;
    }
};
} // namespace arcticdb::detail

void set_remotery_thread_name(const char* task_name) {
    static thread_local arcticdb::detail::ThreadNameCache tnc;
    auto name = tnc.get_thread_name(task_name);
    rmt_SetCurrentThreadName(name.data());
}

#endif
