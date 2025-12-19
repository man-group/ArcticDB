/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/timer.hpp>

#include <memory>

// #define ARCTICDB_LOG_PERFORMANCE

#define ARCTICDB_RUNTIME_SAMPLE(name, flags)                                                                           \
    static bool _scoped_timer_active_ = ConfigsMap::instance()->get_int("Logging.timings", 0) == 1 ||                  \
                                        ConfigsMap::instance()->get_int("Logging.ALL", 0) == 1;                        \
    arcticdb::ScopedTimer runtime_timer =                                                                              \
            !_scoped_timer_active_ ? arcticdb::ScopedTimer()                                                           \
                                   : arcticdb::ScopedTimer(#name, [](auto msg) { log::timings().debug(msg); });

#define ARCTICDB_RUNTIME_SUBSAMPLE(name, flags)                                                                        \
    static bool _scoped_subtimer_##name_active_ = ConfigsMap::instance()->get_int("Logging.timer", 0) == 1;            \
    arcticdb::ScopedTimer runtime_sub_timer_##name =                                                                   \
            !_scoped_subtimer_##name_active_                                                                           \
                    ? arcticdb::ScopedTimer()                                                                          \
                    : arcticdb::ScopedTimer(#name, [](auto msg) { log::timings().debug(msg); });

#ifdef USE_REMOTERY

#include <Remotery.h>

class RemoteryInstance {
  public:
    static std::shared_ptr<RemoteryInstance> instance();
    RemoteryInstance();
    ~RemoteryInstance();
    static std::shared_ptr<RemoteryInstance> instance_;
    static std::once_flag init_flag_;
    static void destroy_instance() { instance_.reset(); }
    Remotery* rmt_ = nullptr;

    static void init() { instance_ = std::make_shared<RemoteryInstance>(); }
};

class RemoteryConfigInstance {
  public:
    static std::shared_ptr<RemoteryConfigInstance> instance();
    arcticdb::proto::utils::RemoteryConfig config;
    static std::shared_ptr<RemoteryConfigInstance> instance_;
    static std::once_flag init_flag_;

    static void init() { instance_ = std::make_shared<RemoteryConfigInstance>(); }
};

#define ARCTICDB_SAMPLE(name, flags)                                                                                   \
    auto instance = RemoteryInstance::instance();                                                                      \
    rmt_ScopedCPUSample(name, flags);

#define ARCTICDB_SUBSAMPLE(name, flags) rmt_ScopedCPUSample(name, flags);

#define ARCTICDB_SAMPLE_DEFAULT(name) ARCTICDB_SAMPLE(name, 0)

#define ARCTICDB_SUBSAMPLE_DEFAULT(name) ARCTICDB_SUBSAMPLE(name, 0)

#define ARCTICDB_SUBSAMPLE_AGG(name) rmt_ScopedCPUSample(name, RMTSF_Aggregate);

void set_remotery_thread_name(const char* task_name);

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name) rmt_LogText(task_name);

#elif defined(ARCTICDB_LOG_PERFORMANCE)
#define ARCTICDB_SAMPLE(name, flags) arcticdb::ScopedTimer _timer{#name, [](auto msg) { std::cout << msg; }};

#define ARCTICDB_SUBSAMPLE(name, flags)                                                                                \
    arcticdb::ScopedTimer _sub_timer_##name{#name, [](auto msg) { std::cout << msg; }};

#define ARCTICDB_SAMPLE_DEFAULT(name) arcticdb::ScopedTimer _default_timer{#name, [](auto msg) { std::cout << msg; }};

#define ARCTICDB_SUBSAMPLE_DEFAULT(name)                                                                               \
    arcticdb::ScopedTimer _sub_timer_##name{#name, [](auto msg) { std::cout << msg; }};

#define ARCTICDB_SUBSAMPLE_AGG(name)

inline void set_remotery_thread_name(const char*) {}

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name)

#else

#define ARCTICDB_SAMPLE(name, flags)

#define ARCTICDB_SUBSAMPLE(name, flags)

#define ARCTICDB_SAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_AGG(name)

inline void set_remotery_thread_name(const char*) {}

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name)

#endif // USE_REMOTERY
