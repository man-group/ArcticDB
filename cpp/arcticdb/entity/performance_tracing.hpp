/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/timer.hpp>

#include <memory>

#define ARCTICDB_RUNTIME_SAMPLE(name, flags) \
static bool _scoped_timer_active_ = ConfigsMap::instance()->get_int("Logging.timings", 0) == 1 || ConfigsMap::instance()->get_int("Logging.ALL", 0) == 1; \
arcticdb::ScopedTimer runtime_timer = !_scoped_timer_active_ ? arcticdb::ScopedTimer() : arcticdb::ScopedTimer(#name, [](auto msg) { \
    log::timings().debug(msg); \
});

#define ARCTICDB_RUNTIME_SUBSAMPLE(name, flags) \
static bool _scoped_subtimer_##name_active_ = ConfigsMap::instance()->get_int("Logging.timer", 0) == 1; \
arcticdb::ScopedTimer runtime_sub_timer_##name = !_scoped_subtimer_##name_active_ ? arcticdb::ScopedTimer() : arcticdb::ScopedTimer(#name, [](auto msg) { \
    log::timings().debug(msg); \
});


#ifdef USE_OTEL_CPP
#include <array>
#include <span>
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"

namespace trace     = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace otlp      = opentelemetry::exporter::otlp;

class OtelInstance {
  public:
    static std::shared_ptr<OtelInstance> instance();
    OtelInstance();
    ~OtelInstance();
    static std::shared_ptr<OtelInstance> instance_;
    static std::once_flag init_flag_;
    static void destroy_instance(){instance_.reset();}
    std::shared_ptr<trace::TracerProvider> provider_;
    nostd::shared_ptr<trace::Span> main_span_;

    nostd::shared_ptr<trace::Tracer> get_tracer()
    {
        return provider_->GetTracer("ArcticDB", OPENTELEMETRY_SDK_VERSION);
    }

    static void init(){
        instance_ = std::make_shared<OtelInstance>();
    }
};

#define ARCTICDB_START_TRACE(msg, flags, trace_id, span_id) \
        auto instance = OtelInstance::instance();  \
        auto tracer = instance->get_tracer(); \
        instance->main_span_->End(); \
        auto trace_msg = std::string("MAIN TRACE: ") + msg; \
        instance->main_span_ = tracer->StartSpan(trace_msg); \
        if (flags != 0) instance->main_span_->SetAttribute("flags", flags); 

#define ARCTICDB_SAMPLE(msg, flags) \
        auto instance = OtelInstance::instance();  \
        auto tracer = instance->get_tracer(); \
        auto active_span = tracer->WithActiveSpan(instance->main_span_); \
        auto trace_msg = __PRETTY_FUNCTION__ + std::string(": ") + msg; \
        auto span = tracer->StartSpan(trace_msg); \
        if (flags != 0) span->SetAttribute("flags", flags); \
        auto scoped_span = trace::Scope(span);

#define ARCTICDB_SUBSAMPLE(name, flags)

#define ARCTICDB_SAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_AGG(name)

inline void set_remotery_thread_name(const char* ) { }

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name)

#elif USE_REMOTERY
#define ARCTICDB_SAMPLE(name, flags) \
        auto instance = OtelInstance::instance();  \
        rmt_ScopedCPUSample(name, flags);

#define ARCTICDB_SUBSAMPLE(name, flags) \
        rmt_ScopedCPUSample(name, flags);

#define ARCTICDB_SAMPLE_DEFAULT(name) \
        ARCTICDB_SAMPLE(name, 0)

#define ARCTICDB_SUBSAMPLE_DEFAULT(name) \
        ARCTICDB_SUBSAMPLE(name, 0)

#define ARCTICDB_SUBSAMPLE_AGG(name) \
        rmt_ScopedCPUSample(name, RMTSF_Aggregate);

void set_remotery_thread_name(const char* task_name);

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name) \
        rmt_LogText(task_name);

#elif defined(ARCTICDB_LOG_PERFORMANCE)
#define ARCTICDB_SAMPLE(name, flags) \
arcticdb::ScopedTimer timer{#name, [](auto msg) { \
    std::cout << msg; \
}};

#define ARCTICDB_SUBSAMPLE(name, flags) \
arcticdb::ScopedTimer sub_timer_##name{#name, [](auto msg) { \
std::cout << msg; \
}};

#define ARCTICDB_SAMPLE_DEFAULT(name) \
arcticdb::ScopedTimer default_timer{#name, [](auto msg) { \
std::cout << msg; \
}};

#define ARCTICDB_SUBSAMPLE_DEFAULT(name) \
arcticdb::ScopedTimer sub_timer_##name{#name, [](auto msg) { \
std::cout << msg; \
}};

#define ARCTICDB_SUBSAMPLE_AGG(name)

inline void set_remotery_thread_name(const char* ) { }

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name)

#else

#define ARCTICDB_SAMPLE(name, flags)

#define ARCTICDB_SUBSAMPLE(name, flags)

#define ARCTICDB_SAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_DEFAULT(name)

#define ARCTICDB_SUBSAMPLE_AGG(name)

inline void set_remotery_thread_name(const char* ) { }

#define ARCTICDB_SAMPLE_THREAD() set_remotery_thread_name("Arcticdb")

#define ARCTICDB_SAMPLE_LOG(task_name)

#endif // USE_REMOTERY

