/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/performance_tracing.hpp>
#include <folly/Singleton.h>
#include <arcticdb/log/log.hpp>
#include <folly/system/ThreadName.h>
#include <folly/container/F14Map.h>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/pb_util.hpp>

#if defined(USE_OTEL_CPP)
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"

namespace resource       = opentelemetry::sdk::resource;

std::shared_ptr<OtelInstance> OtelInstance::instance(){
    std::call_once(OtelInstance::init_flag_, &OtelInstance::init);
    
    return OtelInstance::instance_;
}

std::shared_ptr<OtelInstance> OtelInstance::instance_;
std::once_flag OtelInstance::init_flag_;

OtelInstance::OtelInstance() {
    // Create OTLP exporter instance
    opentelemetry::exporter::otlp::OtlpGrpcExporterOptions opts;
    constexpr int kNumSpans = 1000;

    trace_sdk::BatchSpanProcessorOptions options{};
    // We make the queue size `KNumSpans`*2+5 because when the queue is half full, a preemptive notif
    // is sent to start an export call, which we want to avoid in this simple example.
    options.max_queue_size = kNumSpans * 2 + 5;
    // Time interval (in ms) between two consecutive exports.
    options.schedule_delay_millis = std::chrono::milliseconds(30);
    // We export `kNumSpans` after every `schedule_delay_millis` milliseconds.
    options.max_export_batch_size = kNumSpans;



    auto resource = resource::Resource::Create({{"service.name", "ArcticDB"}});
    // TODO: Add an env var for these 
    opts.endpoint = "localhost:4317";
    opts.use_ssl_credentials = false;
    auto exporter  = otlp::OtlpGrpcExporterFactory::Create(opts);
    // auto processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));
    auto processor = trace_sdk::BatchSpanProcessorFactory::Create(std::move(exporter), options);

    provider_ = trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
    main_span_ = get_tracer()->StartSpan("ArcticDB");
    // TODO: Maybe add a console exporter for debugging

    // Set the global trace provider
    // trace::Provider::SetTracerProvider(provider_);
}

OtelInstance::~OtelInstance() {
    // std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    // trace::Provider::SetTracerProvider(none);
    main_span_->End();
}

#endif

namespace arcticdb::detail {
    struct ThreadNameCache {
        folly::F14FastMap<const char *, std::string> fqn_by_task_name_;
        std::string thread_name_;

        ThreadNameCache():fqn_by_task_name_(),thread_name_(folly::getCurrentThreadName().value_or("Thread")){}

        std::string_view get_thread_name(const char * task_name){
            if(auto fqn_it = fqn_by_task_name_.find(task_name); fqn_it != fqn_by_task_name_.end()){
                return fqn_it->second;
            }
            auto [fqn_it, inserted] = fqn_by_task_name_.emplace(task_name, fmt::format("{}/{}", thread_name_, task_name));
            return fqn_it->second;
        }
    };
}
