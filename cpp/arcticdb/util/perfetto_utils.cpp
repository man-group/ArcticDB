/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#ifdef ARCTICDB_PERFORMANCE_SAMPLING
#include <arcticdb/util/perfetto_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

#include <filesystem>
#include <fstream>

PERFETTO_TRACK_EVENT_STATIC_STORAGE();

namespace {

perfetto::TraceConfig get_config(const arcticdb::tracing::TracingSessionConfig& cfg) {
    perfetto::TraceConfig result;

    result.add_buffers()->set_size_kb(cfg.buffer_size_kb);

    auto* data_source_cfg = result.add_data_sources()->mutable_config();
    data_source_cfg->set_name("track_event");

    perfetto::protos::gen::TrackEventConfig track_event_cfg;
    for (const auto& cat : cfg.disabled_categories) {
        track_event_cfg.add_disabled_categories(cat);
    }
    for (const auto& cat : cfg.enabled_categories) {
        track_event_cfg.add_enabled_categories(cat);
    }
    data_source_cfg->set_track_event_config_raw(track_event_cfg.SerializeAsString());
    return result;
}
} // namespace

namespace arcticdb::tracing {

void initialize() {
    perfetto::TracingInitArgs args;
    args.backends = perfetto::kInProcessBackend;
    perfetto::Tracing::Initialize(args);
    perfetto::TrackEvent::Register();
}

TracingSession::TracingSession(const TracingSessionConfig& cfg) {
    util::check(perfetto::Tracing::IsInitialized(), "Perfetto must be initialized before creating a tracing session");
    session_ = perfetto::Tracing::NewTrace();
    session_->Setup(get_config(cfg));
}

void TracingSession::start() {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            state_ == State::CONFIGURED, "A tracing session can be started only once"
    );
    session_->StartBlocking();
    state_ = State::STARTED;
}

void TracingSession::stop() {
    if (state_ != State::STARTED) {
        return;
    }
    perfetto::TrackEvent::Flush();
    session_->StopBlocking();
    state_ = State::STOPPED;
}

void TracingSession::stop_and_flush(std::string_view file_dest) {
    stop();
    flush(file_dest);
}

void TracingSession::flush(std::string_view file_dest) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            state_ != State::CONFIGURED, "Cannot flush a tracing session which was never started"
    );
    if (state_ == State::STARTED) {
        perfetto::TrackEvent::Flush();
        session_->FlushBlocking();
    }
    const std::vector<char> trace = session_->ReadTraceBlocking();
    std::ofstream result_file(std::filesystem::path(file_dest), std::ios::out | std::ios::binary);
    if (!result_file.is_open()) {
        log::root().warn("Failed to open trace file {}, the trace is discarded", file_dest);
        return;
    }
    result_file.write(trace.data(), std::streamsize(trace.size()));
    if (!result_file.good()) {
        log::root().warn("Failed to write trace file {}", file_dest);
    }
}
} // namespace arcticdb::tracing
#endif
