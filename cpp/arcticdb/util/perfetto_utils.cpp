#ifdef ARCTICDB_PERFORMANCE_SAMPLING
#include <arcticdb/util/perfetto_utils.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace {

using namespace arcticdb;
perfetto::TraceConfig get_config(const tracing::TracingSessionCofig& cfg) {

    perfetto::TraceConfig result;

    result.add_buffers()->set_size_kb(cfg.buffer_size_kb);

    auto* data_source_cfg = result.add_data_sources()->mutable_config();
    data_source_cfg->set_name("track_event");

    perfetto::protos::gen::TrackEventConfig track_event_cfg;
    for (const std::string& cat : cfg.disabled_categories) {
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
TracingSession::TracingSession(const TracingSessionCofig& cfg) {
    session_ = perfetto::Tracing::NewTrace();
    session_->Setup(get_config(cfg));
}

void TracingSession::start() { session_->StartBlocking(); }

void TracingSession::stop_and_flush(std::string_view file_dest) {
    stop();
    flush(file_dest);
}

void TracingSession::flush(std::string_view file_dest) {
    std::vector<char> trace = session_->ReadTraceBlocking();
    std::ofstream result_file(std::filesystem::path(file_dest), std::ios::out | std::ios::binary);
    result_file.write(trace.data(), std::streamsize(trace.size()));
}
} // namespace arcticdb::tracing
#endif
