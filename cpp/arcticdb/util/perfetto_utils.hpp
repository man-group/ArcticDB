/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#ifdef ARCTICDB_PERFORMANCE_SAMPLING

#include <perfetto.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

PERFETTO_DEFINE_CATEGORIES(perfetto::Category("arcticdb.processing"));
#ifdef __clang__
#define ARCTICDB_TRACE_EVENT(...)                                                                                      \
    _Pragma("clang diagnostic push") _Pragma("clang diagnostic ignored \"-Wundefined-internal\"")                      \
            TRACE_EVENT(__VA_ARGS__) _Pragma("clang diagnostic pop")
#else
#define ARCTICDB_TRACE_EVENT(...) TRACE_EVENT(__VA_ARGS__)
#endif

#define ARCTICDB_TRACE_EVENT_BEGIN(...) TRACE_EVENT_BEGIN(__VA_ARGS__)
#define ARCTICDB_TRACE_EVENT_END(...) TRACE_EVENT_END(__VA_ARGS__)

namespace arcticdb::tracing {

/// Must be called once per process before any TracingSession is created
void initialize();

struct TracingSessionConfig {
    std::vector<std::string> disabled_categories;
    std::vector<std::string> enabled_categories = {"*"};
    size_t buffer_size_kb = 1024;
};

class TracingSession {
  public:
    explicit TracingSession(const TracingSessionConfig& cfg);
    void start();
    void stop();
    void stop_and_flush(std::string_view file_dest);
    /// Reading the trace is destructive, each call writes only the events recorded since the previous one
    void flush(std::string_view file_dest);

  private:
    enum class State : uint8_t { CONFIGURED, STARTED, STOPPED };
    std::unique_ptr<perfetto::TracingSession> session_;
    State state_ = State::CONFIGURED;
};

} // namespace arcticdb::tracing

#else

#define ARCTICDB_TRACE_EVENT(...)
#define ARCTICDB_TRACE_EVENT_BEGIN(...)
#define ARCTICDB_TRACE_EVENT_END(...)
#endif
