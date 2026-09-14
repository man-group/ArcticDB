/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace arcticdb::caller_spans {

/*
 * One line per named stretch of work, on whichever thread ran it.
 *
 * The task scheduler already logs one line per completed task (`task_stats`,
 * ThreadPoolExecutor's TaskObserver) and the storage brackets already log one
 * line per synchronous storage call. Between the two there is a hole: the work
 * that is neither a task nor a storage call — the version chain walk, the
 * pybind marshalling, the wait on a future, the assembly of the Python result —
 * and on a read over a high-latency store that hole is most of what the calling
 * thread does.
 *
 * A span here is that hole, named. It is deliberately the same shape as the
 * other two records and goes to the same sink behind the same switch, so a
 * capture is one file and the three records share one axis with no anchor
 * between them: `start_ns` is `std::chrono::steady_clock`, which is what folly
 * stamps `TaskInfo::enqueueTime` with (ThreadPoolExecutor.h, `TaskInfo`), and
 * `SpanClockIsTheTaskStatsClock` in the async tests fails to compile if either
 * side ever moves.
 *
 * `depth` is nesting on the emitting thread at the moment the span was entered,
 * which is what lets a reader rebuild the tree from a flat log: a span is
 * contained by the nearest preceding span on the same thread with a smaller
 * depth. Lines are written when a span *ends*, so the log is in completion
 * order and the depths are the only ordering a reader needs.
 */

/// Is the caller-span sink on? `TaskScheduler.LogTaskStats`, the same switch the
/// task records and the storage records use, read once.
bool enabled();

/// For tests. Not a runtime knob: nothing but the config key turns this on in a
/// real process.
void set_enabled(bool on);

/// The line exactly as it is written, so a test can assert on the format rather
/// than on a log.
std::string format_caller_span_line(
        std::string_view thread, std::string_view name, uint32_t depth, entity::timestamp start_ns,
        entity::timestamp dur_ns
);

/// How deeply nested the calling thread currently is. Exposed for tests only.
uint32_t current_depth();

/*
 * The RAII scope. Costs one relaxed atomic load when the sink is off, and
 * nothing else: no clock read, no thread-name lookup, no allocation. When it is
 * on it costs two `steady_clock::now()` calls, one `getCurrentThreadName` and
 * one formatted debug line — measured in gha 131 and reported there.
 *
 * Not copyable and not movable. A span names a lexical scope; moving one would
 * mean a scope that ends somewhere other than where it was written, and the
 * nesting depth would stop being readable off the log.
 */
class Span {
  public:
    explicit Span(const char* name) noexcept;
    ~Span();

    Span(const Span&) = delete;
    Span& operator=(const Span&) = delete;
    Span(Span&&) = delete;
    Span& operator=(Span&&) = delete;

  private:
    const char* name_;
    std::chrono::steady_clock::time_point start_;
    uint32_t depth_ = 0;
    bool armed_ = false;
};

} // namespace arcticdb::caller_spans

#define ARCTICDB_CALLER_SPAN_JOIN_(a, b) a##b
#define ARCTICDB_CALLER_SPAN_NAME_(line) ARCTICDB_CALLER_SPAN_JOIN_(_arcticdb_caller_span_, line)

/// Name the enclosing scope on the task_stats sink. One statement, no branches
/// at the call site, nothing at all when the switch is off.
#define ARCTICDB_CALLER_SPAN(name) arcticdb::caller_spans::Span ARCTICDB_CALLER_SPAN_NAME_(__LINE__){name};
