/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/caller_spans.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <folly/system/ThreadName.h>

#include <fmt/format.h>

namespace arcticdb::caller_spans {

namespace {

std::atomic<bool>& flag() {
    // Read once, from the same key the task records and the storage records
    // use: one switch lights the whole capture, and a reader never has to ask
    // which of three instruments was on.
    //
    // TaskScheduler.LogCallerSpans overrides it, and exists for exactly one
    // purpose: pricing this instrument against the one already shipped. With
    // both keys the same binary can be run with the task records alone and with
    // the task records plus these, one environment variable apart, so the
    // difference between the two is this instrument and cannot be a build.
    static std::atomic<bool> on{
            ConfigsMap::instance()->get_int(
                    "TaskScheduler.LogCallerSpans",
                    ConfigsMap::instance()->get_int("TaskScheduler.LogTaskStats", 0)
            ) != 0
    };
    return on;
}

// Nesting on this thread. Not atomic and deliberately not: it describes one
// thread's own stack and is touched by nobody else.
thread_local uint32_t thread_depth = 0;

} // namespace

bool enabled() { return flag().load(std::memory_order_relaxed); }

void set_enabled(bool on) { flag().store(on, std::memory_order_relaxed); }

uint32_t current_depth() { return thread_depth; }

std::string format_caller_span_line(
        std::string_view thread, std::string_view name, uint32_t depth, entity::timestamp start_ns,
        entity::timestamp dur_ns
) {
    return fmt::format(
            "caller_span thread={} name={} depth={} start_ns={} dur_ns={}", thread, name, depth, start_ns, dur_ns
    );
}

Span::Span(const char* name) noexcept : name_(name) {
    if (!enabled()) {
        return;
    }
    // The level is checked here as well as at the sink because spdlog would
    // otherwise take the formatted string and drop it — the trap gha 83
    // measured on the shipped task observer, which formats eagerly through
    // ARCTICDB_RUNTIME_DEBUG. A span whose line will not be printed must cost
    // nothing but this branch.
    if (!log::schedule().should_log(spdlog::level::debug)) {
        return;
    }
    armed_ = true;
    depth_ = thread_depth++;
    start_ = std::chrono::steady_clock::now();
}

Span::~Span() {
    if (!armed_) {
        return;
    }
    const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count();
    --thread_depth;
    const auto named = folly::getCurrentThreadName();
    const std::string thread = named.has_value() && !named->empty() ? *named : std::string{"unknown"};
    log::schedule().debug(
            "{}",
            format_caller_span_line(
                    thread,
                    name_,
                    depth_,
                    std::chrono::duration_cast<std::chrono::nanoseconds>(start_.time_since_epoch()).count(),
                    elapsed
            )
    );
}

} // namespace arcticdb::caller_spans
