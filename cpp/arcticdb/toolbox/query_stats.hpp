/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/ThreadCachedInt.h>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/util/constants.hpp>
#include <atomic>
#include <string>
#include <chrono>
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>

namespace arcticdb::query_stats {
enum class TaskType : size_t {
    S3_ListObjectsV2 = 0,
    S3_PutObject = 1,
    S3_GetObject = 2,
    S3_GetObjectAsync = 3,
    S3_DeleteObjects = 4,
    S3_HeadObject = 5,
    Memory_ListObjects = 6,
    Memory_PutObject = 7,
    Memory_GetObject = 8,
    Memory_DeleteObject = 9,
    Memory_HeadObject = 10,
    END
};

// Call counts/times separate successes (including expected missing keys) from failures; times include retries.
// Per-attempt stats require synchronous ArcticCurlHttpClient calls (Linux/macOS); otherwise they remain zero.
// See toolbox/query_stats.py:get_query_stats for metric definitions and formulas.
enum class StatType : std::uint8_t {
    TOTAL_TIME_MS = 0,
    COUNT = 1,
    SIZE_BYTES = 2,
    FAILURE_TIME_MS = 3,
    FAILURE_COUNT = 4,
    ATTEMPTS = 5,
    ATTEMPT_TIME_MS = 6,
    NETWORK_ERRORS = 7,
    SERVER_ERRORS = 8,
    TTFB_MS = 9,
    CONNECT_MS = 10,
    NEW_CONNECTIONS = 11,
    END
};

// TOTAL_TIME_MS, FAILURE_TIME_MS and ATTEMPT_TIME_MS are held in nanoseconds
struct OperationStats {
    std::array<folly::ThreadCachedInt<uint64_t>, static_cast<size_t>(StatType::END)> stats_;
    void reset_stats() {
        for (auto& stat : stats_) {
            stat.set(0);
        }
    }
    folly::ThreadCachedInt<uint64_t>& operator[](StatType stat_type) { return stats_[static_cast<size_t>(stat_type)]; }
    const folly::ThreadCachedInt<uint64_t>& operator[](StatType stat_type) const {
        return stats_[static_cast<size_t>(stat_type)];
    }
    OperationStats() { reset_stats(); }
};

using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

// Times one call. finish() records it as succeeded or failed, with the time up to then. If finish() is not called, the
// destructor records it, as failed if an exception is being thrown. While the call is being timed, the per-attempt
// stats recorded on the thread that created this are added to it. Does nothing if query stats were disabled.
class RAIIAddTime {
  public:
    RAIIAddTime() = default;
    RAIIAddTime(OperationStats& stats, TimePoint start);
    RAIIAddTime(RAIIAddTime&& other) noexcept;
    RAIIAddTime& operator=(RAIIAddTime&&) = delete;
    RAIIAddTime(const RAIIAddTime&) = delete;
    RAIIAddTime& operator=(const RAIIAddTime&) = delete;
    ~RAIIAddTime();

    void finish(bool succeeded);

  private:
    OperationStats* stats_ = nullptr;
    TimePoint start_;
    int uncaught_exceptions_ = 0;
};

/*
Example output, for a write that S3 accepted at the third attempt after two DNS failures:
{
"storage_operations": {
    "S3_PutObject": {
        "TABLE_DATA": {
            "attempt_time_ms": 33,
            "attempts": 3,
            "connect_ms": 4,
            "count": 1,
            "failure_count": 0,
            "failure_time_ms": 0,
            "network_errors": 2,
            "new_connections": 1,
            "server_errors": 0,
            "size_bytes": 103,
            "total_time_ms": 108,
            "ttfb_ms": 15
        }
    }
}
*/

class QueryStats {
  public:
    using OperationStatsOutput = std::map<std::string, uint64_t>;
    using QueryStatsOutput = std::map<std::string, std::map<std::string, std::map<std::string, OperationStatsOutput>>>;
    using STATS_BY_KEY_TYPE = std::array<OperationStats, static_cast<size_t>(entity::KeyType::UNDEFINED)>;
    using STATS_BY_STORAGE_OP_TYPE = std::array<STATS_BY_KEY_TYPE, static_cast<size_t>(TaskType::END)>;

    ARCTICDB_NO_MOVE_OR_COPY(QueryStats);
    void reset_stats();
    static std::shared_ptr<QueryStats> instance();
    void enable();
    void disable();
    bool is_enabled() const;
    void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value);
    [[nodiscard]] RAIIAddTime add_task_count_and_time(
            TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start = std::nullopt
    );
    QueryStatsOutput get_stats() const;
    QueryStats();

  private:
    static std::once_flag init_flag_;
    static std::shared_ptr<QueryStats> instance_;
    std::atomic<bool> is_enabled_ = false;

    STATS_BY_STORAGE_OP_TYPE stats_by_storage_op_type_;
};

void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value);
[[nodiscard]] RAIIAddTime add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start = std::nullopt
);

// Adds a per-attempt stat to the calls being timed on this thread
void add_to_current_calls(StatType stat_type, uint64_t value);
} // namespace arcticdb::query_stats
