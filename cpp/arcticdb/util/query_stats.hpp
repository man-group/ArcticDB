/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <type_traits>
#include <string>
#include <chrono>
#include <array>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb{
namespace async {
    class TaskScheduler;
};
namespace util::query_stats {
enum class TaskType : size_t {
    S3_ListObjectsV2 = 0,
    S3_PutObject = 1,
    S3_GetObject = 2,
    S3_GetObjectAsync = 3,
    S3_DeleteObjects = 4,
    S3_HeadObject = 5,
    Encode = 6,
    Decode = 7,
    AZURE_UploadFrom = 8,
    AZURE_DownloadTo = 9,
    AZURE_DeleteBlobs = 10,
    AZURE_ListBlobs = 11,
    AZURE_GetProperties = 12,
};

constexpr const size_t NUMBER_OF_KEYS = 29;
constexpr const size_t NUMBER_OF_TASK_TYPES = 13;

class OpStats{
public:
    std::array<std::atomic<uint64_t>, NUMBER_OF_KEYS> logical_key_counts_;
    std::atomic<uint64_t> result_count_;
    std::atomic<uint64_t> total_time_ms_;
    std::atomic<uint64_t> count_;
    std::atomic<uint64_t> uncompressed_size_bytes_;
    std::atomic<uint64_t> compressed_size_bytes_;

    void reset_stats();
    OpStats();
};

class Stats{
public:
    std::array<std::array<OpStats, NUMBER_OF_TASK_TYPES>, NUMBER_OF_KEYS> keys_stats_;
    void reset_stats();
    Stats();
};

class QueryStats {
public:
    void reset_stats();
    static QueryStats& instance();
    void enable();
    void disable();
    bool is_enabled() const;
    const Stats& get_stats(async::TaskScheduler* const instance) const;
    QueryStats(const QueryStats&) = delete;
    QueryStats() = default;

    Stats stats_;
private:

    static QueryStats instance_;
    bool is_enabled_ = false;
};

class RAIIAddTime {
public:
    RAIIAddTime(std::atomic<uint64_t>& time_var, std::optional<std::chrono::time_point<std::chrono::steady_clock>> start = std::nullopt);
    ~RAIIAddTime();
private:
    std::atomic<uint64_t>& time_var_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
};

void add_logical_keys(const entity::KeyType physical_key_type, const TaskType task_type, const SegmentInMemory& segment);
} //util::query_stats
} //arcticdb

#define QUERY_STATS_ADD(stat_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        using namespace arcticdb::util::query_stats; \
        QueryStats::instance().stats_.keys_stats_[static_cast<size_t>(query_stat_key_type)][static_cast<size_t>(query_stat_op)].stat_name##_.fetch_add(value, std::memory_order_relaxed); \
    }
#define QUERY_STATS_ADD_TIME(stat_name, ...) \
    std::optional<arcticdb::util::query_stats::RAIIAddTime> log_total_time = std::nullopt; \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        using namespace arcticdb::util::query_stats; \
        log_total_time.emplace(QueryStats::instance().stats_.keys_stats_[static_cast<size_t>(query_stat_key_type)][static_cast<size_t>(query_stat_op)].stat_name##_, ##__VA_ARGS__); \
    }
#define QUERY_STATS_ADD_LOGICAL_KEYS(segment) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        arcticdb::util::query_stats::add_logical_keys(query_stat_key_type, query_stat_op, segment); \
    }

#define QUERY_STATS_SET_KEY_TYPE(key_type) \
    static_assert(std::is_same_v<std::remove_cv_t<std::remove_reference_t<decltype(key_type)>>, arcticdb::entity::KeyType>); \
    auto query_stat_key_type = key_type;
#define QUERY_STATS_SET_TASK_TYPE(task_type, ...) \
    auto query_stat_op = arcticdb::util::query_stats::TaskType::task_type; \
    QUERY_STATS_ADD_TIME(total_time_ms, ##__VA_ARGS__) \
    QUERY_STATS_ADD(count, 1)
