/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <list>
#include <atomic>
#include <type_traits>
#include <string>
#include <ctime>
#include <chrono>
#include <array>
#include <fmt/format.h>
#include <ankerl/unordered_dense.h>

#include <arcticdb/entity/key.hpp>

namespace arcticdb::util::query_stats {
enum class TaskType : size_t {
    S3_ListObjectsV2 = 0
};

constexpr const size_t NUMBER_OF_KEYS = 29;
constexpr const size_t NUMBER_OF_TASK_TYPES = 1;

class OpStats{
public:
    std::array<std::atomic<uint64_t>, NUMBER_OF_KEYS> logical_key_counts_;
    std::atomic<uint64_t> result_count_;
    std::atomic<uint64_t> total_time_ms_;
    std::atomic<uint64_t> count_;

    OpStats();
    OpStats(const OpStats& other);
    OpStats(OpStats&& other) noexcept;
    OpStats& operator=(const OpStats& other);
};

class CallStats{
public:
    std::array<std::array<OpStats, NUMBER_OF_TASK_TYPES>, NUMBER_OF_KEYS> keys_stats_;
    std::atomic<uint64_t> total_time_ms_;
    std::atomic<uint64_t> count_;

    CallStats();
    CallStats(const CallStats& other);
    CallStats(CallStats&& other) noexcept;
};

class QueryStats {
public:
    void reset_stats();
    static QueryStats& instance();
    void enable();
    void disable();
    bool is_enabled() const;
    void set_call(const std::string& call_name);
    void set_call_stats(std::shared_ptr<CallStats>&& call_stats);
    std::shared_ptr<CallStats> get_call_stats();
    ankerl::unordered_dense::map<std::string, std::shared_ptr<CallStats>> get_calls_stats_map();
    QueryStats(const QueryStats&) = delete;
    QueryStats() = default;
private:
    ankerl::unordered_dense::map<std::string, std::shared_ptr<CallStats>> calls_stats_map_;
    std::mutex calls_stats_map_mutex_;
    thread_local inline static std::shared_ptr<CallStats> call_stat_ptr_ = nullptr;

    static QueryStats instance_;
    bool is_enabled_ = false;
};

class RAIIRunLambda {
public:
    RAIIRunLambda(std::function<void(uint64_t)> lambda);
    ~RAIIRunLambda();
private:
    std::function<void(uint64_t)> lambda_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
};
}

#define QUERY_STATS_SET_CALL(call_name) \
    std::optional<arcticdb::util::query_stats::RAIIRunLambda> log_total_time = std::nullopt; \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        using namespace arcticdb::util::query_stats; \
        QueryStats::instance().set_call(call_name); \
        log_total_time.emplace([](auto time){ \
            auto call_stats = QueryStats::instance().get_call_stats(); \
            call_stats->count_.fetch_add(1, std::memory_order_relaxed); \
            call_stats->total_time_ms_.fetch_add(time, std::memory_order_relaxed); \
        }); \
    }

#define QUERY_STATS_ADD(stat_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        using namespace arcticdb::util::query_stats; \
        QueryStats::instance().get_call_stats()->keys_stats_[static_cast<size_t>(query_stat_key_type)][static_cast<size_t>(query_stat_op)].stat_name##_.fetch_add(value, std::memory_order_relaxed); \
    }
#define QUERY_STATS_ADD_TIME(stat_name) \
    std::optional<arcticdb::util::query_stats::RAIIRunLambda> log_total_time = std::nullopt; \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        using namespace arcticdb::util::query_stats; \
        log_total_time.emplace([&stat_name = QueryStats::instance().get_call_stats()->keys_stats_[static_cast<size_t>(query_stat_key_type)][static_cast<size_t>(query_stat_op)].stat_name##_](auto duration){ \
            stat_name.fetch_add(duration, std::memory_order_relaxed); \
        }); \
    }

#define QUERY_STATS_SET_KEY_TYPE(key_type) \
    static_assert(std::is_same_v<std::remove_cv_t<std::remove_reference_t<decltype(key_type)>>, arcticdb::entity::KeyType>); \
    auto query_stat_key_type = key_type; 
#define QUERY_STATS_SET_TASK_TYPE(task_type) \
    auto query_stat_op = arcticdb::util::query_stats::TaskType::task_type; \
    QUERY_STATS_ADD_TIME(total_time_ms)
