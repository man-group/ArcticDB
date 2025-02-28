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
#include <map>
#include <atomic>
#include <type_traits>
#include <string>
#include <ctime>
#include <chrono>
#include <fmt/format.h>

namespace arcticdb::util::stats_query {
using GroupableStats = std::vector<std::shared_ptr<std::pair<std::string, std::string>>>;


// thread-global instance
class StatsInstance { // this will be passed to folly worker threads
public:
    static std::shared_ptr<StatsInstance> instance();
    static void copy_instance(std::shared_ptr<StatsInstance>& ptr);
    static void pass_instance(std::shared_ptr<StatsInstance>&& ptr);
    static void reset_stats();

    GroupableStats info_;
private:
    thread_local static std::shared_ptr<StatsInstance> instance_;
};

// process-global stats entry list
class GroupableStat;
using StatsOutputFormat = std::vector<std::map<std::string, std::string>>;
class StatsQuery {
public:
    void reset_stats();
    
    template<typename T>
    void add_stat(std::shared_ptr<StatsInstance>& stats_instance, const std::string& col_name, T&& value) {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats.emplace_back(stats_instance->info_, std::make_pair(col_name, fmt::format("{}", std::forward<T>(value))));
    }

    StatsOutputFormat get_stats();
    bool is_enabled();
    void register_new_query_stat_tool();
    void deregister_query_stat_tool();

    // TODO: Support enable in the work of a particualr python thread only, need to 
    // 1. Convert this to python_thread_local variable; Check Py_tss_t in python.h
    // 2. Remove atomic
    std::atomic<bool> stats_query_enabled = false;
private:
    std::atomic<int32_t> query_stat_tool_count = 0;
    std::mutex stats_mutex_;
    //TODO: Change to std::list<std::pair<GroupableStats, std::pair<std::string, std::variant<std::string, xxx>>> 
    std::list<std::pair<GroupableStats, std::pair<std::string, std::string>>> stats;
};
extern StatsQuery stats_query;


// function-local object so the additional info will be removed from the stack when the info object gets detroyed
class GroupableStat {
    public:
        template<typename T>
        GroupableStat(std::shared_ptr<StatsInstance> stats_instance, bool log_time, std::string col_name, T&& value) : 
                stats_instance_(std::move(stats_instance)),
                start_(std::chrono::high_resolution_clock::now()),
                log_time_(log_time) {
            stats_instance_->info_.push_back(std::make_shared<std::pair<std::string, std::string>>(std::move(col_name), fmt::format("{}", std::forward<T>(value))));
        }
        ~GroupableStat() {
            if (log_time_) {
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_);
                stats_query.add_stat(stats_instance_, "time", duration.count());
            }
            stats_instance_->info_.pop_back();
        }
    private:
        std::shared_ptr<StatsInstance> stats_instance_;
        std::chrono::time_point<std::chrono::high_resolution_clock> start_;
        bool log_time_;
    };

}

#define GROUPABLE_STAT_PTR_NAME(x) stats_query_info##x

#define STATS_QUERY_ADD_GROUPABLE_STAT_IMPL(log_time, col_name, value) \
    std::unique_ptr<arcticdb::util::stats_query::GroupableStat> GROUPABLE_STAT_PTR_NAME(col_name); \
    if (arcticdb::util::stats_query::stats_query.stats_query_enabled.load(std::memory_order_relaxed)) { \
        auto stats_instance = arcticdb::util::stats_query::StatsInstance::instance(); \
        GROUPABLE_STAT_PTR_NAME(col_name) = std::make_unique<arcticdb::util::stats_query::GroupableStat>(stats_instance, log_time, #col_name, value); \
    }
#define STATS_QUERY_ADD_GROUPABLE_STAT(col_name, value) STATS_QUERY_ADD_GROUPABLE_STAT_IMPL(false, col_name, value)
#define STATS_QUERY_ADD_GROUPABLE_STAT_WITH_TIME(col_name, value) STATS_QUERY_ADD_GROUPABLE_STAT_IMPL(true, col_name, value)

#define STATS_QUERY_ADD_STAT_IMPL(col_name, value) \
    auto stats_instance = arcticdb::util::stats_query::StatsInstance::instance(); \
    arcticdb::util::stats_query::stats_query.add_stat(stats_instance, #col_name, value);
#define STATS_QUERY_ADD_STAT(col_name, value) \
    if (arcticdb::util::stats_query::stats_query.stats_query_enabled.load(std::memory_order_relaxed)) { \
        STATS_QUERY_ADD_STAT_IMPL(col_name, value) \
    }
#define STATS_QUERY_ADD_STAT_CONDITIONAL(condition, col_name, value) \
    if (arcticdb::util::stats_query::stats_query.stats_query_enabled.load(std::memory_order_relaxed)) { \
        if (condition) { \
            STATS_QUERY_ADD_STAT(col_name, value) \
        } \
    }

