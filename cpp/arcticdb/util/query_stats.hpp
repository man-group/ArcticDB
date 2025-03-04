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

namespace arcticdb::util::query_stats {
using GroupableStats = std::vector<std::shared_ptr<std::pair<std::string, std::string>>>;


// thread-global instance
class StatsInstance { // this will be passed to folly worker threads
public:
    static std::shared_ptr<StatsInstance> instance();
    static void copy_instance(std::shared_ptr<StatsInstance>& ptr);
    static void pass_instance(std::shared_ptr<StatsInstance>&& ptr);

    GroupableStats info_;
private:
    thread_local inline static std::shared_ptr<StatsInstance> instance_;
};

// process-global stats entry list
class GroupableStat;
using StatsOutputFormat = std::vector<std::map<std::string, std::string>>;
class QueryStats {
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
    static QueryStats& instance();

    // TODO: Support enable in the work of a particualr python thread only, need to 
    // 1. Convert this to python_thread_local variable; Check Py_tss_t in python.h
    // 2. Remove atomic
    std::atomic<bool> query_stats_enabled = false;
private:
    std::atomic<int32_t> query_stat_tool_count = 0;
    std::mutex stats_mutex_;
    //TODO: Change to std::list<std::pair<GroupableStats, std::pair<std::string, std::variant<std::string, xxx>>> 
    std::list<std::pair<GroupableStats, std::pair<std::string, std::string>>> stats;
};


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
                QueryStats::instance().add_stat(stats_instance_, "time", duration.count());
            }
            stats_instance_->info_.pop_back();
        }
    private:
        std::shared_ptr<StatsInstance> stats_instance_;
        std::chrono::time_point<std::chrono::high_resolution_clock> start_;
        bool log_time_;
    };

}

#define GROUPABLE_STAT_NAME(x) query_stats_info##x

#define QUERY_STATS_ADD_GROUPABLE_STAT_IMPL(log_time, col_name, value) \
    std::optional<arcticdb::util::query_stats::GroupableStat> GROUPABLE_STAT_NAME(col_name); \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        auto stats_instance = arcticdb::util::query_stats::StatsInstance::instance(); \
        GROUPABLE_STAT_NAME(col_name).emplace(stats_instance, log_time, #col_name, value); \
    }
#define QUERY_STATS_ADD_GROUPABLE_STAT(col_name, value) QUERY_STATS_ADD_GROUPABLE_STAT_IMPL(false, col_name, value)
#define QUERY_STATS_ADD_GROUPABLE_STAT_WITH_TIME(col_name, value) QUERY_STATS_ADD_GROUPABLE_STAT_IMPL(true, col_name, value)

#define QUERY_STATS_ADD_STAT_IMPL(col_name, value) \
    auto stats_instance = arcticdb::util::query_stats::StatsInstance::instance(); \
    arcticdb::util::query_stats::QueryStats::instance().add_stat(stats_instance, #col_name, value);
#define QUERY_STATS_ADD_STAT(col_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        QUERY_STATS_ADD_STAT_IMPL(col_name, value) \
    }
#define QUERY_STATS_ADD_STAT_CONDITIONAL(condition, col_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        if (condition) { \
            QUERY_STATS_ADD_STAT(col_name, value) \
        } \
    }

