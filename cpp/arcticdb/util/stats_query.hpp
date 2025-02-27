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

namespace arcticdb::util::stats_query {
using GroupableStats = std::vector<std::shared_ptr<std::pair<std::string, std::string>>>;

// process-global stats entry list
class StatsQuery {
public:
    void reset_stats();
    void add_stat(GroupableStats info, std::string col_name, std::string value);
    std::vector<std::vector<std::string>> get_stats();
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
    std::list<std::pair<GroupableStats, std::pair<std::string, std::string>>> stats;
};

extern StatsQuery stats_query;

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

// function-local object so the additional info will be removed from the stack when the info object gets detroyed
class GroupableStat {
public:
    GroupableStat(std::shared_ptr<StatsInstance> stats_query, std::string col_name, std::string value) : stats_query_(std::move(stats_query)) {
        stats_query_->info_.push_back(std::make_shared<std::pair<std::string, std::string>>(std::move(col_name), std::move(value)));
    }
    ~GroupableStat() {
        stats_query_->info_.pop_back();
    }
private:
    std::shared_ptr<StatsInstance> stats_query_;
};
}

#define STATS_QUERY_ADD_GROUPABLE_STAT(...) \
    std::unique_ptr<arcticdb::util::stats_query::GroupableStat> stats_query_info; \
    if (arcticdb::util::stats_query::stats_query.stats_query_enabled.load(std::memory_order_relaxed)) { \
        auto stats_query = arcticdb::util::stats_query::StatsInstance::instance(); \
        stats_query_info = std::make_unique<arcticdb::util::stats_query::GroupableStat>(stats_query, __VA_ARGS__); \
    }
#define STATS_QUERY_ADD_STAT(...) \
    if (arcticdb::util::stats_query::stats_query.stats_query_enabled.load(std::memory_order_relaxed)) { \
        auto stats_query = arcticdb::util::stats_query::StatsInstance::instance(); \
        arcticdb::util::stats_query::stats_query.add_stat(stats_query->info_, __VA_ARGS__); \
    }

