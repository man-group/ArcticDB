/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <mutex>
#include <memory>
#include <stack>
#include <list>
#include <atomic>

namespace arcticdb::stats_query {
using GroupableStatStack = std::stack<std::shared_ptr<std::pair<std::string, std::string>>>;

class StatsQuery;
// process-global stats entry list
class StatsEntries {
public:
    void reset_stats();
    void add_stat(GroupableStatStack info, std::string col_name, std::string value);
private:
    std::mutex mutex_;
    std::list<std::pair<GroupableStatStack, std::pair<std::string, std::string>>> stats;
};

extern StatsEntries stats_entries;
// TODO: Support enable in the work of a particualr python thread only, need to 
// 1. Convert this to python_thread_local variable; Check Py_tss_t in python.h
// 2. Remove atomic
extern std::atomic<bool> stats_query_enabled;

// thread-global instance
class StatsQuery { // this will be passed to folly worker threads
public:
    friend class GroupableStat;
    static std::shared_ptr<StatsQuery> instance();
    static void reset_stats();
private:
    std::mutex mutex_;
    GroupableStatStack info_;
    thread_local static std::shared_ptr<StatsQuery> instance_;
    thread_local static bool stats_query_enabled_;
};

// function-local object so the additional info will be removed from the stack when the info object gets detroyed
class GroupableStat {
public:
    GroupableStat(std::shared_ptr<StatsQuery> stats_query, std::string col_name, std::string value) : stats_query_(std::move(stats_query)) {
        stats_query_->info_.push(std::make_shared<std::pair<std::string, std::string>>(std::move(col_name), std::move(value)));
    }
    ~GroupableStat() {
        stats_query_->info_.pop();
    }
private:
    std::shared_ptr<StatsQuery> stats_query_;
};
}

#define STATS_QUERY_ADD_GROUPABLE_STAT(...) \
    std::unique_ptr<arcticdb::stats_query::GroupableStat> stats_query_info; \
    if (arcticdb::stats_query::stats_query_enabled.load(std::memory_order_relaxed)) { \
        auto stats_query = arcticdb::stats_query::StatsQuery::instance(); \
        stats_query_info = std::make_unique<arcticdb::stats_query::GroupableStat>(stats_query, __VA_ARGS__); \
    }
#define STATS_QUERY_ADD_STAT(...) \
    if (arcticdb::stats_query::stats_query_enabled.load(std::memory_order_relaxed)) { \
        auto stats_query = arcticdb::stats_query::StatsQuery::instance(); \
        stats_entries.add_stat(stats_query->info_, __VA_ARGS__); \
    }

