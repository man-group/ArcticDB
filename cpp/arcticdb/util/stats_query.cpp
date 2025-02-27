/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#include <arcticdb/util/stats_query.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util::stats_query {

thread_local std::shared_ptr<StatsInstance> StatsInstance::instance_ = nullptr;
StatsQuery stats_query;

std::shared_ptr<StatsInstance> StatsInstance::instance(){
    if (!instance_) {
        // TODO: Add checking for non python threads must have instance_ as non-nullptr
        ARCTICDB_DEBUG(log::version(), "StatsInstance created");
        instance_ = std::make_shared<StatsInstance>();
    }
    return instance_;
}

void StatsInstance::reset_stats(){
    stats_query.reset_stats();
}


// Could be called in 3 situations:
// 1. By the head of the chain of task - Need copy of the instance
// 2. By the appended task of the chain of task that runs on the same thread immediately after the preceding - Nothing needs to be done
// 3. None of the above - Need to pass the instance
// Case 1
void StatsInstance::copy_instance(std::shared_ptr<StatsInstance>& ptr){
    ARCTICDB_DEBUG(log::version(), "StatsInstance being copied");
    instance_ = std::make_shared<StatsInstance>(*ptr);
}

// Case 2 and 3
void StatsInstance::pass_instance(std::shared_ptr<StatsInstance>&& ptr){
    ARCTICDB_DEBUG(log::version(), "StatsInstance being passed");
    instance_ = std::move(ptr);
}


void StatsQuery::reset_stats(){
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats.clear();
}

void StatsQuery::add_stat(GroupableStats info, std::string col_name, std::string value){
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats.emplace_back(std::move(info), std::make_pair(std::move(col_name), std::move(value)));
}

std::vector<std::vector<std::string>> StatsQuery::get_stats() { 
    std::lock_guard<std::mutex> lock(stats_mutex_);
    std::vector<std::vector<std::string>> result;
    for (const auto& [infos, value] : stats_query.stats) {
        std::vector<std::string> items;
        for (const auto &info : infos ) {
            items.push_back(info->first);
            items.push_back(info->second);
        }
        items.push_back(value.first);
        items.push_back(value.second);
        result.push_back(std::move(items));
    }
    return result;
}

bool StatsQuery::is_enabled(){
    return stats_query_enabled.load(std::memory_order_relaxed);
}

void StatsQuery::register_new_query_stat_tool() {
    auto new_stat_tool_count = query_stat_tool_count.fetch_add(1, std::memory_order_relaxed) + 1;
    if (new_stat_tool_count) {
        stats_query_enabled.store(true, std::memory_order_relaxed);
    }
}

void StatsQuery::deregister_query_stat_tool() {
    auto new_stat_tool_count = query_stat_tool_count.fetch_sub(1, std::memory_order_relaxed) - 1;
    util::check(new_stat_tool_count >= 0,
                "Stat Query Tool count cannot be negative");
    if (new_stat_tool_count == 0) {
        stats_query_enabled.store(false, std::memory_order_relaxed);
        reset_stats();
    }
}

}