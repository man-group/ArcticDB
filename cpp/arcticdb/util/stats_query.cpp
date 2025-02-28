/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#include <arcticdb/util/stats_query.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

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

StatsOutputFormat StatsQuery::get_stats() { 
    std::lock_guard<std::mutex> lock(stats_mutex_);
    StatsOutputFormat result;
    for (const auto& [infos, value] : stats_query.stats) {
        StatsOutputFormat::value_type items;
        for (const auto &info : infos ) {
            if (!items.emplace(info->first, info->second).second) {
                arcticdb::log::storage().warn("Duplicate key in stats query {}:{}", info->first, info->second);
            }
        }
        if (!items.emplace(value.first, value.second).second) {
            arcticdb::log::storage().warn("Duplicate key in stats query {}:{}", value.first, value.second);
        }
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