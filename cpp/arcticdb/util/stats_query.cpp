/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#include <arcticdb/util/stats_query.hpp>
#include <mutex>

namespace arcticdb::stats_query {

thread_local std::shared_ptr<StatsQuery> StatsQuery::instance_ = nullptr;
std::atomic<bool> stats_query_enabled = false;
StatsEntries stats_entries;

std::shared_ptr<StatsQuery> StatsQuery::instance(){
    if (!instance_) {
        instance_ = std::make_shared<StatsQuery>();
    }
    return instance_;
}

void StatsQuery::reset_stats(){
    stats_entries.reset_stats();
}

void StatsEntries::reset_stats(){
    std::lock_guard<std::mutex> lock(mutex_);
    stats.clear();
}

void StatsEntries::add_stat(GroupableStatStack info, std::string col_name, std::string value){
    std::lock_guard<std::mutex> lock(mutex_);
    stats.emplace_back(std::move(info), std::make_pair(std::move(col_name), std::move(value)));
}

}