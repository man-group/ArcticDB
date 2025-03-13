/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#include <arcticdb/util/query_stats.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb::util::query_stats {

std::shared_ptr<GroupingLevel> QueryStats::current_level(){
    // current_level_ != nullptr && root_level_ != nullptr -> stats has been setup; Nothing to do
    // current_level_ == nullptr && root_level_ == nullptr -> clean slate; Need to setup
    // current_level_ != nullptr && root_level_ == nullptr -> Something is off
    // current_level_ == nullptr && root_level_ != nullptr -> Something is off
    if (!thread_local_var_.current_level_ || !thread_local_var_.root_level_) {
        check(!async::is_folly_thread, "Folly thread should have its GroupingLevel passed by caller only");
        check(thread_local_var_.current_level_.operator bool() == thread_local_var_.root_level_.operator bool(), 
            "QueryStats root_level_ and current_level_ should be either both null or both non-null");
        thread_local_var_.root_level_ = std::make_shared<GroupingLevel>();
        {
            std::lock_guard<std::mutex> lock(root_level_mutex_);
            root_levels_.emplace_back(thread_local_var_.root_level_);
        }
        thread_local_var_.current_level_ = thread_local_var_.root_level_;
        ARCTICDB_DEBUG(log::version(), "Current GroupingLevel created");
    }
    return thread_local_var_.current_level_;
}

void QueryStats::set_level(std::shared_ptr<GroupingLevel> &level) {
    thread_local_var_.current_level_ = level;
}

void QueryStats::set_root_level(std::shared_ptr<GroupingLevel> &level) {
    thread_local_var_.current_level_ = level;
    thread_local_var_.root_level_ = level;
}

void QueryStats::reset_stats() {
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    std::lock_guard<std::mutex> lock(root_level_mutex_);
    for (auto& level : root_levels_) {
        level->reset_stats();
    }
}

QueryStats& QueryStats::instance() {
    static QueryStats instance;
    return instance;
}

void QueryStats::merge_levels() {
    for (auto& child_level : thread_local_var_.child_levels_) {
        child_level.parent_level_->merge_from(*child_level.root_level_);
    }
    thread_local_var_.child_levels_.clear();
}

void GroupingLevel::reset_stats() {
    stats_.fill(0);
    for (auto& next_level_map : next_level_maps_) {
        next_level_map.clear();
    }
}

void GroupingLevel::merge_from(const GroupingLevel& other) {
    for (size_t i = 0; i < stats_.size(); ++i) {
        stats_[i] += other.stats_[i];
    }
    
    for (size_t i = 0; i < next_level_maps_.size(); ++i) {
        for (const auto& [key, other_level] : other.next_level_maps_[i]) {
            auto& next_level_map = next_level_maps_[i];
            auto it = next_level_map.find(key);
            if (it == next_level_map.end()) {
                it = next_level_map.emplace(key, std::make_shared<GroupingLevel>()).first;
            }
            it->second->merge_from(*other_level);
        }
    }
}

std::shared_ptr<GroupingLevel> QueryStats::root_level() {
    if (!thread_local_var_.root_level_) {
        util::check(!thread_local_var_.current_level_, "QueryStats current_level_ should be null if root_level_ is null");
        thread_local_var_.root_level_ = std::make_shared<GroupingLevel>();
        thread_local_var_.current_level_ = thread_local_var_.root_level_;
        ARCTICDB_DEBUG(log::version(), "Root GroupingLevel created");
    }
    return thread_local_var_.root_level_;
}

const std::vector<std::shared_ptr<GroupingLevel>>& QueryStats::root_levels() const{
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    return root_levels_;
}

bool QueryStats::is_root_level_set() const {
    return thread_local_var_.root_level_.operator bool();
}

void QueryStats::create_child_level(ThreadLocalQueryStatsVar& parent_thread_local_var) {
    auto child_level = std::make_shared<GroupingLevel>();
    set_root_level(child_level);
    std::lock_guard<std::mutex> lock(parent_thread_local_var.child_level_creation_mutex_);
    parent_thread_local_var.child_levels_.emplace_back(parent_thread_local_var.current_level_, child_level);
}

void QueryStats::enable() {
    is_enabled_ = true;
}

void QueryStats::disable() {
    is_enabled_ = false;
}

bool QueryStats::is_enabled() const {
    return is_enabled_;
}

StatsGroup::StatsGroup(bool log_time, GroupName col, const std::string& value) : 
        prev_level_(QueryStats::instance().current_level()),
        start_(std::chrono::high_resolution_clock::now()),
        log_time_(log_time) {
    // check(!async::is_folly_thread || 
    //     prev_level_ != QueryStats::instance().root_level() || 
    //     col == GroupName::arcticdb_call,
    //     "Root of query stats map should always be set to arcticdb_call {} {} {} {} {}", size_t(col), value, !async::is_folly_thread, prev_level_ != QueryStats::instance().root_level());
    // if (prev_level_ == QueryStats::instance().root_level() && col != GroupName::arcticdb_call) {
    //     log::root().warn("Query stats arcticdb_call should be set as the root level");
    // }
    auto& next_level_map = prev_level_->next_level_maps_[static_cast<size_t>(col)];
    if (next_level_map.find(value) == next_level_map.end()) {
        next_level_map[value] = std::make_shared<GroupingLevel>();
    }
    QueryStats::instance().set_level(next_level_map[value]);
}

StatsGroup::~StatsGroup() {
    auto& query_stats_instance = QueryStats::instance();
    if (log_time_) {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_);
        auto& stats = query_stats_instance.current_level()->stats_;
        stats[static_cast<size_t>(StatsName::total_time_ms)] += duration.count();
        stats[static_cast<size_t>(StatsName::count)]++;
    }
    query_stats_instance.set_level(prev_level_);
    if (!async::is_folly_thread && query_stats_instance.current_level() == query_stats_instance.root_level()) {
        query_stats_instance.merge_levels();
    }
}
}