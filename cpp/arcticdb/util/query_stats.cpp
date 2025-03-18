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

#include <pybind11/pybind11.h>

namespace arcticdb::util::query_stats {

namespace py = pybind11;

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
        thread_local_var_.current_level_ = thread_local_var_.root_level_;
        std::lock_guard<std::mutex> lock(root_level_mutex_);
        root_levels_.emplace_back(thread_local_var_.root_level_);
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
        std::lock_guard<std::mutex> lock(root_level_mutex_);
        root_levels_.emplace_back(thread_local_var_.root_level_);
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

std::string get_runtime_arcticdb_call(const std::string& default_arcticdb_call){
    // Inside your C++ function
    py::object traceback_module = py::module::import("traceback");
    py::object stack_summary = traceback_module.attr("extract_stack")();

    // Extract last 1 or 2 frames based on list size
    std::string func_name = default_arcticdb_call;
    for (size_t i = py::len(stack_summary); i > 0; i--) 
    {
        py::object relevant_frame = stack_summary.attr("__getitem__")(i - 1);
        std::string filename = py::cast<std::string>(relevant_frame.attr("filename"));
        // get top-most arcticdb API being called in the stack
        if (filename.find("arcticdb/version_store/_store.py") != std::string::npos || filename.rfind("arcticdb/version_store/library.py") != std::string::npos) {
            func_name = py::cast<std::string>(relevant_frame.attr("name"));
        }
    }
    return func_name;
}

StatsGroup::StatsGroup(bool log_time, GroupName col, const std::string& default_value) : 
        prev_level_(QueryStats::instance().current_level()),
        start_(std::chrono::high_resolution_clock::now()),
        log_time_(log_time) {
    check(async::is_folly_thread || 
        prev_level_ != QueryStats::instance().root_level() || 
        col == GroupName::arcticdb_call,
        "Root of query stats map should always be set to arcticdb_call");
    auto& next_level_map = prev_level_->next_level_maps_[static_cast<size_t>(col)];
    std::string value = default_value;
    if (col == GroupName::arcticdb_call) {
        value = get_runtime_arcticdb_call(default_value);
    }
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
        // The 1 ms minimum is for giving meaningful result in local test with moto. In real world the S3 API will take >1 ms
        stats[static_cast<size_t>(StatsName::total_time_ms)] += std::max(static_cast<int64_t>(1), duration.count());
        stats[static_cast<size_t>(StatsName::count)]++;
    }
    query_stats_instance.set_level(prev_level_);
    if (!async::is_folly_thread && query_stats_instance.current_level() == query_stats_instance.root_level()) {
        query_stats_instance.merge_levels();
    }
}
}