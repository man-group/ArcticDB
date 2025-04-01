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
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/stream/stream_utils.hpp>

#include <pybind11/pybind11.h>
#include <sys/syscall.h>

namespace arcticdb::util::query_stats {

namespace py = pybind11;

std::shared_ptr<GroupingLevel> QueryStats::current_level(){
    // current_level_ != nullptr && root_level_ != nullptr -> stats has been setup; Nothing to do
    // current_level_ == nullptr && root_level_ == nullptr -> clean slate; Need to setup
    // current_level_ != nullptr && root_level_ == nullptr -> Something is off
    // current_level_ == nullptr && root_level_ != nullptr -> Something is off
    if (!thread_local_var_->current_level_ || !thread_local_var_->root_level_) {
        check(!async::is_folly_thread, "Folly thread should have its GroupingLevel passed by caller only");
        check(thread_local_var_->current_level_.operator bool() == thread_local_var_->root_level_.operator bool(), 
            "QueryStats root_level_ and current_level_ should be either both null or both non-null");
        thread_local_var_->root_level_ = std::make_shared<GroupingLevel>();
        thread_local_var_->current_level_ = thread_local_var_->root_level_;
        std::lock_guard<std::mutex> lock(root_level_mutex_);
        root_levels_.emplace_back(thread_local_var_->root_level_);
        ARCTICDB_INFO(log::version(), "Current GroupingLevel created");
    }
    return thread_local_var_->current_level_;
}

void QueryStats::set_level(std::shared_ptr<GroupingLevel> &level) {
    thread_local_var_->current_level_ = level;
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
    // std::lock_guard<std::mutex> lock(thread_local_var_->child_level_creation_mutex_);
    // if (!(!async::is_folly_thread || thread_local_var_->child_levels_.size() == 0)) {
    //     check(!async::is_folly_thread || thread_local_var_->child_levels_.size() == 0, "ABC");
    // }
    return instance;
}

void QueryStats::merge_levels() {
    for (auto& child_level : thread_local_var_->child_levels_) {
        child_level.parent_level_->merge_from(*child_level.root_level_);
    }
    thread_local_var_->child_levels_.clear();
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
    if (!thread_local_var_->root_level_) {
        util::check(!thread_local_var_->current_level_, "QueryStats current_level_ should be null if root_level_ is null");
        thread_local_var_->root_level_ = std::make_shared<GroupingLevel>();
        thread_local_var_->current_level_ = thread_local_var_->root_level_;
        std::lock_guard<std::mutex> lock(root_level_mutex_);
        root_levels_.emplace_back(thread_local_var_->root_level_);
        ARCTICDB_INFO(log::version(), "Root GroupingLevel created");
    }
    return thread_local_var_->root_level_;
}

const std::vector<std::shared_ptr<GroupingLevel>>& QueryStats::root_levels(async::TaskScheduler* const instance) const{
    check(!instance->tasks_pending(), "Folly tasks are still running");
    return root_levels_;
}

void QueryStats::create_child_level(std::shared_ptr<GroupingLevel> new_root_level) {
    auto child_level = std::make_shared<GroupingLevel>();
    // Since ExecutorWithStatsInstance::add will be called first, which will pass the root ThreadLocalQueryStatsVar to folly thread
    // folly thread's one neeeded to be reset here
    thread_local_var_ = std::make_shared<ThreadLocalQueryStatsVar>(); 
    thread_local_var_->current_level_ = new_root_level;
    thread_local_var_->root_level_ = new_root_level;
}

std::function<std::shared_ptr<GroupingLevel>()> QueryStats::get_create_childs_root_level_callback() {
    if (async::is_folly_thread) {
        check(create_childs_root_level_callback_.operator bool(), "create_childs_root_level_callback_ is empty");
        return create_childs_root_level_callback_;
    }
    else {
        return std::bind(create_childs_root_level, thread_local_var_, current_level());
    }
}

void QueryStats::set_create_childs_root_level_callback(std::function<std::shared_ptr<GroupingLevel>()> callback) {
    create_childs_root_level_callback_ = callback;
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

std::shared_ptr<GroupingLevel> create_childs_root_level(std::shared_ptr<ThreadLocalQueryStatsVar> thread_local_var, std::shared_ptr<GroupingLevel> parent_current_level){
    auto child_level = std::make_shared<GroupingLevel>();
    std::lock_guard<std::mutex> lock(thread_local_var->child_level_creation_mutex_);
    thread_local_var->child_levels_.emplace_back(std::move(parent_current_level), child_level);
    return child_level;
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

StatsGroup::StatsGroup(
            bool log_time, 
            GroupName col, 
            const std::string& default_value, 
            std::chrono::time_point<std::chrono::high_resolution_clock> start) : 
        prev_level_(QueryStats::instance().current_level()),
        start_(start),
        log_time_(log_time) {
    auto id = syscall(SYS_gettid);
    if (col == GroupName::key_type && default_value == "KeyType::TABLE_DATA") {
        ARCTICDB_INFO(log::version(), "StatsGroup::StatsGroup {} {} {}", syscall(SYS_gettid), id, default_value);
    }
    else {
        ARCTICDB_INFO(log::version(), "StatsGroup::StatsGroup {} {} {}", syscall(SYS_gettid), id, default_value);
    }
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
    if (!query_stats_instance.thread_local_var_->child_levels_.empty() && query_stats_instance.current_level() == query_stats_instance.root_level()) {
        ARCTICDB_INFO(log::version(), "merge_levels {}", syscall(SYS_gettid));
        query_stats_instance.merge_levels();
    }
}

void add_logical_keys(GroupName group_name, const entity::KeyType physical_key_type, const SegmentInMemory& segment){
    if (physical_key_type == entity::KeyType::TABLE_INDEX ||
        physical_key_type == entity::KeyType::VERSION_REF ||
        physical_key_type == entity::KeyType::VERSION ||
        physical_key_type == entity::KeyType::APPEND_REF ||
        physical_key_type == entity::KeyType::MULTI_KEY ||
        physical_key_type == entity::KeyType::SNAPSHOT_REF ||
        physical_key_type == entity::KeyType::SNAPSHOT ||
        physical_key_type == entity::KeyType::SNAPSHOT_TOMBSTONE ||
        physical_key_type == entity::KeyType::BLOCK_VERSION_REF) {
        for (size_t i = 0; i < segment.row_count(); ++i) {
            StatsGroup logical_key_group(false, group_name, format_group_value(GroupName::key_type, stream::read_key_row(segment, i)));
            QueryStats::instance().current_level()->stats_[static_cast<size_t>(StatsName::count)] += 1;
        }
    }
}
}