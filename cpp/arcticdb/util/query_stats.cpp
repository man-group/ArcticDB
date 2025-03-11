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

std::shared_ptr<StatsGroupLayer> QueryStats::current_layer(){
    // current_layer_ != nullptr && root_layer_ != nullptr -> stats has been setup; Nothing to do
    // current_layer_ == nullptr && root_layer_ == nullptr -> clean slate; Need to setup
    // current_layer_ != nullptr && root_layer_ == nullptr -> stats have been reset; Need to setup
    // current_layer_ == nullptr && root_layer_ != nullptr -> Something is off
    if (!thread_local_var_.current_layer_ || !thread_local_var_.root_layer_) {
        check(!async::is_folly_thread, "Folly thread should have its StatsGroupLayer passed by caller only");
        check(thread_local_var_.current_layer_ || !thread_local_var_.root_layer_, "QueryStats root_layer_ should be null if current_layer_ is null");
        thread_local_var_.root_layer_ = std::make_shared<StatsGroupLayer>();
        {
            std::lock_guard<std::mutex> lock(root_layer_mutex_);
            root_layers_.emplace_back(thread_local_var_.root_layer_);
        }
        thread_local_var_.current_layer_ = thread_local_var_.root_layer_;
        ARCTICDB_DEBUG(log::version(), "Current StatsGroupLayer created");
    }
    return thread_local_var_.current_layer_;
}

void QueryStats::set_layer(std::shared_ptr<StatsGroupLayer> &layer) {
    thread_local_var_.current_layer_ = layer;
}

void QueryStats::set_root_layer(std::shared_ptr<StatsGroupLayer> &layer) {
    thread_local_var_.current_layer_ = layer;
    thread_local_var_.root_layer_ = layer;
}

void QueryStats::reset_stats() {
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    std::lock_guard<std::mutex> lock(root_layer_mutex_);
    for (auto& layer : root_layers_) {
        layer.reset();
    }
}

QueryStats& QueryStats::instance() {
    static QueryStats instance;
    return instance;
}

void QueryStats::merge_layers() {
    for (auto& [parent_layer, child_layer] : thread_local_var_.child_layers_) {
        parent_layer->merge_from(*child_layer);
    }
    thread_local_var_.child_layers_.clear();
}

void StatsGroupLayer::reset_stats() {
    stats_.fill(0);
    for (auto& next_layer_map : next_layer_maps_) {
        next_layer_map.clear();
    }
}

void StatsGroupLayer::merge_from(const StatsGroupLayer& other) {
    for (size_t i = 0; i < stats_.size(); ++i) {
        stats_[i] += other.stats_[i];
    }
    
    for (size_t i = 0; i < next_layer_maps_.size(); ++i) {
        for (const auto& [key, other_layer] : other.next_layer_maps_[i]) {
            auto& next_layer_map = next_layer_maps_[i];
            auto it = next_layer_map.find(key);
            if (it == next_layer_map.end()) {
                it = next_layer_map.emplace(key, std::make_shared<StatsGroupLayer>()).first;
            }
            it->second->merge_from(*other_layer);
        }
    }
}

std::shared_ptr<StatsGroupLayer> QueryStats::root_layer() {
    if (!thread_local_var_.root_layer_) {
        util::check(!thread_local_var_.current_layer_, "QueryStats current_layer_ should be null if root_layer_ is null");
        thread_local_var_.root_layer_ = std::make_shared<StatsGroupLayer>();
        thread_local_var_.current_layer_ = thread_local_var_.root_layer_;
        ARCTICDB_DEBUG(log::version(), "Root StatsGroupLayer created");
    }
    return thread_local_var_.root_layer_;
}

const std::vector<std::shared_ptr<StatsGroupLayer>>& QueryStats::root_layers() const{
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    return root_layers_;
}

bool QueryStats::is_root_layer_set() {
    return thread_local_var_.root_layer_.operator bool();
}

void QueryStats::create_child_layer(ThreadLocalQueryStatsVar& parent_thread_local_var) {
    auto child_layer = std::make_shared<StatsGroupLayer>();
    set_root_layer(child_layer);
    std::lock_guard<std::mutex> lock(parent_thread_local_var.child_layer_creation_mutex_);
    parent_thread_local_var.child_layers_.emplace_back(parent_thread_local_var.current_layer_, child_layer);
}

StatsGroup::StatsGroup(bool log_time, StatsGroupName col, const std::string& value) : 
        prev_layer_(QueryStats::instance().current_layer()),
        start_(std::chrono::high_resolution_clock::now()),
        log_time_(log_time) {
        auto& next_layer_map = prev_layer_->next_layer_maps_[static_cast<size_t>(col)];
    std::shared_ptr<StatsGroupLayer> cur_layer_ = nullptr;
    if (next_layer_map.find(value) == next_layer_map.end()) {
        next_layer_map[value] = std::make_shared<StatsGroupLayer>();
    }
    QueryStats::instance().set_layer(next_layer_map[value]);
}

StatsGroup::~StatsGroup() {
    auto& query_stats_instance = QueryStats::instance();
    if (log_time_) {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_);
        auto& stats = query_stats_instance.current_layer()->stats_;
        stats[static_cast<size_t>(StatsName::total_time_ms)] += duration.count();
        stats[static_cast<size_t>(StatsName::count)]++;
    }
    query_stats_instance.set_layer(prev_layer_);
    if (!async::is_folly_thread && query_stats_instance.current_layer() == query_stats_instance.root_layer()) {
        query_stats_instance.merge_layers();
    }
    auto& root_layers = QueryStats::instance().root_layers();
    
    ARCTICDB_INFO(log::version(), "root_layers {}", root_layers.size());
}
}