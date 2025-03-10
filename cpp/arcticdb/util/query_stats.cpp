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
    if (!current_layer_) {
        util::check(!async::is_folly_thread, "Folly thread should have its StatsGroupLayer passed by caller only");
        util::check(!root_layer_, "QueryStats root_layer_ should be null if current_layer_ is null");
        root_layer_ = std::make_shared<StatsGroupLayer>();
        current_layer_ = root_layer_;
        ARCTICDB_DEBUG(log::version(), "Current StatsGroupLayer created");
    }
    return current_layer_;
}

void QueryStats::set_layer(std::shared_ptr<StatsGroupLayer> &layer) {
    current_layer_ = layer;
}

void QueryStats::reset_stats(){
    root_layer_.reset();
    child_layers_.clear();
    current_layer_.reset();
}

QueryStats& QueryStats::instance() {
    static QueryStats instance;
    return instance;
}

void QueryStats::merge_layers() {
    for (auto& [parent_layer, child_layer] : child_layers_) {
        parent_layer->merge_from(*child_layer);
    }
    child_layers_.clear();
}

std::shared_ptr<StatsGroupLayer> QueryStats::root_layer() {
    if (!root_layer_) {
        util::check(!current_layer_, "QueryStats current_layer_ should be null if root_layer_ is null");
        root_layer_ = std::make_shared<StatsGroupLayer>();
        current_layer_ = root_layer_;
        ARCTICDB_DEBUG(log::version(), "Root StatsGroupLayer created");
    }
    return root_layer_;
}

bool QueryStats::is_root_layer_set() {
    return root_layer_.operator bool();
}

void QueryStats::create_child_layer(std::shared_ptr<StatsGroupLayer>& layer) {
    auto child_layer = std::make_shared<StatsGroupLayer>();
    current_layer_ = child_layer;
    std::lock_guard<std::mutex> lock(child_layer_creation_mutex_);
    child_layers_.emplace_back(layer, child_layer);
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
}
}