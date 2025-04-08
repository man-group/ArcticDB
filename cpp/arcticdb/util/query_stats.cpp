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


OpStats::OpStats() : 
    logical_key_counts_{0},
    result_count_(0),
    total_time_ms_(0),
    count_(0) {

}

OpStats::OpStats(const OpStats& other) : 
    result_count_(other.result_count_.load(std::memory_order_relaxed)),
    total_time_ms_(other.total_time_ms_.load(std::memory_order_relaxed)),
    count_(other.count_.load(std::memory_order_relaxed)) {
    for (size_t i = 0; i < logical_key_counts_.size(); ++i) {
        logical_key_counts_[i] = other.logical_key_counts_[i].load(std::memory_order_relaxed);
    }
}

OpStats::OpStats(OpStats&& other) noexcept : 
    result_count_(other.result_count_.load(std::memory_order_relaxed)),
    total_time_ms_(other.total_time_ms_.load(std::memory_order_relaxed)),
    count_(other.count_.load(std::memory_order_relaxed)) {
    for (size_t i = 0; i < logical_key_counts_.size(); ++i) {
        logical_key_counts_[i] = other.logical_key_counts_[i].load(std::memory_order_relaxed);
    }
}

OpStats& OpStats::operator=(const OpStats& other) {
    if (this != &other) {
        for (size_t i = 0; i < logical_key_counts_.size(); ++i) {
            logical_key_counts_[i] = other.logical_key_counts_[i].load(std::memory_order_relaxed);
        }
        result_count_ = other.result_count_.load(std::memory_order_relaxed);
        total_time_ms_ = other.total_time_ms_.load(std::memory_order_relaxed);
        count_ = other.count_.load(std::memory_order_relaxed);
    }
    return *this;
}


CallStats::CallStats() : 
    keys_stats_{},
    total_time_ms_(0),
    count_(0) {

}

CallStats::CallStats(const CallStats& other) : 
    total_time_ms_(other.total_time_ms_.load(std::memory_order_relaxed)),
    count_(other.count_.load(std::memory_order_relaxed)) {
    for (size_t i = 0; i < NUMBER_OF_KEYS; ++i) {
        for (size_t j = 0; j < NUMBER_OF_TASK_TYPES; ++j) {
            keys_stats_[i][j] = other.keys_stats_[i][j];
        }
    }
}

CallStats::CallStats(CallStats&& other) noexcept : 
    total_time_ms_(other.total_time_ms_.load(std::memory_order_relaxed)),
    count_(other.count_.load(std::memory_order_relaxed)) {
    for (size_t i = 0; i < NUMBER_OF_KEYS; ++i) {
        for (size_t j = 0; j < NUMBER_OF_TASK_TYPES; ++j) {
            keys_stats_[i][j] = std::move(other.keys_stats_[i][j]);
        }
    }
}

QueryStats QueryStats::instance_;

QueryStats& QueryStats::instance() {
    return instance_;
}

void QueryStats::reset_stats() {
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    calls_stats_map_.clear();
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

void QueryStats::set_call(const std::string& call_name){
    std::lock_guard<std::mutex> lock(calls_stats_map_mutex_);
    if (auto it = calls_stats_map_.find(call_name); it != calls_stats_map_.end()) {
        call_stat_ptr_ = it->second;
    } 
    else {
        auto insert_result = calls_stats_map_.emplace(call_name, std::make_shared<CallStats>());
        call_stat_ptr_ = insert_result.first->second;
    }
}

std::shared_ptr<CallStats> QueryStats::get_call_stats(){
    check(call_stat_ptr_.operator bool(), "Call stat pointer is null");
    return call_stat_ptr_;
}

void QueryStats::set_call_stats(std::shared_ptr<CallStats>&& call_stats) {
    call_stat_ptr_ = std::move(call_stats);
}

ankerl::unordered_dense::map<std::string, std::shared_ptr<CallStats>> QueryStats::get_calls_stats_map(async::TaskScheduler* const instance) {
    check(!instance->tasks_pending(), "Folly tasks are still running");
    std::lock_guard<std::mutex> lock(calls_stats_map_mutex_);
    return calls_stats_map_;
}

RAIIRunLambda::RAIIRunLambda(std::function<void(uint64_t)> lambda) :
    lambda_(std::move(lambda)),
    start_(std::chrono::steady_clock::now()) {

}

RAIIRunLambda::~RAIIRunLambda() {
    lambda_(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_).count());
}
}