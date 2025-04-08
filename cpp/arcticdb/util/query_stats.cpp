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

void OpStats::reset_stats() {
    for (auto& logical_key_count : logical_key_counts_) {
        logical_key_count = 0;
    }
    result_count_ = 0;
    total_time_ms_ = 0;
    count_ = 0;
}


Stats::Stats() : 
    keys_stats_{} {

}

void Stats::reset_stats() {
    for (auto& op_stats : keys_stats_) {
        for (auto& op_stat : op_stats) {
            op_stat.reset_stats();
        }
    }
}

QueryStats QueryStats::instance_;

QueryStats& QueryStats::instance() {
    return instance_;
}

void QueryStats::reset_stats() {
    check(!async::TaskScheduler::instance()->tasks_pending(), "Folly tasks are still running");
    stats_.reset_stats();
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

const Stats& QueryStats::get_stats(async::TaskScheduler* const instance) const {
    check(!instance->tasks_pending(), "Folly tasks are still running");
    return stats_;
}

RAIIRunLambda::RAIIRunLambda(std::function<void(uint64_t)>&& lambda) :
    lambda_(std::move(lambda)),
    start_(std::chrono::steady_clock::now()) {

}

RAIIRunLambda::~RAIIRunLambda() {
    lambda_(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_).count());
}
}