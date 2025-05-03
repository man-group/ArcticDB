/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb::query_stats {

std::shared_ptr<QueryStats> QueryStats::instance_;
std::once_flag QueryStats::init_flag_;

std::shared_ptr<QueryStats> QueryStats::instance() {
    std::call_once(init_flag_, [] () {
        instance_ = std::make_shared<QueryStats>();
    });
    return instance_;
}

QueryStats::QueryStats(){
    reset_stats();
}

void QueryStats::reset_stats() {
    for (auto& op_stats : stats_by_key_type_) {
        for (auto& op_stat : op_stats) {
            op_stat.reset_stats();
        }
    }
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


std::string task_type_to_string(TaskType task_type) {
    switch (task_type) {
    case TaskType::S3_ListObjectsV2:
        return "S3_ListObjectsV2";
    default:
        log::version().warn("Unknown task type {}", static_cast<int>(task_type));
        return "Unknown";
    }
}

std::string stat_type_to_string(StatType stat_type) {
    switch (stat_type) {
        case StatType::TOTAL_TIME_MS:
            return "total_time_ms";
        case StatType::COUNT:
            return "count";
        default:
            log::version().warn("Unknown stat type {}", static_cast<int>(stat_type));
            return "unknown";
    }
}

QueryStats::QueryStatsOutput QueryStats::get_stats() const {
    QueryStatsOutput result;
    
    for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
        entity::KeyType key_type = static_cast<entity::KeyType>(key_idx);
        std::string key_type_str = entity::get_key_description(key_type);
        std::string token = "::";
        auto token_pos = key_type_str.find(token); //KeyType::SYMBOL_LIST -> SYMBOL_LIST
        key_type_str = token_pos == std::string::npos ? key_type_str : key_type_str.substr(token_pos + token.size());
        
        for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
            TaskType task_type = static_cast<TaskType>(task_idx);
            std::string task_type_str = task_type_to_string(task_type);
            
            const auto& op_stats = stats_by_key_type_[key_idx][task_idx];
            
            OperationStatsOutput op_output;
            
            for (size_t stat_idx = 0; stat_idx < static_cast<size_t>(StatType::END); ++stat_idx) {
                StatType stat_type = static_cast<StatType>(stat_idx);
                uint32_t value = 0;
                switch (stat_type) {
                    case StatType::TOTAL_TIME_MS:
                        value = op_stats.total_time_ns_.readFull() / 1e6;
                        break;
                    case StatType::COUNT:
                        value = op_stats.count_.readFull();
                        break;
                    default:
                        continue;
                }
                if (value > 0) {
                    std::string stat_name = stat_type_to_string(stat_type);
                    op_output.stats_[stat_name] = value;
                }
            }
            
            // Only non-zero stats will be added to the output
            if (!op_output.stats_.empty()) {
                result[key_type_str]["storage_ops"][task_type_str] = std::move(op_output);
            }
        }
    }
    
    return result;
}

void QueryStats::add(entity::KeyType key_type, TaskType task_type, uint32_t value) {
    if (is_enabled()) {
        stats_by_key_type_[static_cast<size_t>(key_type)][static_cast<size_t>(task_type)].count_.increment(value);
    }
}

[[nodiscard]] std::optional<RAIIAddTime> QueryStats::add_task_count_and_time(entity::KeyType key_type, TaskType task_type) {
    if (is_enabled()) {
        auto& stats = stats_by_key_type_[static_cast<size_t>(key_type)][static_cast<size_t>(task_type)];
        stats.count_.increment(1);
        return std::make_optional<RAIIAddTime>(stats.total_time_ns_);
    }
    return std::nullopt;
}

RAIIAddTime::RAIIAddTime(folly::ThreadCachedInt<timestamp>& time_var) :
    time_var_(time_var),
    start_(std::chrono::steady_clock::now()) {

}

RAIIAddTime::~RAIIAddTime() {
    time_var_.increment(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count());
}

void add(entity::KeyType key_type, TaskType task_type, uint32_t value) {
    QueryStats::instance()->add(key_type, task_type, value);
}

[[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(entity::KeyType key_type, TaskType task_type) {
    return QueryStats::instance()->add_task_count_and_time(key_type, task_type);
}

}