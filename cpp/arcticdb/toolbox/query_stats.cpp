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
    case TaskType::S3_PutObject:
        return "S3_PutObject";
    case TaskType::S3_GetObject:
        return "S3_GetObject";
    case TaskType::S3_GetObjectAsync:
        return "S3_GetObjectAsync";
    case TaskType::S3_DeleteObjects:
        return "S3_DeleteObjects";
    case TaskType::S3_HeadObject:
        return "S3_HeadObject";
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
        case StatType::SIZE_BYTES:
            return "size_bytes";
        default:
            log::version().warn("Unknown stat type {}", static_cast<int>(stat_type));
            return "unknown";
    }
}

std::string get_key_type_str(entity::KeyType key) {
    const std::string token = "::";
    std::string key_type_str = entity::get_key_description(key);
    auto token_pos = key_type_str.find(token); //KeyType::SYMBOL_LIST -> SYMBOL_LIST
    return token_pos == std::string::npos ? key_type_str : key_type_str.substr(token_pos + token.size());
}

QueryStats::QueryStatsOutput QueryStats::get_stats() const {
    QueryStatsOutput result;
    
    for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
        entity::KeyType key_type = static_cast<entity::KeyType>(key_idx);
        
        for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
            TaskType task_type = static_cast<TaskType>(task_idx);
            std::string task_type_str = task_type_to_string(task_type);

            const auto& op_stats = stats_by_key_type_[key_idx][task_idx];

            OperationStatsOutput op_output;
            
            bool has_non_zero_stats = op_stats.count_.readFull();
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
                    case StatType::SIZE_BYTES:
                        value = op_stats.size_bytes_.readFull();
                        break;
                    default:
                        continue;
                }
                if (has_non_zero_stats) {
                    std::string stat_name = stat_type_to_string(stat_type);
                    op_output[stat_name] = value;
                }
            }
            
            // Only non-zero stats will be added to the output
            if (!op_output.empty()) {
                std::string key_type_str = get_key_type_str(key_type);
                result[key_type_str]["storage_ops"][task_type_str] = std::move(op_output);
            }
        }
    }
    
    return result;
}

void QueryStats::add(entity::KeyType key_type, TaskType task_type, StatType stat_type, uint32_t value) {
    if (is_enabled()) {
        auto& stats = stats_by_key_type_[static_cast<size_t>(key_type)][static_cast<size_t>(task_type)];
        switch (stat_type) {
            case StatType::TOTAL_TIME_MS:
                stats.total_time_ns_.increment(value);
                break;
            case StatType::COUNT:
                stats.count_.increment(value);
                break;
            case StatType::SIZE_BYTES:
                stats.size_bytes_.increment(value);
                break;
            default:
                throw std::invalid_argument("Invalid stat type");
        }
    }
}

[[nodiscard]] std::optional<RAIIAddTime> QueryStats::add_task_count_and_time(
        entity::KeyType key_type, TaskType task_type, std::optional<std::chrono::time_point<std::chrono::steady_clock>> start
) {
    if (is_enabled()) {
        auto& stats = stats_by_key_type_[static_cast<size_t>(key_type)][static_cast<size_t>(task_type)];
        stats.count_.increment(1);
        return std::make_optional<RAIIAddTime>(stats.total_time_ns_, start.value_or(std::chrono::steady_clock::now()));
    }
    return std::nullopt;
}

RAIIAddTime::RAIIAddTime(folly::ThreadCachedInt<timestamp>& time_var, std::chrono::time_point<std::chrono::steady_clock> start) :
    time_var_(time_var),
    start_(start) {

}

RAIIAddTime::~RAIIAddTime() {
    time_var_.increment(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count());
}

void add(entity::KeyType key_type, TaskType task_type, StatType stat_type, uint32_t value) {
    QueryStats::instance()->add(key_type, task_type, stat_type, value);
}

[[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(
    entity::KeyType key_type, TaskType task_type, std::optional<std::chrono::time_point<std::chrono::steady_clock>> start
) {
    return QueryStats::instance()->add_task_count_and_time(key_type, task_type, start);
}

}