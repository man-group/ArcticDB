/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

namespace arcticdb::query_stats {

std::shared_ptr<QueryStats> QueryStats::instance_;
std::once_flag QueryStats::init_flag_;

std::shared_ptr<QueryStats> QueryStats::instance() {
    std::call_once(init_flag_, []() { instance_ = std::make_shared<QueryStats>(); });
    return instance_;
}

QueryStats::QueryStats() { reset_stats(); }

void QueryStats::reset_stats() {
    for (auto& key_stats : stats_by_storage_op_type_) {
        for (auto& op_stat : key_stats) {
            op_stat.reset_stats();
        }
    }
}

void QueryStats::enable() { is_enabled_ = true; }

void QueryStats::disable() { is_enabled_ = false; }

bool QueryStats::is_enabled() const { return is_enabled_; }

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
    auto token_pos = key_type_str.find(token); // KeyType::SYMBOL_LIST -> SYMBOL_LIST
    return token_pos == std::string::npos ? key_type_str : key_type_str.substr(token_pos + token.size());
}

QueryStats::QueryStatsOutput QueryStats::get_stats() const {
    QueryStatsOutput result;

    for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
        auto task_type = static_cast<TaskType>(task_idx);
        std::string task_type_str = task_type_to_string(task_type);

        for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
            const auto& op_stats = stats_by_storage_op_type_[task_idx][key_idx];
            std::string key_type_str = get_key_type_str(static_cast<entity::KeyType>(key_idx));
            OperationStatsOutput op_output;

            bool has_non_zero_stats = op_stats.count_.readFull();
            for (size_t stat_idx = 0; stat_idx < static_cast<size_t>(StatType::END); ++stat_idx) {
                auto stat_type = static_cast<StatType>(stat_idx);
                uint64_t value = 0;
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
                result["storage_operations"][task_type_str][key_type_str] = std::move(op_output);
            }
        }
    }

    return result;
}

void QueryStats::add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value) {
    if (is_enabled()) {
        auto& stats = stats_by_storage_op_type_[static_cast<size_t>(task_type)][static_cast<size_t>(key_type)];
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
            internal::raise<ErrorCode::E_INVALID_ARGUMENT>("Invalid stat type");
        }
    }
}

[[nodiscard]] std::optional<RAIIAddTime> QueryStats::add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start
) {
    if (is_enabled()) {
        auto& stats = stats_by_storage_op_type_[static_cast<size_t>(task_type)][static_cast<size_t>(key_type)];
        stats.count_.increment(1);
        return std::make_optional<RAIIAddTime>(stats.total_time_ns_, start.value_or(std::chrono::steady_clock::now()));
    }
    return std::nullopt;
}

RAIIAddTime::RAIIAddTime(folly::ThreadCachedInt<timestamp>& time_var, TimePoint start) :
    time_var_(time_var),
    start_(start) {}

RAIIAddTime::~RAIIAddTime() {
    time_var_.increment(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count()
    );
}

void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value) {
    QueryStats::instance()->add(task_type, key_type, stat_type, value);
}

[[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start
) {
    return QueryStats::instance()->add_task_count_and_time(task_type, key_type, start);
}

} // namespace arcticdb::query_stats