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

#include <algorithm>
#include <exception>
#include <ranges>
#include <utility>
#include <vector>

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
    case TaskType::Memory_ListObjects:
        return "Memory_ListObjects";
    case TaskType::Memory_PutObject:
        return "Memory_PutObject";
    case TaskType::Memory_GetObject:
        return "Memory_GetObject";
    case TaskType::Memory_DeleteObject:
        return "Memory_DeleteObject";
    case TaskType::Memory_HeadObject:
        return "Memory_HeadObject";
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
    case StatType::FAILURE_TIME_MS:
        return "failure_time_ms";
    case StatType::FAILURE_COUNT:
        return "failure_count";
    case StatType::ATTEMPTS:
        return "attempts";
    case StatType::ATTEMPT_TIME_MS:
        return "attempt_time_ms";
    case StatType::NETWORK_ERRORS:
        return "network_errors";
    case StatType::SERVER_ERRORS:
        return "server_errors";
    case StatType::TTFB_MS:
        return "ttfb_ms";
    case StatType::CONNECT_MS:
        return "connect_ms";
    case StatType::NEW_CONNECTIONS:
        return "new_connections";
    default:
        log::version().warn("Unknown stat type {}", static_cast<int>(stat_type));
        return "unknown";
    }
}

namespace {
bool is_held_in_nanoseconds(StatType stat_type) {
    return stat_type == StatType::TOTAL_TIME_MS || stat_type == StatType::FAILURE_TIME_MS ||
           stat_type == StatType::ATTEMPT_TIME_MS;
}
} // namespace

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

            // Only operations that were called are added to the output
            if (op_stats[StatType::COUNT].readFull() == 0 && op_stats[StatType::FAILURE_COUNT].readFull() == 0) {
                continue;
            }
            for (size_t stat_idx = 0; stat_idx < static_cast<size_t>(StatType::END); ++stat_idx) {
                auto stat_type = static_cast<StatType>(stat_idx);
                uint64_t value = op_stats[stat_type].readFull();
                op_output[stat_type_to_string(stat_type)] = is_held_in_nanoseconds(stat_type) ? value / 1000000 : value;
            }

            result["storage_operations"][task_type_str][key_type_str] = std::move(op_output);
        }
    }

    return result;
}

void QueryStats::add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value) {
    if (is_enabled()) {
        internal::check<ErrorCode::E_INVALID_ARGUMENT>(stat_type < StatType::END, "Invalid stat type");
        stats_by_storage_op_type_[static_cast<size_t>(task_type)][static_cast<size_t>(key_type)][stat_type].increment(
                value
        );
    }
}

[[nodiscard]] RAIIAddTime QueryStats::add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start
) {
    if (is_enabled()) {
        return {stats_by_storage_op_type_[static_cast<size_t>(task_type)][static_cast<size_t>(key_type)],
                start.value_or(std::chrono::steady_clock::now())};
    }
    return {};
}

namespace {
// The stats of the calls being timed on this thread, which per-attempt stats are added to. Usually at most one, but a
// DeleteObjects batch is timed once for each key type in it.
thread_local std::vector<OperationStats*> current_calls;
} // namespace

RAIIAddTime::RAIIAddTime(OperationStats& stats, TimePoint start) :
    stats_(&stats),
    start_(start),
    uncaught_exceptions_(std::uncaught_exceptions()) {
    current_calls.push_back(stats_);
}

RAIIAddTime::RAIIAddTime(RAIIAddTime&& other) noexcept :
    stats_(std::exchange(other.stats_, nullptr)),
    start_(other.start_),
    uncaught_exceptions_(other.uncaught_exceptions_) {}

void RAIIAddTime::finish(bool succeeded) {
    if (stats_ == nullptr) {
        return;
    }
    if (auto it = std::ranges::find(current_calls | std::views::reverse, stats_); it != current_calls.rend()) {
        current_calls.erase(std::next(it).base());
    }
    const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count();
    (*stats_)[succeeded ? StatType::COUNT : StatType::FAILURE_COUNT].increment(1);
    (*stats_)[succeeded ? StatType::TOTAL_TIME_MS : StatType::FAILURE_TIME_MS].increment(elapsed);
    stats_ = nullptr;
}

RAIIAddTime::~RAIIAddTime() { finish(std::uncaught_exceptions() <= uncaught_exceptions_); }

void add_to_current_calls(StatType stat_type, uint64_t value) {
    for (auto* stats : current_calls) {
        (*stats)[stat_type].increment(value);
    }
}

void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value) {
    QueryStats::instance()->add(task_type, key_type, stat_type, value);
}

[[nodiscard]] RAIIAddTime add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start
) {
    return QueryStats::instance()->add_task_count_and_time(task_type, key_type, start);
}

} // namespace arcticdb::query_stats