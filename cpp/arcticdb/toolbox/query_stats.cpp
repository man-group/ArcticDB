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

QueryStats::LatenciesOutput QueryStats::get_latencies() const {
    LatenciesOutput result;
    for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
        std::vector<double> combined;
        for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
            auto lats = *stats_by_storage_op_type_[task_idx][key_idx].latencies_ms_.rlock();
            combined.insert(combined.end(), lats.begin(), lats.end());
        }
        if (!combined.empty()) {
            result[task_type_to_string(static_cast<TaskType>(task_idx))] = std::move(combined);
        }
    }
    return result;
}

QueryStats::SizesOutput QueryStats::get_sizes() const {
    SizesOutput result;
    for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
        std::vector<uint64_t> combined;
        for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
            auto sizes = *stats_by_storage_op_type_[task_idx][key_idx].sizes_bytes_.rlock();
            combined.insert(combined.end(), sizes.begin(), sizes.end());
        }
        if (!combined.empty()) {
            result[task_type_to_string(static_cast<TaskType>(task_idx))] = std::move(combined);
        }
    }
    return result;
}

QueryStats::DetailedLatenciesOutput QueryStats::get_detailed_latencies() const {
    DetailedLatenciesOutput result;
    for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
        auto task_name = task_type_to_string(static_cast<TaskType>(task_idx));
        for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
            auto lats = *stats_by_storage_op_type_[task_idx][key_idx].latencies_ms_.rlock();
            if (!lats.empty()) {
                auto key_name = get_key_type_str(static_cast<entity::KeyType>(key_idx));
                result[task_name][key_name] = std::move(lats);
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
            stats.record_size(value);
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
        auto* lat_ptr = &stats.latencies_ms_;
        util::check(lat_ptr != nullptr, "Latencies pointer should not be null");
        return RAIIAddTime(
            stats.total_time_ns_,
            start.value_or(std::chrono::steady_clock::now()),
            lat_ptr);
    }
    return std::nullopt;
}

RAIIAddTime::RAIIAddTime(folly::ThreadCachedInt<timestamp>& time_var, TimePoint start,
                         folly::Synchronized<std::vector<double>>* latencies) :
    time_var_(time_var),
    start_(start),
    latencies_(latencies),
    active_(true) {}

RAIIAddTime::RAIIAddTime(RAIIAddTime&& other) noexcept :
    time_var_(other.time_var_),
    start_(other.start_),
    latencies_(other.latencies_),
    active_(other.active_) {
    other.active_ = false;
}

RAIIAddTime::~RAIIAddTime() {
    if (!active_) return;
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start_).count();
    time_var_.increment(ns);
    if (latencies_ != nullptr) {
        latencies_->wlock()->push_back(static_cast<double>(ns) / 1e6);
    }
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