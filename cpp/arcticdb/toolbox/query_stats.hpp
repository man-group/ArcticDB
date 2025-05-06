/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <type_traits>
#include <string>
#include <chrono>
#include <array>
#include <memory>
#include <folly/ThreadCachedInt.h>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb::query_stats{
enum class TaskType : size_t {
    S3_ListObjectsV2 = 0,
    S3_PutObject = 1,
    S3_GetObject = 2,
    S3_GetObjectAsync = 3,
    S3_DeleteObjects = 4,
    S3_HeadObject = 5,
    Encode = 6,
    Decode = 7,
    DecodeMetadata = 8,
    DecodeTimeseriesDescriptor = 9,
    DecodeMetadataAndDescriptor = 10,
    END
};

enum class StatType : size_t {
    TOTAL_TIME_MS = 0,
    COUNT = 1,
    UNCOMPRESSED_SIZE_BYTES = 2,
    COMPRESSED_SIZE_BYTES = 3,
    END
};

class RAIIAddTime {
public:
    RAIIAddTime(folly::ThreadCachedInt<timestamp>& time_var, std::chrono::time_point<std::chrono::steady_clock> start);
    ~RAIIAddTime();
private:
    folly::ThreadCachedInt<timestamp>& time_var_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
};

// Example output:
// {
//     "VERSION_REF": { <- STATS_BY_KEY_TYPE
//         "storage_ops": { <- STATS_BY_OP_TYPE
//             "S3_ListObjectsV2": { <- OperationStats::stats_
//                 "total_time_ms": 32,
//                 "count": 2
//             },
//             "S3_GetObject": { <- OperationStats::stats_
//                 "total_time_ms": 50,
//                 "count": 3
//             },
//             "Decode": { <- OperationStats::stats_
//                 "count": 3,
//                 "uncompressed_size_bytes": 300,
//                 "compressed_size_bytes": 1827,
//                 "key_type": { <- OperationStats::logical_key_counts_
//                     "TABLE_INDEX": {
//                         "count": 3
//                     },
//                     "VERSION": {
//                         "count": 3
//                     }
//                 }
//             }
//         }
//     }
// }



class QueryStats {
public:
    struct OperationStats{
        std::array<folly::ThreadCachedInt<uint32_t>, static_cast<size_t>(entity::KeyType::UNDEFINED)> logical_key_counts_;
        folly::ThreadCachedInt<timestamp> total_time_ns_;
        folly::ThreadCachedInt<uint32_t> count_;
        folly::ThreadCachedInt<uint32_t> uncompressed_size_bytes_;
        folly::ThreadCachedInt<uint32_t> compressed_size_bytes_;
        void reset_stats(){
            total_time_ns_.set(0);
            count_.set(0);
            uncompressed_size_bytes_.set(0);
            compressed_size_bytes_.set(0);
            for (auto& logical_key_count : logical_key_counts_) {
                logical_key_count.set(0);
            }
        }
        OperationStats(){
            reset_stats(); 
        }
    };
    struct OperationStatsOutput {
        std::map<std::string, uint32_t> stats_;
        std::map<std::string, std::map<std::string, uint32_t>> key_type_;
    };
    using QueryStatsOutput = std::map<std::string, std::map<std::string, std::map<std::string, OperationStatsOutput>>>;
    using STATS_BY_OP_TYPE = std::array<OperationStats, static_cast<size_t>(TaskType::END)>;
    using STATS_BY_KEY_TYPE = std::array<STATS_BY_OP_TYPE, static_cast<size_t>(entity::KeyType::UNDEFINED)>;

    ARCTICDB_NO_MOVE_OR_COPY(QueryStats);
    void reset_stats();
    static std::shared_ptr<QueryStats> instance();
    void enable();
    void disable();
    bool is_enabled() const;
    void add(entity::KeyType key_type, TaskType task_type, StatType stat_type, uint32_t value);
    void add_logical_keys(entity::KeyType physical_key_type, TaskType task_type, const SegmentInMemory& segment);
    [[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(entity::KeyType key_type, TaskType task_type, std::optional<std::chrono::time_point<std::chrono::steady_clock>> start = std::nullopt);
    QueryStatsOutput get_stats() const;
    QueryStats();

private:
    static std::once_flag init_flag_;
    static std::shared_ptr<QueryStats> instance_;
    std::atomic<bool> is_enabled_ = false;

    STATS_BY_KEY_TYPE stats_by_key_type_;
};

void add(entity::KeyType key_type, TaskType task_type, StatType stat_type, uint32_t value);
void add_logical_keys(entity::KeyType physical_key_type, TaskType task_type, const SegmentInMemory& segment);
[[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(entity::KeyType key_type, TaskType task_type, std::optional<std::chrono::time_point<std::chrono::steady_clock>> start = std::nullopt);
}
