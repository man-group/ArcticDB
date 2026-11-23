/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/util/constants.hpp>

#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace arcticdb {
class PrometheusInstance;
} // namespace arcticdb

namespace arcticdb::query_stats {

// ---------------------------------------------------------------------------
// Core enums
// ---------------------------------------------------------------------------

enum class StorageBackend : uint8_t {
    S3 = 0,
    GcpXml = 1,
    NfsBacked = 2,
    Azure = 3,
    Lmdb = 4,
    Mongo = 5,
    Memory = 6,
    MappedFile = 7,
    Custom = 8,
    COUNT
};

enum class StorageOp : uint8_t {
    Read = 0,
    AsyncRead = 1,
    Write = 2,
    WriteIfNone = 3,
    Update = 4,
    Delete = 5,
    DeleteBatch = 6,
    Exists = 7,
    List = 8,
    Scan = 9,
    VisitSizes = 10,
    FastDelete = 11,
    VisitMetadata = 12,
    WriteIfMatch = 13,
    COUNT
};

enum class StorageOutcome : uint8_t { Ok = 0, NotFound = 1, Conflict = 2, Unsupported = 3, Error = 4, COUNT };

enum class HttpStatusClass : uint8_t {
    Status2xx = 0,
    Status3xx = 1,
    Status4xx = 2,
    Status5xx = 3,
    NetworkError = 4,
    COUNT
};

enum class PoolKind : uint8_t { Cpu = 0, Io = 1, COUNT };

enum class LockKind : uint8_t { StorageLock = 0, ReliableStorageLock = 1, COUNT };

enum class ApiOp : uint8_t {
    Read = 0,
    Write = 1,
    Append = 2,
    Update = 3,
    Delete = 4,
    ReadBatch = 5,
    WriteBatch = 6,
    AppendBatch = 7,
    UpdateBatch = 8,
    DeleteBatch = 9,
    ListSymbols = 10,
    ListVersions = 11,
    Snapshot = 12,
    PrunePreviousVersions = 13,
    Merge = 14,
    Metadata = 15,
    Compact = 16,
    COUNT
};

enum class BoundaryPhase : uint8_t { Normalize = 0, Denormalize = 1, CppInput = 2, CppOutput = 3, GilWait = 4, COUNT };

enum class ClauseKind : uint8_t {
    Passthrough = 0,
    Filter = 1,
    Project = 2,
    GroupByPartition = 3,
    Aggregation = 4,
    Resample = 5,
    RemoveColumnPartitioning = 6,
    Split = 7,
    Sort = 8,
    Merge = 9,
    ColumnStats = 10,
    RowRange = 11,
    DateRange = 12,
    Concat = 13,
    Write = 14,
    MergeUpdate = 15,
    CompactData = 16,
    RenameColumns = 17,
    Other = 18,
    COUNT
};

enum class CompactionKind : uint8_t { Incomplete = 0, SortMerge = 1, Defragment = 2, SymbolList = 3, COUNT };

enum class WriteConflictKind : uint8_t {
    NonIncreasingVersion = 0,
    ConcurrentSymbolListCompaction = 1,
    SymbolListLoadRetry = 2,
    COUNT
};

// Legacy enums preserved for backward compatibility with existing C++ callers and tests.
enum class TaskType : size_t {
    S3_ListObjectsV2 = 0,
    S3_PutObject = 1,
    S3_GetObject = 2,
    S3_GetObjectAsync = 3,
    S3_DeleteObjects = 4,
    S3_HeadObject = 5,
    Memory_ListObjects = 6,
    Memory_PutObject = 7,
    Memory_GetObject = 8,
    Memory_DeleteObject = 9,
    Memory_HeadObject = 10,
    END
};

enum class StatType : size_t { TOTAL_TIME_MS = 0, COUNT = 1, SIZE_BYTES = 2, END };

using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

[[nodiscard]] const char* to_string(StorageBackend backend) noexcept;
[[nodiscard]] const char* to_string(StorageOp op) noexcept;
[[nodiscard]] const char* to_string(StorageOutcome outcome) noexcept;
[[nodiscard]] const char* to_string(HttpStatusClass status_class) noexcept;
[[nodiscard]] const char* to_string(PoolKind pool) noexcept;
[[nodiscard]] const char* to_string(LockKind lock) noexcept;
[[nodiscard]] const char* to_string(ApiOp op) noexcept;
[[nodiscard]] const char* to_string(BoundaryPhase phase) noexcept;
[[nodiscard]] const char* to_string(ClauseKind kind) noexcept;
[[nodiscard]] const char* to_string(CompactionKind kind) noexcept;
[[nodiscard]] const char* to_string(WriteConflictKind kind) noexcept;
[[nodiscard]] std::string key_type_name(entity::KeyType key_type);

// Returns true when VariantKey::id() represents a user symbol name rather than an
// action id, snapshot name, lock name, library name, or internal identifier.
[[nodiscard]] constexpr bool is_symbol_scoped_key_type(entity::KeyType key_type) noexcept {
    switch (key_type) {
    case entity::KeyType::TABLE_DATA:
    case entity::KeyType::TABLE_INDEX:
    case entity::KeyType::VERSION:
    case entity::KeyType::VERSION_JOURNAL:
    case entity::KeyType::VERSION_REF:
    case entity::KeyType::APPEND_REF:
    case entity::KeyType::APPEND_DATA:
    case entity::KeyType::MULTI_KEY:
    case entity::KeyType::TOMBSTONE:
    case entity::KeyType::TOMBSTONE_ALL:
    case entity::KeyType::LOG:
    case entity::KeyType::PARTITION:
    case entity::KeyType::OFFSET:
    case entity::KeyType::COLUMN_STATS:
        return true;
    default:
        return false;
    }
}

// ---------------------------------------------------------------------------
// Fast-path active check (single relaxed atomic load when disabled)
// ---------------------------------------------------------------------------

namespace detail {
extern std::atomic<uint32_t> active_collector_count;
extern std::atomic<int64_t> write_semaphore_in_flight;
extern std::atomic<uint64_t> write_semaphore_limit;
extern std::atomic<int64_t> version_map_entries;
extern std::atomic<int64_t> config_cache_libraries;
extern std::atomic<int64_t> config_cache_storages;

inline void adjust_write_semaphore_in_flight(int64_t delta) noexcept {
    write_semaphore_in_flight.fetch_add(delta, std::memory_order_relaxed);
}
inline void set_write_semaphore_limit(uint64_t limit) noexcept {
    write_semaphore_limit.store(limit, std::memory_order_relaxed);
}
inline void adjust_version_map_entries(int64_t delta) noexcept {
    version_map_entries.fetch_add(delta, std::memory_order_relaxed);
}
inline void adjust_config_cache_counts(int64_t lib_delta, int64_t storage_delta) noexcept {
    if (lib_delta != 0) {
        config_cache_libraries.fetch_add(lib_delta, std::memory_order_relaxed);
    }
    if (storage_delta != 0) {
        config_cache_storages.fetch_add(storage_delta, std::memory_order_relaxed);
    }
}
} // namespace detail

[[nodiscard]] inline bool is_active() noexcept {
    return detail::active_collector_count.load(std::memory_order_relaxed) != 0;
}

// ---------------------------------------------------------------------------
// Compact zero-allocation log-linear latency histogram (nanosecond precision)
// ---------------------------------------------------------------------------
// Bucket 0:        [0, 1024) ns
// Buckets 1..112:  28 binary octaves [2^(e+10), 2^(e+11)) ns for e in 0..27
//                  (1.024 us .. 274.88 s), 4 linear sub-buckets per octave
// Bucket 113:      >= 2^38 ns (~274.88 s)

struct LatencyHistogram {
    static constexpr size_t NUM_OCTAVES = 28;
    static constexpr size_t SUB_BUCKETS = 4;
    static constexpr size_t OVERFLOW_BUCKET = 1 + NUM_OCTAVES * SUB_BUCKETS; // 113
    static constexpr size_t NUM_BUCKETS = OVERFLOW_BUCKET + 1;               // 114

    uint64_t count{0};
    uint64_t sum_ns{0};
    uint64_t min_ns{std::numeric_limits<uint64_t>::max()};
    uint64_t max_ns{0};
    std::array<uint64_t, NUM_BUCKETS> buckets{};

    static constexpr size_t bucket_for(uint64_t ns) noexcept {
        if (ns < 1024ULL) {
            return 0;
        }
        const unsigned msb = 63U - static_cast<unsigned>(std::countl_zero(ns));
        const unsigned octave = msb - 10U;
        if (octave >= NUM_OCTAVES) {
            return OVERFLOW_BUCKET;
        }
        const size_t sub = static_cast<size_t>((ns >> (octave + 8U)) & 3ULL);
        return 1U + static_cast<size_t>(octave) * SUB_BUCKETS + sub;
    }

    static constexpr std::pair<uint64_t, uint64_t> bucket_bounds(size_t idx) noexcept {
        if (idx == 0) {
            return {0ULL, 1024ULL};
        }
        if (idx >= OVERFLOW_BUCKET) {
            return {1ULL << 38U, std::numeric_limits<uint64_t>::max()};
        }
        const size_t zero_based = idx - 1U;
        const unsigned octave = static_cast<unsigned>(zero_based / SUB_BUCKETS);
        const uint64_t sub = static_cast<uint64_t>(zero_based % SUB_BUCKETS);
        const uint64_t lower = (4ULL + sub) << (octave + 8U);
        const uint64_t upper = (5ULL + sub) << (octave + 8U);
        return {lower, upper};
    }

    void record(uint64_t ns) noexcept {
        ++count;
        sum_ns += ns;
        if (ns < min_ns) {
            min_ns = ns;
        }
        if (ns > max_ns) {
            max_ns = ns;
        }
        ++buckets[bucket_for(ns)];
    }

    void merge(const LatencyHistogram& other) noexcept {
        if (other.count == 0) {
            return;
        }
        count += other.count;
        sum_ns += other.sum_ns;
        if (other.min_ns < min_ns) {
            min_ns = other.min_ns;
        }
        if (other.max_ns > max_ns) {
            max_ns = other.max_ns;
        }
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            buckets[i] += other.buckets[i];
        }
    }

    void reset() noexcept {
        count = 0;
        sum_ns = 0;
        min_ns = std::numeric_limits<uint64_t>::max();
        max_ns = 0;
        buckets.fill(0);
    }

    [[nodiscard]] bool empty() const noexcept { return count == 0; }
    [[nodiscard]] double mean_ns() const noexcept { return count == 0 ? 0.0 : static_cast<double>(sum_ns) / count; }
    [[nodiscard]] uint64_t percentile_ns(double p) const noexcept;

    [[nodiscard]] double total_ms() const noexcept { return static_cast<double>(sum_ns) / 1e6; }
    [[nodiscard]] double mean_ms() const noexcept { return mean_ns() / 1e6; }
    [[nodiscard]] double min_ms() const noexcept { return count == 0 ? 0.0 : static_cast<double>(min_ns) / 1e6; }
    [[nodiscard]] double max_ms() const noexcept { return count == 0 ? 0.0 : static_cast<double>(max_ns) / 1e6; }
    [[nodiscard]] double p50_ms() const noexcept { return static_cast<double>(percentile_ns(0.50)) / 1e6; }
    [[nodiscard]] double p95_ms() const noexcept { return static_cast<double>(percentile_ns(0.95)) / 1e6; }
    [[nodiscard]] double p99_ms() const noexcept { return static_cast<double>(percentile_ns(0.99)) / 1e6; }

    [[nodiscard]] LatencyHistogram diff(const LatencyHistogram& earlier) const noexcept;
};

// ---------------------------------------------------------------------------
// Structured statistics types
// ---------------------------------------------------------------------------

struct StorageOpStats {
    uint64_t ok{0};
    uint64_t not_found{0};
    uint64_t conflict{0};
    uint64_t unsupported{0};
    uint64_t error{0};
    uint64_t bytes{0};
    uint64_t items{0};
    uint64_t pages{0};
    uint64_t partial_delete_retries{0};
    LatencyHistogram latency;
    LatencyHistogram error_latency;

    [[nodiscard]] uint64_t total_count() const noexcept { return ok + not_found + conflict + unsupported + error; }
    [[nodiscard]] uint64_t non_error_count() const noexcept { return ok + not_found + conflict + unsupported; }
    [[nodiscard]] bool empty() const noexcept {
        return total_count() == 0 && bytes == 0 && items == 0 && pages == 0 && partial_delete_retries == 0;
    }
    void merge(const StorageOpStats& other) noexcept;
};

struct HttpAttemptTimings {
    uint64_t dns_ns{0};
    uint64_t tcp_connect_ns{0};
    uint64_t tls_handshake_ns{0};
    uint64_t ttfb_ns{0};
    uint64_t transfer_ns{0};
    bool has_timings{false};
};

struct HttpOpStats {
    uint64_t requests{0};
    uint64_t attempts{0};
    uint64_t retries{0};
    uint64_t status_2xx{0};
    uint64_t status_3xx{0};
    uint64_t status_4xx{0};
    uint64_t status_5xx{0};
    uint64_t throttled{0};
    uint64_t network_errors{0};
    uint64_t checksum_mismatch_retries{0};
    uint64_t new_connections{0};
    uint64_t backoff_time_ns{0};
    LatencyHistogram request_latency;
    LatencyHistogram attempt_latency;
    LatencyHistogram dns_latency;
    LatencyHistogram tcp_connect_latency;
    LatencyHistogram tls_handshake_latency;
    LatencyHistogram ttfb_latency;
    LatencyHistogram transfer_latency;

    [[nodiscard]] bool empty() const noexcept {
        return requests == 0 && attempts == 0 && retries == 0 && checksum_mismatch_retries == 0;
    }
    void merge(const HttpOpStats& other) noexcept;
};

struct SymbolStats {
    uint64_t reads{0};
    uint64_t writes{0};
    uint64_t deletes{0};
    uint64_t exists{0};
    uint64_t lists{0};
    uint64_t errors{0};
    uint64_t bytes_read{0};
    uint64_t bytes_written{0};
    uint64_t http_requests{0};
    uint64_t http_retries{0};
    LatencyHistogram storage_latency;

    [[nodiscard]] uint64_t total_ops() const noexcept { return reads + writes + deletes + exists + lists; }
    [[nodiscard]] bool empty() const noexcept {
        return total_ops() == 0 && errors == 0 && http_requests == 0 && http_retries == 0;
    }
    void merge(const SymbolStats& other) noexcept;
};

struct ThreadPoolStats {
    uint64_t tasks{0};
    uint64_t expired{0};
    LatencyHistogram wait_latency;
    LatencyHistogram run_latency;

    [[nodiscard]] bool empty() const noexcept { return tasks == 0 && expired == 0; }
    void merge(const ThreadPoolStats& other) noexcept;
    void reset() noexcept;
};

struct ThreadPoolsStats {
    ThreadPoolStats cpu;
    ThreadPoolStats io;
    uint64_t semaphore_waits{0};
    LatencyHistogram semaphore_wait_latency;

    [[nodiscard]] bool empty() const noexcept { return cpu.empty() && io.empty() && semaphore_waits == 0; }
    void merge(const ThreadPoolsStats& other) noexcept;
    void reset() noexcept;
};

struct VersionMapCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t reloads{0};
    uint64_t ref_retries{0};
    uint64_t chain_hops{0};

    [[nodiscard]] bool empty() const noexcept {
        return hits == 0 && misses == 0 && reloads == 0 && ref_retries == 0 && chain_hops == 0;
    }
    void merge(const VersionMapCacheStats& other) noexcept;
    void reset() noexcept { *this = {}; }
};

struct DeDupCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t bytes_saved{0};

    [[nodiscard]] bool empty() const noexcept { return hits == 0 && misses == 0 && bytes_saved == 0; }
    void merge(const DeDupCacheStats& other) noexcept;
    void reset() noexcept { *this = {}; }
};

struct ConfigCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};

    [[nodiscard]] bool empty() const noexcept { return hits == 0 && misses == 0; }
    void merge(const ConfigCacheStats& other) noexcept;
    void reset() noexcept { *this = {}; }
};

struct CacheStats {
    VersionMapCacheStats version_map;
    DeDupCacheStats dedup_map;
    ConfigCacheStats config_cache;

    [[nodiscard]] bool empty() const noexcept {
        return version_map.empty() && dedup_map.empty() && config_cache.empty();
    }
    void merge(const CacheStats& other) noexcept;
    void reset() noexcept;
};

struct CodecStats {
    uint64_t encodes{0};
    uint64_t decodes{0};
    uint64_t encode_uncompressed_bytes{0};
    uint64_t encode_compressed_bytes{0};
    uint64_t decode_compressed_bytes{0};
    uint64_t decode_uncompressed_bytes{0};
    LatencyHistogram encode_latency;
    LatencyHistogram decode_latency;

    [[nodiscard]] double encode_compression_ratio() const noexcept {
        return encode_compressed_bytes == 0
                       ? 0.0
                       : static_cast<double>(encode_uncompressed_bytes) / static_cast<double>(encode_compressed_bytes);
    }
    [[nodiscard]] double decode_compression_ratio() const noexcept {
        return decode_compressed_bytes == 0
                       ? 0.0
                       : static_cast<double>(decode_uncompressed_bytes) / static_cast<double>(decode_compressed_bytes);
    }
    [[nodiscard]] bool empty() const noexcept { return encodes == 0 && decodes == 0; }
    void merge(const CodecStats& other) noexcept;
    void reset() noexcept;
};

struct QueryPruningStats {
    uint64_t queries{0};
    uint64_t segments_total{0};
    uint64_t segments_pruned_index{0};
    uint64_t segments_pruned_column_stats{0};
    uint64_t segments_read{0};
    uint64_t columns_total{0};
    uint64_t columns_projected{0};
    uint64_t rows_total{0};
    uint64_t rows_selected{0};

    [[nodiscard]] bool empty() const noexcept { return queries == 0; }
    void merge(const QueryPruningStats& other) noexcept;
    void reset() noexcept { *this = {}; }
};

struct LockStats {
    uint64_t acquires{0};
    uint64_t conflicts{0};
    uint64_t retries{0};
    uint64_t timeouts{0};
    LatencyHistogram wait_latency;

    [[nodiscard]] bool empty() const noexcept {
        return acquires == 0 && conflicts == 0 && retries == 0 && timeouts == 0 && wait_latency.empty();
    }
    void merge(const LockStats& other) noexcept;
    void reset() noexcept;
};

struct ApiOpStats {
    uint64_t ok{0};
    uint64_t error{0};
    LatencyHistogram latency;
    LatencyHistogram error_latency;

    [[nodiscard]] uint64_t total_count() const noexcept { return ok + error; }
    [[nodiscard]] bool empty() const noexcept { return ok == 0 && error == 0; }
    void merge(const ApiOpStats& other) noexcept;
};

struct BoundaryPhaseStats {
    uint64_t count{0};
    LatencyHistogram latency;

    [[nodiscard]] bool empty() const noexcept { return count == 0; }
    void merge(const BoundaryPhaseStats& other) noexcept;
};

struct ClauseExecStats {
    uint64_t executions{0};
    uint64_t rows_in{0};
    uint64_t rows_out{0};
    LatencyHistogram latency;

    [[nodiscard]] bool empty() const noexcept { return executions == 0; }
    void merge(const ClauseExecStats& other) noexcept;
};

struct AdmissionStats {
    uint64_t units_admitted{0};
    uint64_t residency_waits{0};
    uint64_t segments_dispatched{0};
    uint64_t read_window_waits{0};
    LatencyHistogram residency_wait_latency;
    LatencyHistogram read_window_wait_latency;

    [[nodiscard]] bool empty() const noexcept {
        return units_admitted == 0 && residency_waits == 0 && segments_dispatched == 0 && read_window_waits == 0;
    }
    void merge(const AdmissionStats& other) noexcept;
    void reset() noexcept;
};

using StorageStatsMap = std::map<StorageBackend, std::map<StorageOp, std::map<std::string, StorageOpStats>>>;
using HttpStatsMap = std::map<std::string, std::map<std::string, HttpOpStats>>;
using SymbolStatsMap = std::map<std::string, SymbolStats>;
using LockStatsMap = std::map<LockKind, LockStats>;
using ApiStatsMap = std::map<ApiOp, ApiOpStats>;
using BoundaryStatsMap = std::map<BoundaryPhase, BoundaryPhaseStats>;
using ClauseStatsMap = std::map<ClauseKind, ClauseExecStats>;

struct ProcessingStats {
    ClauseStatsMap clauses;
    AdmissionStats admission;

    [[nodiscard]] bool empty() const noexcept { return clauses.empty() && admission.empty(); }
};

struct RowCountHistogram {
    static constexpr std::array<uint64_t, 13> BOUNDS =
            {1, 10, 100, 500, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000};
    static constexpr size_t NUM_BUCKETS = BOUNDS.size() + 1; // 14

    uint64_t count{0};
    uint64_t sum{0};
    uint64_t min{std::numeric_limits<uint64_t>::max()};
    uint64_t max{0};
    std::array<uint64_t, NUM_BUCKETS> buckets{};

    static constexpr size_t bucket_for(uint64_t rows) noexcept {
        size_t idx = 0;
        while (idx < BOUNDS.size() && rows > BOUNDS[idx]) {
            ++idx;
        }
        return idx;
    }

    void record(uint64_t rows) noexcept {
        ++count;
        sum += rows;
        if (rows < min) {
            min = rows;
        }
        if (rows > max) {
            max = rows;
        }
        ++buckets[bucket_for(rows)];
    }

    void merge(const RowCountHistogram& other) noexcept {
        if (other.count == 0) {
            return;
        }
        count += other.count;
        sum += other.sum;
        if (other.min < min) {
            min = other.min;
        }
        if (other.max > max) {
            max = other.max;
        }
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            buckets[i] += other.buckets[i];
        }
    }

    void reset() noexcept {
        count = 0;
        sum = 0;
        min = std::numeric_limits<uint64_t>::max();
        max = 0;
        buckets.fill(0);
    }

    [[nodiscard]] bool empty() const noexcept { return count == 0; }
    [[nodiscard]] double mean() const noexcept {
        return count == 0 ? 0.0 : static_cast<double>(sum) / static_cast<double>(count);
    }
    [[nodiscard]] uint64_t min_value() const noexcept { return count == 0 ? 0 : min; }
    [[nodiscard]] uint64_t percentile(double p) const noexcept;
    [[nodiscard]] uint64_t p50() const noexcept { return percentile(0.50); }
    [[nodiscard]] uint64_t p95() const noexcept { return percentile(0.95); }
    [[nodiscard]] uint64_t p99() const noexcept { return percentile(0.99); }
    [[nodiscard]] RowCountHistogram diff(const RowCountHistogram& earlier) const noexcept;
};

struct CompactionStats {
    uint64_t count{0};
    uint64_t input_segments{0};
    uint64_t output_segments{0};
    LatencyHistogram latency;

    [[nodiscard]] bool empty() const noexcept { return count == 0; }
    void merge(const CompactionStats& other) noexcept;
    void reset() noexcept;
};

using CompactionStatsMap = std::map<CompactionKind, CompactionStats>;
using WriteConflictStatsMap = std::map<WriteConflictKind, uint64_t>;

struct StreamWriteStats {
    static constexpr uint64_t SMALL_SEGMENT_ROW_THRESHOLD = 1000;

    uint64_t data_segments_written{0};
    uint64_t staged_segments_written{0};
    uint64_t small_segments_written{0};
    RowCountHistogram rows_per_segment;
    uint64_t incomplete_chain_loads{0};
    uint64_t incomplete_chain_hops{0};
    uint64_t incomplete_iteration_loads{0};
    uint64_t incomplete_iteration_segments{0};
    CompactionStatsMap compactions;
    WriteConflictStatsMap conflicts;

    [[nodiscard]] bool empty() const noexcept {
        return data_segments_written == 0 && staged_segments_written == 0 && small_segments_written == 0 &&
               rows_per_segment.empty() && incomplete_chain_loads == 0 && incomplete_chain_hops == 0 &&
               incomplete_iteration_loads == 0 && incomplete_iteration_segments == 0 && compactions.empty() &&
               conflicts.empty();
    }
    void merge(const StreamWriteStats& other);
    void reset();
};

struct PoolGaugeSnapshot {
    uint64_t pending_tasks{0};
    uint64_t active_threads{0};
    uint64_t total_threads{0};
    uint64_t max_threads{0};
};

struct LmdbGaugeSnapshot {
    uint64_t open_envs{0};
    uint64_t map_size_bytes{0};
    uint64_t used_bytes{0};
    uint64_t active_readers{0};
    uint64_t map_full_errors{0};
};

struct ResourceStats {
    PoolGaugeSnapshot cpu_pool;
    PoolGaugeSnapshot io_pool;
    uint64_t write_semaphore_in_flight{0};
    uint64_t write_semaphore_limit{0};
    uint64_t version_map_entries{0};
    uint64_t config_cache_libraries{0};
    uint64_t config_cache_storages{0};
    LmdbGaugeSnapshot lmdb;
    bool has_pool_gauges{false};
};

struct LibraryStatsSnapshot {
    StorageStatsMap storage;
    HttpStatsMap http;
    ApiStatsMap api;
    SymbolStatsMap by_symbol;
    uint64_t dropped_symbols{0};
};

struct QueryStatsSnapshot {
    double wall_time_ms{0.0};
    StorageStatsMap storage;
    HttpStatsMap http;
    ApiStatsMap api;
    std::map<std::string, LibraryStatsSnapshot> by_library;
    SymbolStatsMap by_symbol;
    uint64_t dropped_symbols{0};
    ThreadPoolsStats thread_pools;
    CacheStats caches;
    CodecStats codec;
    QueryPruningStats query;
    LockStatsMap locks;
    BoundaryStatsMap boundary;
    ProcessingStats processing;
    StreamWriteStats stream;
    ResourceStats resources;
};

// ---------------------------------------------------------------------------
// Scoped collector, session, and context propagation
// ---------------------------------------------------------------------------

struct CollectorOptions {
    bool by_library{true};
    bool by_symbol{false};
    size_t max_symbols{1000};
    bool isolate{false};
};

class QueryStatsCollector {
  public:
    explicit QueryStatsCollector(CollectorOptions options = {});
    ~QueryStatsCollector();

    ARCTICDB_NO_MOVE_OR_COPY(QueryStatsCollector);

    [[nodiscard]] QueryStatsSnapshot snapshot() const;
    void reset();
    [[nodiscard]] const CollectorOptions& options() const noexcept;

    struct Impl;

  private:
    friend class CollectorSession;
    friend class QueryStats;
    std::shared_ptr<Impl> impl_;
};

// Attaches a QueryStatsCollector to the current thread's folly::RequestContext
// (automatically inherited by TaskScheduler CPU/IO tasks, folly::Future chains,
// and ContextPropagatingAwsExecutor). Supports explicit enter()/exit() for
// Python context managers as well as RAII scoped usage in C++.
class CollectorSession {
  public:
    explicit CollectorSession(std::shared_ptr<QueryStatsCollector> collector, bool auto_enter = true);
    explicit CollectorSession(CollectorOptions options = {}, bool auto_enter = true);
    ~CollectorSession();

    ARCTICDB_NO_MOVE_OR_COPY(CollectorSession);

    void enter();
    void exit();
    [[nodiscard]] bool is_entered() const noexcept { return entered_; }
    [[nodiscard]] QueryStatsSnapshot snapshot() const;
    [[nodiscard]] const std::shared_ptr<QueryStatsCollector>& collector() const noexcept { return collector_; }

  private:
    std::shared_ptr<QueryStatsCollector> collector_;
    std::shared_ptr<void> prev_ctx_;
    bool entered_{false};
};

// Tags the current folly::RequestContext with ambient library and/or symbol
// attribution while active. Completely zero-cost when !is_active().
class ContextTagScope {
  public:
    ContextTagScope(
            std::string_view library, std::string_view symbol = {}, std::optional<StorageOp> op = std::nullopt,
            std::optional<entity::KeyType> key_type = std::nullopt
    );
    ~ContextTagScope();

    ARCTICDB_NO_MOVE_OR_COPY(ContextTagScope);

  private:
    std::shared_ptr<void> prev_ctx_;
    bool active_{false};
};

// Opaque handle capturing the current folly::RequestContext across async boundaries
// (TaskScheduler, AWS async executor, etc.) without exposing folly headers here.
class CapturedContext {
  public:
    CapturedContext() = default;
    [[nodiscard]] explicit operator bool() const noexcept { return ctx_ != nullptr; }

  private:
    friend CapturedContext capture_context();
    friend class RestoredContextGuard;
    explicit CapturedContext(std::shared_ptr<void> ctx) noexcept : ctx_(std::move(ctx)) {}
    std::shared_ptr<void> ctx_;
};

[[nodiscard]] CapturedContext capture_context();

class RestoredContextGuard {
  public:
    explicit RestoredContextGuard(const CapturedContext& ctx);
    ~RestoredContextGuard();

    ARCTICDB_NO_MOVE_OR_COPY(RestoredContextGuard);

  private:
    std::shared_ptr<void> prev_;
    bool active_{false};
};

// ---------------------------------------------------------------------------
// Recording entry points (all check is_active() fast-path)
// ---------------------------------------------------------------------------

struct BatchDeleteKeyInfo {
    entity::KeyType key_type{entity::KeyType::UNDEFINED};
    std::string_view symbol{};
};

void record_storage_op(
        StorageBackend backend, StorageOp op, entity::KeyType key_type, StorageOutcome outcome, uint64_t duration_ns,
        uint64_t bytes = 0, uint64_t items = 1, std::string_view library = {}, std::string_view symbol = {}
);

void record_storage_batch_delete(
        StorageBackend backend, std::span<const BatchDeleteKeyInfo> keys, StorageOutcome outcome, uint64_t duration_ns,
        uint64_t bytes = 0, std::string_view library = {}, std::string_view fallback_symbol = {}
);

// Records an individual paginated list fetch (e.g. S3 ListObjectsV2 page or Azure ListBlobs page).
// Updates `pages` in structured storage stats and preserves per-page legacy `S3_ListObjectsV2` counts.
void record_storage_page(
        StorageBackend backend, StorageOp op, entity::KeyType key_type, uint64_t duration_ns, uint64_t items = 0,
        std::string_view library = {}, std::string_view symbol = {}
);

void record_partial_delete_retry(
        StorageBackend backend, std::string_view key_type_label, std::string_view library = {},
        std::string_view symbol = {}
);

// Preserves legacy Memory_DeleteObject per-key counts and deleted byte sizes during MemoryStorage::do_remove.
void record_legacy_memory_delete(entity::KeyType key_type, uint64_t bytes);

// Captures total deleted bytes reported by a backend (e.g. MemoryStorage) during Storage::remove.
class ScopedDeletedBytesTracker {
  public:
    ScopedDeletedBytesTracker() noexcept;
    ~ScopedDeletedBytesTracker() noexcept;

    ARCTICDB_NO_MOVE_OR_COPY(ScopedDeletedBytesTracker);

    [[nodiscard]] uint64_t bytes() const noexcept { return bytes_; }

  private:
    friend void note_deleted_bytes(uint64_t bytes) noexcept;
    uint64_t bytes_{0};
    ScopedDeletedBytesTracker* prev_{nullptr};
};

void note_deleted_bytes(uint64_t bytes) noexcept;

void record_http_attempt(
        std::string_view service, std::string_view operation, HttpStatusClass status_class,
        uint64_t attempt_duration_ns, uint64_t new_connections = 0, uint64_t backoff_ns = 0, bool throttled = false,
        const HttpAttemptTimings& timings = {}
);

void record_http_retry(std::string_view service, std::string_view operation, uint64_t backoff_ns = 0);

void record_http_request_finish(
        std::string_view service, std::string_view operation, uint64_t total_request_duration_ns,
        uint64_t extra_backoff_ns = 0
);

void record_checksum_mismatch_retry(std::string_view service, std::string_view operation);

void record_pool_task(PoolKind pool, uint64_t wait_ns, uint64_t run_ns);
void record_pool_task_expired(PoolKind pool);
void record_semaphore_wait(uint64_t wait_ns);

void record_version_map_cache(bool hit, bool is_reload = false);
void record_version_map_chain_hops(uint64_t hops);
void record_version_map_ref_retry();
void record_dedup_lookup(bool hit, uint64_t bytes_saved = 0);
void record_config_cache(bool hit);

void record_codec_encode(uint64_t duration_ns, uint64_t uncompressed_bytes, uint64_t compressed_bytes);
void record_codec_decode(uint64_t duration_ns, uint64_t compressed_bytes, uint64_t uncompressed_bytes);

void record_query_pruning(
        uint64_t segments_total, uint64_t segments_pruned_index, uint64_t segments_pruned_column_stats,
        uint64_t segments_read, uint64_t columns_total, uint64_t columns_projected, uint64_t rows_total,
        uint64_t rows_selected
);

void record_lock_event(
        LockKind kind, bool acquired, uint64_t conflicts, uint64_t retries, bool timed_out, uint64_t wait_ns
);

void record_api_op(ApiOp op, bool ok, uint64_t duration_ns, std::string_view library = {});

// Inline disabled fast path (~0.5 ns when !is_active()); automatically guards against
// double-counting nested internal calls on the same thread and classifies thrown exceptions as errors.
class ApiOpScope {
  public:
    ApiOpScope(ApiOp op, std::string_view library = {}) noexcept : op_(op), library_(library) {
        if (is_active()) {
            enter();
        }
    }

    ~ApiOpScope() noexcept {
        if (active_) {
            exit();
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(ApiOpScope);

    void mark_error() noexcept { forced_error_ = true; }

  private:
    void enter() noexcept;
    void exit() noexcept;

    ApiOp op_;
    std::string_view library_;
    TimePoint start_{};
    int uncaught_on_enter_{0};
    bool active_{false};
    bool forced_error_{false};
};

void record_boundary_op(BoundaryPhase phase, uint64_t duration_ns);

// Inline disabled fast path (~0.5 ns when !is_active()) for Python/Arrow C++ boundary conversions.
class BoundaryOpScope {
  public:
    explicit BoundaryOpScope(BoundaryPhase phase) noexcept : phase_(phase) {
        if (is_active()) {
            enter();
        }
    }

    ~BoundaryOpScope() noexcept {
        if (active_) {
            exit();
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(BoundaryOpScope);

  private:
    void enter() noexcept;
    void exit() noexcept;

    BoundaryPhase phase_;
    TimePoint start_{};
    bool active_{false};
};

void record_clause_exec(ClauseKind kind, uint64_t duration_ns, uint64_t rows_in, uint64_t rows_out);
void record_admission_unit(bool waited, uint64_t wait_ns = 0);
void record_admission_segment(bool waited, uint64_t wait_ns = 0);

void record_segment_written(uint64_t rows, bool staged);
void record_incomplete_chain_load(uint64_t segments_or_hops, bool via_iteration);
void record_compaction(CompactionKind kind, uint64_t input_segments, uint64_t output_segments, uint64_t duration_ns);
void record_write_conflict(WriteConflictKind kind);
void record_lmdb_map_full();

using PoolGaugeProvider = std::function<void(PoolGaugeSnapshot& cpu, PoolGaugeSnapshot& io)>;
void set_pool_gauge_provider(PoolGaugeProvider provider);

using LmdbEnvGaugeFn = std::function<void(uint64_t& map_size_bytes, uint64_t& used_bytes, uint64_t& active_readers)>;

class LmdbGaugeHandle {
  public:
    LmdbGaugeHandle() noexcept = default;
    explicit LmdbGaugeHandle(uint64_t id) noexcept : id_(id) {}
    LmdbGaugeHandle(LmdbGaugeHandle&& other) noexcept : id_(other.id_) { other.id_ = 0; }
    LmdbGaugeHandle& operator=(LmdbGaugeHandle&& other) noexcept {
        if (this != &other) {
            reset();
            id_ = other.id_;
            other.id_ = 0;
        }
        return *this;
    }
    LmdbGaugeHandle(const LmdbGaugeHandle&) = delete;
    LmdbGaugeHandle& operator=(const LmdbGaugeHandle&) = delete;
    ~LmdbGaugeHandle() noexcept { reset(); }

    void reset() noexcept;

  private:
    uint64_t id_{0};
};

[[nodiscard]] LmdbGaugeHandle register_lmdb_env_gauge(LmdbEnvGaugeFn fn);
[[nodiscard]] ResourceStats sample_live_resources();

// ---------------------------------------------------------------------------
// Optional export sink (e.g. Prometheus forwarding; never emits symbol labels)
// ---------------------------------------------------------------------------

class StatsSink {
  public:
    virtual ~StatsSink() = default;
    virtual void on_storage_op(
            StorageBackend backend, StorageOp op, std::string_view key_type, StorageOutcome outcome,
            uint64_t duration_ns, uint64_t bytes, uint64_t items, std::string_view library
    ) = 0;
    virtual void on_http_attempt(
            std::string_view service, std::string_view operation, HttpStatusClass status_class,
            uint64_t attempt_duration_ns, uint64_t new_connections, std::string_view library
    ) = 0;
    virtual void on_http_request_finish(
            std::string_view service, std::string_view operation, uint64_t total_request_duration_ns,
            std::string_view library
    ) = 0;
    virtual void on_http_retry(
            std::string_view service, std::string_view operation, uint64_t backoff_ns, std::string_view library
    ) = 0;

    virtual void on_storage_page(StorageBackend /*backend*/, StorageOp /*op*/, std::string_view /*library*/) {}
    virtual void on_partial_delete_retry(StorageBackend /*backend*/, std::string_view /*library*/) {}
    virtual void on_http_backoff(
            std::string_view /*service*/, std::string_view /*operation*/, uint64_t /*backoff_ns*/,
            std::string_view /*library*/
    ) {}
    virtual void on_http_checksum_mismatch_retry(
            std::string_view /*service*/, std::string_view /*operation*/, std::string_view /*library*/
    ) {}
    virtual void on_http_throttled(
            std::string_view /*service*/, std::string_view /*operation*/, std::string_view /*library*/
    ) {}
    virtual void on_http_phase_timings(
            std::string_view /*service*/, std::string_view /*operation*/, const HttpAttemptTimings& /*timings*/,
            std::string_view /*library*/
    ) {}

    virtual void on_pool_task(PoolKind /*pool*/, uint64_t /*wait_ns*/, uint64_t /*run_ns*/) {}
    virtual void on_pool_task_expired(PoolKind /*pool*/) {}
    virtual void on_semaphore_wait(uint64_t /*wait_ns*/) {}

    virtual void on_version_map_cache(bool /*hit*/, bool /*is_reload*/) {}
    virtual void on_version_map_chain_hops(uint64_t /*hops*/) {}
    virtual void on_version_map_ref_retry() {}
    virtual void on_dedup_lookup(bool /*hit*/, uint64_t /*bytes_saved*/) {}
    virtual void on_config_cache(bool /*hit*/) {}

    virtual void on_codec_encode(
            uint64_t /*duration_ns*/, uint64_t /*uncompressed_bytes*/, uint64_t /*compressed_bytes*/
    ) {}
    virtual void on_codec_decode(
            uint64_t /*duration_ns*/, uint64_t /*compressed_bytes*/, uint64_t /*uncompressed_bytes*/
    ) {}

    virtual void on_query_pruning(
            uint64_t /*segments_total*/, uint64_t /*segments_pruned_index*/, uint64_t /*segments_pruned_column_stats*/,
            uint64_t /*segments_read*/, uint64_t /*columns_total*/, uint64_t /*columns_projected*/,
            uint64_t /*rows_total*/, uint64_t /*rows_selected*/
    ) {}

    virtual void on_lock_event(
            LockKind /*kind*/, bool /*acquired*/, uint64_t /*conflicts*/, uint64_t /*retries*/, bool /*timed_out*/,
            uint64_t /*wait_ns*/
    ) {}

    virtual void on_api_op(ApiOp /*op*/, bool /*ok*/, uint64_t /*duration_ns*/, std::string_view /*library*/) {}
    virtual void on_boundary_op(BoundaryPhase /*phase*/, uint64_t /*duration_ns*/) {}
    virtual void on_clause_exec(
            ClauseKind /*kind*/, uint64_t /*duration_ns*/, uint64_t /*rows_in*/, uint64_t /*rows_out*/
    ) {}
    virtual void on_admission_unit(bool /*waited*/, uint64_t /*wait_ns*/) {}
    virtual void on_admission_segment(bool /*waited*/, uint64_t /*wait_ns*/) {}

    virtual void on_segment_written(uint64_t /*rows*/, bool /*staged*/) {}
    virtual void on_incomplete_chain_load(uint64_t /*segments_or_hops*/, bool /*via_iteration*/) {}
    virtual void on_compaction(
            CompactionKind /*kind*/, uint64_t /*input_segments*/, uint64_t /*output_segments*/, uint64_t /*duration_ns*/
    ) {}
    virtual void on_write_conflict(WriteConflictKind /*kind*/) {}
    virtual void on_lmdb_map_full() {}
};

void set_stats_sink(std::shared_ptr<StatsSink> sink);
[[nodiscard]] std::shared_ptr<StatsSink> get_stats_sink();
[[nodiscard]] std::shared_ptr<StatsSink> make_prometheus_stats_sink(
        std::shared_ptr<PrometheusInstance> prometheus = nullptr, bool include_library = false
);
void enable_prometheus_sink(bool include_library = false);
void disable_prometheus_sink();

// ---------------------------------------------------------------------------
// Backward-compatible global singleton API (QueryStats)
// ---------------------------------------------------------------------------

class RAIIAddTime {
  public:
    RAIIAddTime(TaskType task_type, entity::KeyType key_type, TimePoint start);
    RAIIAddTime(RAIIAddTime&& other) noexcept;
    RAIIAddTime& operator=(RAIIAddTime&& other) noexcept;
    RAIIAddTime(const RAIIAddTime&) = delete;
    RAIIAddTime& operator=(const RAIIAddTime&) = delete;
    ~RAIIAddTime();

  private:
    TaskType task_type_;
    entity::KeyType key_type_;
    TimePoint start_;
    bool active_{true};
};

class QueryStats {
  public:
    using OperationStatsOutput = std::map<std::string, uint64_t>;
    using QueryStatsOutput = std::map<std::string, std::map<std::string, std::map<std::string, OperationStatsOutput>>>;

    ARCTICDB_NO_MOVE_OR_COPY(QueryStats);

    static std::shared_ptr<QueryStats> instance();

    void reset_stats();
    void enable();
    void disable();
    [[nodiscard]] bool is_enabled() const;

    void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value);
    [[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(
            TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start = std::nullopt
    );

    [[nodiscard]] QueryStatsOutput get_stats() const;
    [[nodiscard]] QueryStatsSnapshot get_structured_snapshot() const;

    QueryStats();
    ~QueryStats();

  private:
    friend void detail_record_legacy_task(TaskType, entity::KeyType, uint64_t, uint64_t, uint64_t);
    static std::once_flag init_flag_;
    static std::shared_ptr<QueryStats> instance_;

    std::atomic<bool> is_enabled_{false};
    std::shared_ptr<QueryStatsCollector> global_collector_;
};

void add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value);
[[nodiscard]] std::optional<RAIIAddTime> add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start = std::nullopt
);

} // namespace arcticdb::query_stats
