/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <ankerl/unordered_dense.h>
#include <folly/ThreadLocal.h>
#include <folly/io/async/Request.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <exception>
#include <functional>
#include <limits>
#include <mutex>
#include <utility>

namespace arcticdb::query_stats {

namespace detail {
std::atomic<uint32_t> active_collector_count{0};
std::atomic<int64_t> write_semaphore_in_flight{0};
std::atomic<uint64_t> write_semaphore_limit{0};
std::atomic<int64_t> version_map_entries{0};
std::atomic<int64_t> config_cache_libraries{0};
std::atomic<int64_t> config_cache_storages{0};
} // namespace detail

namespace {

constexpr const char* OTHER_SYMBOL_BUCKET = "__other__";
constexpr const char* MIXED_KEY_TYPE_LABEL = "MIXED";

// Fixed-size O(1)-memory approximate distinct counter for spilled symbols once max_symbols is reached.
struct DroppedSymbolFilter {
    static constexpr size_t NUM_WORDS = 32; // 2048 bits = 256 bytes
    static constexpr size_t NUM_BITS = NUM_WORDS * 64;

    std::array<uint64_t, NUM_WORDS> bits{};
    uint64_t count{0};

    void note(std::string_view symbol) noexcept {
        const size_t h = ankerl::unordered_dense::hash<std::string_view>{}(symbol);
        const size_t bit = h & (NUM_BITS - 1);
        const uint64_t mask = 1ULL << (bit & 63U);
        auto& word = bits[bit >> 6U];
        if ((word & mask) == 0) {
            word |= mask;
            ++count;
        }
    }

    void merge(const DroppedSymbolFilter& other) noexcept {
        if (other.count == 0) {
            return;
        }
        uint64_t pop = 0;
        for (size_t i = 0; i < NUM_WORDS; ++i) {
            bits[i] |= other.bits[i];
            pop += static_cast<uint64_t>(std::popcount(bits[i]));
        }
        count = std::max(count, pop);
    }

    void reset() noexcept {
        bits.fill(0);
        count = 0;
    }
};

struct LegacyOpBucket {
    uint64_t count{0};
    uint64_t total_time_ns{0};
    uint64_t size_bytes{0};

    void merge(const LegacyOpBucket& other) noexcept {
        count += other.count;
        total_time_ns += other.total_time_ns;
        size_bytes += other.size_bytes;
    }
};

using LegacyTaskArray = std::array<
        std::array<LegacyOpBucket, static_cast<size_t>(entity::KeyType::UNDEFINED)>,
        static_cast<size_t>(TaskType::END)>;

struct LibraryBucket {
    ankerl::unordered_dense::map<
            uint16_t, // (backend << 8) | op
            ankerl::unordered_dense::map<std::string, StorageOpStats>>
            storage;
    ankerl::unordered_dense::map<std::string, ankerl::unordered_dense::map<std::string, HttpOpStats>> http;
    ankerl::unordered_dense::map<uint8_t, ApiOpStats> api;
    ankerl::unordered_dense::map<std::string, SymbolStats> by_symbol;
    DroppedSymbolFilter dropped_symbols;

    void reset() {
        storage.clear();
        http.clear();
        api.clear();
        by_symbol.clear();
        dropped_symbols.reset();
    }
};

struct StreamBucket {
    uint64_t data_segments_written{0};
    uint64_t staged_segments_written{0};
    uint64_t small_segments_written{0};
    RowCountHistogram rows_per_segment;
    uint64_t incomplete_chain_loads{0};
    uint64_t incomplete_chain_hops{0};
    uint64_t incomplete_iteration_loads{0};
    uint64_t incomplete_iteration_segments{0};
    ankerl::unordered_dense::map<uint8_t, CompactionStats> compactions;
    std::array<uint64_t, static_cast<size_t>(WriteConflictKind::COUNT)> conflicts{};

    void merge(const StreamBucket& other) {
        data_segments_written += other.data_segments_written;
        staged_segments_written += other.staged_segments_written;
        small_segments_written += other.small_segments_written;
        rows_per_segment.merge(other.rows_per_segment);
        incomplete_chain_loads += other.incomplete_chain_loads;
        incomplete_chain_hops += other.incomplete_chain_hops;
        incomplete_iteration_loads += other.incomplete_iteration_loads;
        incomplete_iteration_segments += other.incomplete_iteration_segments;
        for (const auto& [k, v] : other.compactions) {
            compactions[k].merge(v);
        }
        for (size_t i = 0; i < conflicts.size(); ++i) {
            conflicts[i] += other.conflicts[i];
        }
    }
};

struct StatsBucket {
    ankerl::unordered_dense::map<
            uint16_t, // (backend << 8) | op
            ankerl::unordered_dense::map<std::string, StorageOpStats>>
            storage;
    ankerl::unordered_dense::map<std::string, ankerl::unordered_dense::map<std::string, HttpOpStats>> http;
    ankerl::unordered_dense::map<uint8_t, ApiOpStats> api;
    ankerl::unordered_dense::map<std::string, LibraryBucket> by_library;
    ankerl::unordered_dense::map<std::string, SymbolStats> by_symbol;
    DroppedSymbolFilter dropped_symbols;
    LegacyTaskArray legacy{};
    ThreadPoolsStats thread_pools;
    CacheStats caches;
    CodecStats codec;
    QueryPruningStats query;
    std::array<LockStats, static_cast<size_t>(LockKind::COUNT)> locks{};
    ankerl::unordered_dense::map<uint8_t, BoundaryPhaseStats> boundary;
    ankerl::unordered_dense::map<uint8_t, ClauseExecStats> clauses;
    std::optional<AdmissionStats> admission;
    std::optional<StreamBucket> stream;
    uint64_t lmdb_map_full_errors{0};

    void reset() {
        storage.clear();
        http.clear();
        api.clear();
        by_library.clear();
        by_symbol.clear();
        dropped_symbols.reset();
        legacy = {};
        thread_pools.reset();
        caches.reset();
        codec.reset();
        query.reset();
        for (auto& lk : locks) {
            lk.reset();
        }
        boundary.clear();
        clauses.clear();
        admission.reset();
        stream.reset();
        lmdb_map_full_errors = 0;
    }
};

constexpr uint16_t pack_backend_op(StorageBackend backend, StorageOp op) noexcept {
    return static_cast<uint16_t>((static_cast<uint16_t>(backend) << 8U) | static_cast<uint16_t>(op));
}

constexpr StorageBackend unpack_backend(uint16_t packed) noexcept {
    return static_cast<StorageBackend>((packed >> 8U) & 0xFFU);
}

constexpr StorageOp unpack_op(uint16_t packed) noexcept { return static_cast<StorageOp>(packed & 0xFFU); }

std::string normalize_service_name(std::string_view service) {
    std::string out;
    out.reserve(service.size());
    for (unsigned char ch : service) {
        out.push_back(static_cast<char>(std::tolower(ch)));
    }
    return out;
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

std::optional<TaskType> to_legacy_task_type(StorageBackend backend, StorageOp op) noexcept {
    const bool is_s3_family =
            backend == StorageBackend::S3 || backend == StorageBackend::NfsBacked || backend == StorageBackend::GcpXml;
    if (is_s3_family) {
        switch (op) {
        case StorageOp::Read:
            return TaskType::S3_GetObject;
        case StorageOp::AsyncRead:
            return TaskType::S3_GetObjectAsync;
        case StorageOp::Write:
        case StorageOp::WriteIfNone:
        case StorageOp::WriteIfMatch:
        case StorageOp::Update:
            return TaskType::S3_PutObject;
        case StorageOp::Delete:
        case StorageOp::DeleteBatch:
            return TaskType::S3_DeleteObjects;
        case StorageOp::Exists:
            return TaskType::S3_HeadObject;
        case StorageOp::List:
        case StorageOp::Scan:
        case StorageOp::VisitSizes:
        case StorageOp::VisitMetadata:
            return TaskType::S3_ListObjectsV2;
        default:
            return std::nullopt;
        }
    }
    if (backend == StorageBackend::Memory) {
        switch (op) {
        case StorageOp::Read:
        case StorageOp::AsyncRead:
            return TaskType::Memory_GetObject;
        case StorageOp::Write:
        case StorageOp::WriteIfNone:
        case StorageOp::WriteIfMatch:
        case StorageOp::Update:
            return TaskType::Memory_PutObject;
        case StorageOp::Delete:
        case StorageOp::DeleteBatch:
            return TaskType::Memory_DeleteObject;
        case StorageOp::Exists:
            return TaskType::Memory_HeadObject;
        case StorageOp::List:
        case StorageOp::Scan:
        case StorageOp::VisitSizes:
        case StorageOp::VisitMetadata:
            return TaskType::Memory_ListObjects;
        default:
            return std::nullopt;
        }
    }
    return std::nullopt;
}

void record_outcome_count(StorageOpStats& stats, StorageOutcome outcome) noexcept {
    switch (outcome) {
    case StorageOutcome::Ok:
        ++stats.ok;
        break;
    case StorageOutcome::NotFound:
        ++stats.not_found;
        break;
    case StorageOutcome::Conflict:
        ++stats.conflict;
        break;
    case StorageOutcome::Unsupported:
        ++stats.unsupported;
        break;
    case StorageOutcome::Error:
        ++stats.error;
        break;
    default:
        break;
    }
}

void apply_storage_op_to_stats(
        StorageOpStats& stats, StorageOutcome outcome, uint64_t duration_ns, uint64_t bytes, uint64_t items
) noexcept {
    record_outcome_count(stats, outcome);
    stats.bytes += bytes;
    stats.items += items;
    stats.latency.record(duration_ns);
    if (outcome == StorageOutcome::Error) {
        stats.error_latency.record(duration_ns);
    }
}

void apply_storage_op_to_symbol(
        SymbolStats& sym, StorageOp op, StorageOutcome outcome, uint64_t duration_ns, uint64_t bytes
) noexcept {
    switch (op) {
    case StorageOp::Read:
    case StorageOp::AsyncRead:
        ++sym.reads;
        sym.bytes_read += bytes;
        break;
    case StorageOp::Write:
    case StorageOp::WriteIfNone:
    case StorageOp::WriteIfMatch:
    case StorageOp::Update:
        ++sym.writes;
        sym.bytes_written += bytes;
        break;
    case StorageOp::Delete:
    case StorageOp::DeleteBatch:
    case StorageOp::FastDelete:
        ++sym.deletes;
        break;
    case StorageOp::Exists:
        ++sym.exists;
        break;
    case StorageOp::List:
    case StorageOp::Scan:
    case StorageOp::VisitSizes:
    case StorageOp::VisitMetadata:
        ++sym.lists;
        break;
    default:
        break;
    }
    if (outcome == StorageOutcome::Error) {
        ++sym.errors;
    }
    sym.storage_latency.record(duration_ns);
}

SymbolStats& find_or_insert_symbol(
        ankerl::unordered_dense::map<std::string, SymbolStats>& sym_map, DroppedSymbolFilter& dropped,
        std::string_view symbol, size_t max_symbols
) {
    auto it = sym_map.find(std::string(symbol));
    if (it != sym_map.end()) {
        return it->second;
    }
    if (max_symbols == 0 || sym_map.size() < max_symbols) {
        return sym_map.emplace(std::string(symbol), SymbolStats{}).first->second;
    }
    dropped.note(symbol);
    return sym_map[OTHER_SYMBOL_BUCKET];
}

void merge_storage_map(
        ankerl::unordered_dense::map<uint16_t, ankerl::unordered_dense::map<std::string, StorageOpStats>>& dst,
        const ankerl::unordered_dense::map<uint16_t, ankerl::unordered_dense::map<std::string, StorageOpStats>>& src
) {
    for (const auto& [packed, kt_map] : src) {
        auto& dst_kt = dst[packed];
        for (const auto& [kt, st] : kt_map) {
            dst_kt[kt].merge(st);
        }
    }
}

void merge_http_map(
        ankerl::unordered_dense::map<std::string, ankerl::unordered_dense::map<std::string, HttpOpStats>>& dst,
        const ankerl::unordered_dense::map<std::string, ankerl::unordered_dense::map<std::string, HttpOpStats>>& src
) {
    for (const auto& [svc, op_map] : src) {
        auto& dst_op = dst[svc];
        for (const auto& [op, st] : op_map) {
            dst_op[op].merge(st);
        }
    }
}

void merge_api_map(
        ankerl::unordered_dense::map<uint8_t, ApiOpStats>& dst,
        const ankerl::unordered_dense::map<uint8_t, ApiOpStats>& src
) {
    for (const auto& [op, st] : src) {
        dst[op].merge(st);
    }
}

void merge_symbol_map(
        ankerl::unordered_dense::map<std::string, SymbolStats>& dst,
        const ankerl::unordered_dense::map<std::string, SymbolStats>& src
) {
    for (const auto& [sym, st] : src) {
        dst[sym].merge(st);
    }
}

void merge_stats_bucket(StatsBucket& dst, const StatsBucket& src) {
    merge_storage_map(dst.storage, src.storage);
    merge_http_map(dst.http, src.http);
    merge_api_map(dst.api, src.api);
    merge_symbol_map(dst.by_symbol, src.by_symbol);
    dst.dropped_symbols.merge(src.dropped_symbols);

    for (const auto& [lib, lb] : src.by_library) {
        auto& dst_lb = dst.by_library[lib];
        merge_storage_map(dst_lb.storage, lb.storage);
        merge_http_map(dst_lb.http, lb.http);
        merge_api_map(dst_lb.api, lb.api);
        merge_symbol_map(dst_lb.by_symbol, lb.by_symbol);
        dst_lb.dropped_symbols.merge(lb.dropped_symbols);
    }

    for (size_t t = 0; t < static_cast<size_t>(TaskType::END); ++t) {
        for (size_t k = 0; k < static_cast<size_t>(entity::KeyType::UNDEFINED); ++k) {
            dst.legacy[t][k].merge(src.legacy[t][k]);
        }
    }

    dst.thread_pools.merge(src.thread_pools);
    dst.caches.merge(src.caches);
    dst.codec.merge(src.codec);
    dst.query.merge(src.query);
    for (size_t i = 0; i < static_cast<size_t>(LockKind::COUNT); ++i) {
        dst.locks[i].merge(src.locks[i]);
    }
    for (const auto& [phase, st] : src.boundary) {
        dst.boundary[phase].merge(st);
    }
    for (const auto& [kind, st] : src.clauses) {
        dst.clauses[kind].merge(st);
    }
    if (src.admission.has_value()) {
        if (!dst.admission.has_value()) {
            dst.admission.emplace();
        }
        dst.admission->merge(*src.admission);
    }
    if (src.stream.has_value()) {
        if (!dst.stream.has_value()) {
            dst.stream.emplace();
        }
        dst.stream->merge(*src.stream);
    }
    dst.lmdb_map_full_errors += src.lmdb_map_full_errors;
}

StorageStatsMap materialize_storage_map(
        const ankerl::unordered_dense::map<uint16_t, ankerl::unordered_dense::map<std::string, StorageOpStats>>& src
) {
    StorageStatsMap out;
    for (const auto& [packed, kt_map] : src) {
        const auto backend = unpack_backend(packed);
        const auto op = unpack_op(packed);
        for (const auto& [kt, st] : kt_map) {
            if (!st.empty()) {
                out[backend][op][kt] = st;
            }
        }
    }
    return out;
}

HttpStatsMap materialize_http_map(
        const ankerl::unordered_dense::map<std::string, ankerl::unordered_dense::map<std::string, HttpOpStats>>& src
) {
    HttpStatsMap out;
    for (const auto& [svc, op_map] : src) {
        for (const auto& [op, st] : op_map) {
            if (!st.empty()) {
                out[svc][op] = st;
            }
        }
    }
    return out;
}

ApiStatsMap materialize_api_map(const ankerl::unordered_dense::map<uint8_t, ApiOpStats>& src) {
    ApiStatsMap out;
    for (const auto& [op, st] : src) {
        if (!st.empty()) {
            out.emplace(static_cast<ApiOp>(op), st);
        }
    }
    return out;
}

void materialize_symbol_map(
        const ankerl::unordered_dense::map<std::string, SymbolStats>& src, const DroppedSymbolFilter& dropped_filter,
        size_t max_symbols, SymbolStatsMap& out, uint64_t& dropped_out
) {
    DroppedSymbolFilter merged_dropped = dropped_filter;
    std::optional<SymbolStats> other_bucket;
    std::vector<std::pair<std::string, SymbolStats>> named;
    named.reserve(src.size());

    for (const auto& [sym, st] : src) {
        if (st.empty()) {
            continue;
        }
        if (sym == OTHER_SYMBOL_BUCKET) {
            if (!other_bucket) {
                other_bucket = st;
            } else {
                other_bucket->merge(st);
            }
        } else {
            named.emplace_back(sym, st);
        }
    }

    if (max_symbols > 0 && named.size() > max_symbols) {
        // Keep top max_symbols by storage latency sum (tie-break by total_ops, then name)
        std::nth_element(
                named.begin(),
                named.begin() + static_cast<std::ptrdiff_t>(max_symbols),
                named.end(),
                [](const auto& a, const auto& b) {
                    if (a.second.storage_latency.sum_ns != b.second.storage_latency.sum_ns) {
                        return a.second.storage_latency.sum_ns > b.second.storage_latency.sum_ns;
                    }
                    if (a.second.total_ops() != b.second.total_ops()) {
                        return a.second.total_ops() > b.second.total_ops();
                    }
                    return a.first < b.first;
                }
        );
        for (size_t i = max_symbols; i < named.size(); ++i) {
            if (!other_bucket) {
                other_bucket = named[i].second;
            } else {
                other_bucket->merge(named[i].second);
            }
            merged_dropped.note(named[i].first);
        }
        named.resize(max_symbols);
    }

    dropped_out = merged_dropped.count;
    for (auto& [sym, st] : named) {
        out.emplace(std::move(sym), std::move(st));
    }
    if (other_bucket && !other_bucket->empty()) {
        out.emplace(OTHER_SYMBOL_BUCKET, std::move(*other_bucket));
    }
}

struct QueryStatsTlsTag {};

} // namespace

// ---------------------------------------------------------------------------
// QueryStatsCollector::Impl
// ---------------------------------------------------------------------------

struct QueryStatsCollector::Impl {
    // Shared with ThreadLocalEntry so a detached thread racing in StaticMetaBase::onThreadExit
    // against ~Impl() never dereferences a destroyed Impl while flushing its bucket.
    struct RetiredState {
        mutable std::mutex mutex;
        StatsBucket stats;
        std::atomic<bool> alive{true};
    };

    struct ThreadLocalEntry {
        explicit ThreadLocalEntry(std::shared_ptr<RetiredState> retired) : retired_(std::move(retired)) {}
        ~ThreadLocalEntry() {
            if (retired_ && retired_->alive.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lock(retired_->mutex);
                if (retired_->alive.load(std::memory_order_relaxed)) {
                    merge_stats_bucket(retired_->stats, bucket_);
                }
            }
        }

        std::shared_ptr<RetiredState> retired_;
        mutable std::mutex mutex_;
        StatsBucket bucket_;
    };

    explicit Impl(CollectorOptions options) :
        options_(options),
        start_time_(std::chrono::steady_clock::now()),
        retired_(std::make_shared<RetiredState>()),
        tls_([retired = retired_]() { return std::make_unique<ThreadLocalEntry>(retired); }) {}

    ~Impl() {
        retired_->alive.store(false, std::memory_order_release);
        // tls_ destroys after alive == false; any thread already mid-onThreadExit keeps
        // RetiredState alive via its own shared_ptr until ~ThreadLocalEntry() completes.
    }

    template<typename Fn>
    void mutate_local(Fn&& fn) {
        auto& entry = *(*tls_);
        std::lock_guard<std::mutex> lock(entry.mutex_);
        fn(entry.bucket_);
    }

    StatsBucket collect_merged_bucket() const {
        StatsBucket merged;
        // Strict lock order:
        // 1) Hold StaticMeta via accessAllThreads(), locking each thread's entry.mutex_ one at a time.
        // 2) Release accessAllThreads() BEFORE locking retired_mutex_.
        {
            auto accessor = tls_.accessAllThreads();
            for (const auto& entry_ptr : accessor) {
                if (!entry_ptr) {
                    continue;
                }
                std::lock_guard<std::mutex> lock(entry_ptr->mutex_);
                merge_stats_bucket(merged, entry_ptr->bucket_);
            }
        }
        {
            std::lock_guard<std::mutex> lock(retired_->mutex);
            merge_stats_bucket(merged, retired_->stats);
        }
        return merged;
    }

    QueryStatsSnapshot snapshot() const {
        const auto merged = collect_merged_bucket();
        const auto now = std::chrono::steady_clock::now();
        double wall_ms = 0.0;
        {
            std::lock_guard<std::mutex> lock(retired_->mutex);
            wall_ms =
                    static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_).count()
                    ) /
                    1e6;
        }

        QueryStatsSnapshot snap;
        snap.wall_time_ms = wall_ms;
        snap.storage = materialize_storage_map(merged.storage);
        snap.http = materialize_http_map(merged.http);
        snap.api = materialize_api_map(merged.api);
        if (options_.by_symbol) {
            materialize_symbol_map(
                    merged.by_symbol, merged.dropped_symbols, options_.max_symbols, snap.by_symbol, snap.dropped_symbols
            );
        }
        if (options_.by_library) {
            for (const auto& [lib, lb] : merged.by_library) {
                LibraryStatsSnapshot lib_snap;
                lib_snap.storage = materialize_storage_map(lb.storage);
                lib_snap.http = materialize_http_map(lb.http);
                lib_snap.api = materialize_api_map(lb.api);
                if (options_.by_symbol) {
                    materialize_symbol_map(
                            lb.by_symbol,
                            lb.dropped_symbols,
                            options_.max_symbols,
                            lib_snap.by_symbol,
                            lib_snap.dropped_symbols
                    );
                }
                if (!lib_snap.storage.empty() || !lib_snap.http.empty() || !lib_snap.api.empty() ||
                    !lib_snap.by_symbol.empty()) {
                    snap.by_library.emplace(lib, std::move(lib_snap));
                }
            }
        }
        snap.thread_pools = merged.thread_pools;
        snap.caches = merged.caches;
        snap.codec = merged.codec;
        snap.query = merged.query;
        for (size_t i = 0; i < static_cast<size_t>(LockKind::COUNT); ++i) {
            if (!merged.locks[i].empty()) {
                snap.locks.emplace(static_cast<LockKind>(i), merged.locks[i]);
            }
        }
        for (const auto& [phase, st] : merged.boundary) {
            if (!st.empty()) {
                snap.boundary.emplace(static_cast<BoundaryPhase>(phase), st);
            }
        }
        for (const auto& [kind, st] : merged.clauses) {
            if (!st.empty()) {
                snap.processing.clauses.emplace(static_cast<ClauseKind>(kind), st);
            }
        }
        if (merged.admission.has_value()) {
            snap.processing.admission = *merged.admission;
        }
        if (merged.stream.has_value()) {
            const auto& sb = *merged.stream;
            snap.stream.data_segments_written = sb.data_segments_written;
            snap.stream.staged_segments_written = sb.staged_segments_written;
            snap.stream.small_segments_written = sb.small_segments_written;
            snap.stream.rows_per_segment = sb.rows_per_segment;
            snap.stream.incomplete_chain_loads = sb.incomplete_chain_loads;
            snap.stream.incomplete_chain_hops = sb.incomplete_chain_hops;
            snap.stream.incomplete_iteration_loads = sb.incomplete_iteration_loads;
            snap.stream.incomplete_iteration_segments = sb.incomplete_iteration_segments;
            for (const auto& [kind, st] : sb.compactions) {
                if (!st.empty()) {
                    snap.stream.compactions.emplace(static_cast<CompactionKind>(kind), st);
                }
            }
            for (size_t i = 0; i < sb.conflicts.size(); ++i) {
                if (sb.conflicts[i] > 0) {
                    snap.stream.conflicts.emplace(static_cast<WriteConflictKind>(i), sb.conflicts[i]);
                }
            }
        }
        snap.resources = sample_live_resources();
        snap.resources.lmdb.map_full_errors = merged.lmdb_map_full_errors;
        return snap;
    }

    QueryStats::QueryStatsOutput legacy_snapshot() const {
        const auto merged = collect_merged_bucket();
        QueryStats::QueryStatsOutput result;

        for (size_t task_idx = 0; task_idx < static_cast<size_t>(TaskType::END); ++task_idx) {
            const auto task_type = static_cast<TaskType>(task_idx);
            const std::string task_str = task_type_to_string(task_type);
            for (size_t key_idx = 0; key_idx < static_cast<size_t>(entity::KeyType::UNDEFINED); ++key_idx) {
                const auto& b = merged.legacy[task_idx][key_idx];
                if (b.count == 0) {
                    continue;
                }
                const std::string kt_str = key_type_name(static_cast<entity::KeyType>(key_idx));
                auto& op_out = result["storage_operations"][task_str][kt_str];
                op_out["count"] = b.count;
                op_out["size_bytes"] = b.size_bytes;
                op_out["total_time_ms"] = b.total_time_ns / 1000000ULL;
            }
        }
        return result;
    }

    void reset() {
        {
            auto accessor = tls_.accessAllThreads();
            for (auto& entry_ptr : accessor) {
                if (!entry_ptr) {
                    continue;
                }
                std::lock_guard<std::mutex> lock(entry_ptr->mutex_);
                entry_ptr->bucket_.reset();
            }
        }
        {
            std::lock_guard<std::mutex> lock(retired_->mutex);
            retired_->stats.reset();
            start_time_ = std::chrono::steady_clock::now();
        }
    }

    CollectorOptions options_;
    TimePoint start_time_;
    std::shared_ptr<RetiredState> retired_;
    // Declared last so it is destroyed first (after retired_->alive = false in ~Impl).
    folly::ThreadLocal<std::unique_ptr<ThreadLocalEntry>, QueryStatsTlsTag> tls_;
};

// ---------------------------------------------------------------------------
// RequestContext state & collector routing
// ---------------------------------------------------------------------------

namespace {

struct CollectorBinding {
    std::shared_ptr<QueryStatsCollector::Impl> collector;
    bool isolate{false};
};

struct RequestState {
    std::vector<CollectorBinding> collectors;
    std::string library;
    std::string symbol;
    std::optional<StorageOp> current_op;
    std::optional<entity::KeyType> current_key_type;
};

const folly::RequestToken& query_stats_token() {
    static const folly::RequestToken token{"arcticdb_query_stats"};
    return token;
}

const RequestState* current_request_state() noexcept {
    auto* ctx = folly::RequestContext::try_get();
    if (!ctx) {
        return nullptr;
    }
    const auto* data = ctx->getContextData(query_stats_token());
    if (!data) {
        return nullptr;
    }
    return &static_cast<const folly::ImmutableRequestData<RequestState>*>(data)->value();
}

std::shared_ptr<folly::RequestContext> push_request_state(RequestState new_state) {
    std::shared_ptr<folly::RequestContext> next;
    if (auto* cur = folly::RequestContext::try_get()) {
        next = folly::RequestContext::copyAsChild(*cur);
    } else {
        next = std::make_shared<folly::RequestContext>();
    }
    next->overwriteContextData(
            query_stats_token(),
            std::make_unique<folly::ImmutableRequestData<RequestState>>(std::move(new_state)),
            /*safe=*/true
    );
    return folly::RequestContext::setContext(std::move(next));
}

// Global collector holder so record_* does not pay std::call_once or shared_ptr refcount churn
// when global QueryStats is disabled.
std::atomic<QueryStatsCollector::Impl*> g_global_collector_raw{nullptr};
std::atomic<bool> g_global_enabled{false};

struct ActiveSinkNode {
    std::shared_ptr<StatsSink> sink;
};

struct alignas(64) SinkReaderSlot {
    std::mutex mutex;
};
struct SinkReaderTag {};

std::mutex g_sink_write_mutex;
std::atomic<ActiveSinkNode*> g_active_sink{nullptr};
folly::ThreadLocal<SinkReaderSlot, SinkReaderTag> g_sink_reader_slots;

template<typename Fn>
inline void dispatch_to_active_sink(Fn&& fn) {
    if (g_active_sink.load(std::memory_order_relaxed) == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> guard(g_sink_reader_slots->mutex);
    if (auto* node = g_active_sink.load(std::memory_order_acquire)) {
        fn(*node->sink);
    }
}

template<typename Fn>
void dispatch_to_active_collectors(Fn&& fn) {
    const auto* state = current_request_state();
    bool isolated = false;
    if (state != nullptr && !state->collectors.empty()) {
        for (auto it = state->collectors.rbegin(); it != state->collectors.rend(); ++it) {
            if (it->collector) {
                fn(*it->collector, state);
            }
            if (it->isolate) {
                isolated = true;
                break;
            }
        }
    }
    if (!isolated && g_global_enabled.load(std::memory_order_relaxed)) {
        if (auto* global_impl = g_global_collector_raw.load(std::memory_order_acquire)) {
            fn(*global_impl, state);
        }
    }
}

std::string_view resolve_library(std::string_view explicit_lib, const RequestState* state) noexcept {
    if (!explicit_lib.empty()) {
        return explicit_lib;
    }
    if (state != nullptr && !state->library.empty()) {
        return state->library;
    }
    return {};
}

std::string_view resolve_symbol(std::string_view explicit_sym, const RequestState* state) noexcept {
    if (!explicit_sym.empty()) {
        return explicit_sym;
    }
    if (state != nullptr && !state->symbol.empty()) {
        return state->symbol;
    }
    return {};
}

} // namespace

// ---------------------------------------------------------------------------
// String conversions
// ---------------------------------------------------------------------------

const char* to_string(StorageBackend backend) noexcept {
    switch (backend) {
    case StorageBackend::S3:
        return "s3";
    case StorageBackend::GcpXml:
        return "gcp_xml";
    case StorageBackend::NfsBacked:
        return "nfs_backed";
    case StorageBackend::Azure:
        return "azure";
    case StorageBackend::Lmdb:
        return "lmdb";
    case StorageBackend::Mongo:
        return "mongo";
    case StorageBackend::Memory:
        return "memory";
    case StorageBackend::MappedFile:
        return "mapped_file";
    case StorageBackend::Custom:
        return "custom";
    default:
        return "unknown";
    }
}

const char* to_string(StorageOp op) noexcept {
    switch (op) {
    case StorageOp::Read:
        return "read";
    case StorageOp::AsyncRead:
        return "async_read";
    case StorageOp::Write:
        return "write";
    case StorageOp::WriteIfNone:
        return "write_if_none";
    case StorageOp::WriteIfMatch:
        return "write_if_match";
    case StorageOp::Update:
        return "update";
    case StorageOp::Delete:
        return "delete";
    case StorageOp::DeleteBatch:
        return "delete_batch";
    case StorageOp::Exists:
        return "exists";
    case StorageOp::List:
        return "list";
    case StorageOp::Scan:
        return "scan";
    case StorageOp::VisitSizes:
        return "visit_sizes";
    case StorageOp::FastDelete:
        return "fast_delete";
    case StorageOp::VisitMetadata:
        return "visit_metadata";
    default:
        return "unknown";
    }
}

const char* to_string(StorageOutcome outcome) noexcept {
    switch (outcome) {
    case StorageOutcome::Ok:
        return "ok";
    case StorageOutcome::NotFound:
        return "not_found";
    case StorageOutcome::Conflict:
        return "conflict";
    case StorageOutcome::Unsupported:
        return "unsupported";
    case StorageOutcome::Error:
        return "error";
    default:
        return "unknown";
    }
}

const char* to_string(HttpStatusClass status_class) noexcept {
    switch (status_class) {
    case HttpStatusClass::Status2xx:
        return "2xx";
    case HttpStatusClass::Status3xx:
        return "3xx";
    case HttpStatusClass::Status4xx:
        return "4xx";
    case HttpStatusClass::Status5xx:
        return "5xx";
    case HttpStatusClass::NetworkError:
        return "network_error";
    default:
        return "unknown";
    }
}

const char* to_string(PoolKind pool) noexcept {
    switch (pool) {
    case PoolKind::Cpu:
        return "cpu";
    case PoolKind::Io:
        return "io";
    default:
        return "unknown";
    }
}

const char* to_string(LockKind lock) noexcept {
    switch (lock) {
    case LockKind::StorageLock:
        return "storage_lock";
    case LockKind::ReliableStorageLock:
        return "reliable_storage_lock";
    default:
        return "unknown";
    }
}

const char* to_string(ApiOp op) noexcept {
    switch (op) {
    case ApiOp::Read:
        return "read";
    case ApiOp::Write:
        return "write";
    case ApiOp::Append:
        return "append";
    case ApiOp::Update:
        return "update";
    case ApiOp::Delete:
        return "delete";
    case ApiOp::ReadBatch:
        return "read_batch";
    case ApiOp::WriteBatch:
        return "write_batch";
    case ApiOp::AppendBatch:
        return "append_batch";
    case ApiOp::UpdateBatch:
        return "update_batch";
    case ApiOp::DeleteBatch:
        return "delete_batch";
    case ApiOp::ListSymbols:
        return "list_symbols";
    case ApiOp::ListVersions:
        return "list_versions";
    case ApiOp::Snapshot:
        return "snapshot";
    case ApiOp::PrunePreviousVersions:
        return "prune_previous_versions";
    case ApiOp::Merge:
        return "merge";
    case ApiOp::Metadata:
        return "metadata";
    case ApiOp::Compact:
        return "compact";
    default:
        return "unknown";
    }
}

const char* to_string(BoundaryPhase phase) noexcept {
    switch (phase) {
    case BoundaryPhase::Normalize:
        return "normalize";
    case BoundaryPhase::Denormalize:
        return "denormalize";
    case BoundaryPhase::CppInput:
        return "cpp_input";
    case BoundaryPhase::CppOutput:
        return "cpp_output";
    case BoundaryPhase::GilWait:
        return "gil_wait";
    default:
        return "unknown";
    }
}

const char* to_string(ClauseKind kind) noexcept {
    switch (kind) {
    case ClauseKind::Passthrough:
        return "passthrough";
    case ClauseKind::Filter:
        return "filter";
    case ClauseKind::Project:
        return "project";
    case ClauseKind::GroupByPartition:
        return "groupby_partition";
    case ClauseKind::Aggregation:
        return "aggregation";
    case ClauseKind::Resample:
        return "resample";
    case ClauseKind::RemoveColumnPartitioning:
        return "remove_column_partitioning";
    case ClauseKind::Split:
        return "split";
    case ClauseKind::Sort:
        return "sort";
    case ClauseKind::Merge:
        return "merge";
    case ClauseKind::ColumnStats:
        return "column_stats";
    case ClauseKind::RowRange:
        return "row_range";
    case ClauseKind::DateRange:
        return "date_range";
    case ClauseKind::Concat:
        return "concat";
    case ClauseKind::Write:
        return "write";
    case ClauseKind::MergeUpdate:
        return "merge_update";
    case ClauseKind::CompactData:
        return "compact_data";
    case ClauseKind::RenameColumns:
        return "rename_columns";
    case ClauseKind::Other:
        return "other";
    default:
        return "unknown";
    }
}

const char* to_string(CompactionKind kind) noexcept {
    switch (kind) {
    case CompactionKind::Incomplete:
        return "incomplete";
    case CompactionKind::SortMerge:
        return "sort_merge";
    case CompactionKind::Defragment:
        return "defragment";
    case CompactionKind::SymbolList:
        return "symbol_list";
    default:
        return "unknown";
    }
}

const char* to_string(WriteConflictKind kind) noexcept {
    switch (kind) {
    case WriteConflictKind::NonIncreasingVersion:
        return "non_increasing_version";
    case WriteConflictKind::ConcurrentSymbolListCompaction:
        return "concurrent_symbol_list_compaction";
    case WriteConflictKind::SymbolListLoadRetry:
        return "symbol_list_load_retry";
    default:
        return "unknown";
    }
}

namespace {

const std::string& key_type_name_cached(entity::KeyType key_type) {
    static const auto table = []() {
        constexpr size_t N = static_cast<size_t>(entity::KeyType::UNDEFINED) + 1;
        std::array<std::string, N> names{};
        const std::string token = "::";
        for (size_t i = 0; i < N; ++i) {
            const auto kt = static_cast<entity::KeyType>(i);
            if (kt == entity::KeyType::UNDEFINED) {
                names[i] = "UNDEFINED";
            } else {
                std::string str = entity::get_key_description(kt);
                const auto pos = str.find(token);
                names[i] = (pos == std::string::npos) ? std::move(str) : str.substr(pos + token.size());
            }
        }
        return names;
    }();
    const auto idx = static_cast<size_t>(key_type);
    if (idx < table.size()) {
        return table[idx];
    }
    return table[static_cast<size_t>(entity::KeyType::UNDEFINED)];
}

} // namespace

std::string key_type_name(entity::KeyType key_type) { return key_type_name_cached(key_type); }

// ---------------------------------------------------------------------------
// LatencyHistogram methods
// ---------------------------------------------------------------------------

uint64_t LatencyHistogram::percentile_ns(double p) const noexcept {
    if (count == 0) {
        return 0;
    }
    if (p <= 0.0) {
        return min_ns;
    }
    if (p >= 1.0) {
        return max_ns;
    }
    const double target = p * static_cast<double>(count);
    uint64_t cum = 0;
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        const uint64_t c = buckets[i];
        if (c == 0) {
            continue;
        }
        const uint64_t prev = cum;
        cum += c;
        if (static_cast<double>(cum) >= target) {
            auto [lower, upper] = bucket_bounds(i);
            const uint64_t eff_low = std::max(lower, min_ns);
            const uint64_t eff_high = std::min(upper, max_ns);
            if (eff_high <= eff_low) {
                return eff_low;
            }
            const double frac = std::clamp((target - static_cast<double>(prev)) / static_cast<double>(c), 0.0, 1.0);
            const uint64_t est =
                    eff_low + static_cast<uint64_t>(std::llround(frac * static_cast<double>(eff_high - eff_low)));
            return std::clamp(est, min_ns, max_ns);
        }
    }
    return max_ns;
}

LatencyHistogram LatencyHistogram::diff(const LatencyHistogram& earlier) const noexcept {
    LatencyHistogram out;
    if (count <= earlier.count) {
        return out;
    }
    out.count = count - earlier.count;
    out.sum_ns = sum_ns >= earlier.sum_ns ? (sum_ns - earlier.sum_ns) : 0;
    bool found_any = false;
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        const uint64_t d = buckets[i] >= earlier.buckets[i] ? (buckets[i] - earlier.buckets[i]) : 0;
        out.buckets[i] = d;
        if (d > 0) {
            auto [lo, hi] = bucket_bounds(i);
            const uint64_t eff_lo = std::max(lo, min_ns);
            const uint64_t eff_hi = std::min(hi, max_ns);
            if (!found_any) {
                out.min_ns = eff_lo;
                found_any = true;
            }
            out.max_ns = std::max(out.max_ns, eff_hi);
        }
    }
    if (!found_any) {
        out.reset();
    } else if (out.min_ns > out.max_ns) {
        out.min_ns = out.max_ns;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Struct merge methods
// ---------------------------------------------------------------------------

void StorageOpStats::merge(const StorageOpStats& other) noexcept {
    ok += other.ok;
    not_found += other.not_found;
    conflict += other.conflict;
    unsupported += other.unsupported;
    error += other.error;
    bytes += other.bytes;
    items += other.items;
    pages += other.pages;
    partial_delete_retries += other.partial_delete_retries;
    latency.merge(other.latency);
    error_latency.merge(other.error_latency);
}

void HttpOpStats::merge(const HttpOpStats& other) noexcept {
    requests += other.requests;
    attempts += other.attempts;
    retries += other.retries;
    status_2xx += other.status_2xx;
    status_3xx += other.status_3xx;
    status_4xx += other.status_4xx;
    status_5xx += other.status_5xx;
    throttled += other.throttled;
    network_errors += other.network_errors;
    checksum_mismatch_retries += other.checksum_mismatch_retries;
    new_connections += other.new_connections;
    backoff_time_ns += other.backoff_time_ns;
    request_latency.merge(other.request_latency);
    attempt_latency.merge(other.attempt_latency);
    dns_latency.merge(other.dns_latency);
    tcp_connect_latency.merge(other.tcp_connect_latency);
    tls_handshake_latency.merge(other.tls_handshake_latency);
    ttfb_latency.merge(other.ttfb_latency);
    transfer_latency.merge(other.transfer_latency);
}

void SymbolStats::merge(const SymbolStats& other) noexcept {
    reads += other.reads;
    writes += other.writes;
    deletes += other.deletes;
    exists += other.exists;
    lists += other.lists;
    errors += other.errors;
    bytes_read += other.bytes_read;
    bytes_written += other.bytes_written;
    http_requests += other.http_requests;
    http_retries += other.http_retries;
    storage_latency.merge(other.storage_latency);
}

void ThreadPoolStats::merge(const ThreadPoolStats& other) noexcept {
    tasks += other.tasks;
    expired += other.expired;
    wait_latency.merge(other.wait_latency);
    run_latency.merge(other.run_latency);
}

void ThreadPoolStats::reset() noexcept {
    tasks = 0;
    expired = 0;
    wait_latency.reset();
    run_latency.reset();
}

void ThreadPoolsStats::merge(const ThreadPoolsStats& other) noexcept {
    cpu.merge(other.cpu);
    io.merge(other.io);
    semaphore_waits += other.semaphore_waits;
    semaphore_wait_latency.merge(other.semaphore_wait_latency);
}

void ThreadPoolsStats::reset() noexcept {
    cpu.reset();
    io.reset();
    semaphore_waits = 0;
    semaphore_wait_latency.reset();
}

void VersionMapCacheStats::merge(const VersionMapCacheStats& other) noexcept {
    hits += other.hits;
    misses += other.misses;
    reloads += other.reloads;
    ref_retries += other.ref_retries;
    chain_hops += other.chain_hops;
}

void DeDupCacheStats::merge(const DeDupCacheStats& other) noexcept {
    hits += other.hits;
    misses += other.misses;
    bytes_saved += other.bytes_saved;
}

void ConfigCacheStats::merge(const ConfigCacheStats& other) noexcept {
    hits += other.hits;
    misses += other.misses;
}

void CacheStats::merge(const CacheStats& other) noexcept {
    version_map.merge(other.version_map);
    dedup_map.merge(other.dedup_map);
    config_cache.merge(other.config_cache);
}

void CacheStats::reset() noexcept {
    version_map.reset();
    dedup_map.reset();
    config_cache.reset();
}

void CodecStats::merge(const CodecStats& other) noexcept {
    encodes += other.encodes;
    decodes += other.decodes;
    encode_uncompressed_bytes += other.encode_uncompressed_bytes;
    encode_compressed_bytes += other.encode_compressed_bytes;
    decode_compressed_bytes += other.decode_compressed_bytes;
    decode_uncompressed_bytes += other.decode_uncompressed_bytes;
    encode_latency.merge(other.encode_latency);
    decode_latency.merge(other.decode_latency);
}

void CodecStats::reset() noexcept {
    encodes = 0;
    decodes = 0;
    encode_uncompressed_bytes = 0;
    encode_compressed_bytes = 0;
    decode_compressed_bytes = 0;
    decode_uncompressed_bytes = 0;
    encode_latency.reset();
    decode_latency.reset();
}

void QueryPruningStats::merge(const QueryPruningStats& other) noexcept {
    queries += other.queries;
    segments_total += other.segments_total;
    segments_pruned_index += other.segments_pruned_index;
    segments_pruned_column_stats += other.segments_pruned_column_stats;
    segments_read += other.segments_read;
    columns_total += other.columns_total;
    columns_projected += other.columns_projected;
    rows_total += other.rows_total;
    rows_selected += other.rows_selected;
}

void LockStats::merge(const LockStats& other) noexcept {
    acquires += other.acquires;
    conflicts += other.conflicts;
    retries += other.retries;
    timeouts += other.timeouts;
    wait_latency.merge(other.wait_latency);
}

void LockStats::reset() noexcept {
    acquires = 0;
    conflicts = 0;
    retries = 0;
    timeouts = 0;
    wait_latency.reset();
}

void ApiOpStats::merge(const ApiOpStats& other) noexcept {
    ok += other.ok;
    error += other.error;
    latency.merge(other.latency);
    error_latency.merge(other.error_latency);
}

void BoundaryPhaseStats::merge(const BoundaryPhaseStats& other) noexcept {
    count += other.count;
    latency.merge(other.latency);
}

void ClauseExecStats::merge(const ClauseExecStats& other) noexcept {
    executions += other.executions;
    rows_in += other.rows_in;
    rows_out += other.rows_out;
    latency.merge(other.latency);
}

void AdmissionStats::merge(const AdmissionStats& other) noexcept {
    units_admitted += other.units_admitted;
    residency_waits += other.residency_waits;
    segments_dispatched += other.segments_dispatched;
    read_window_waits += other.read_window_waits;
    residency_wait_latency.merge(other.residency_wait_latency);
    read_window_wait_latency.merge(other.read_window_wait_latency);
}

void AdmissionStats::reset() noexcept {
    units_admitted = 0;
    residency_waits = 0;
    segments_dispatched = 0;
    read_window_waits = 0;
    residency_wait_latency.reset();
    read_window_wait_latency.reset();
}

uint64_t RowCountHistogram::percentile(double p) const noexcept {
    if (count == 0) {
        return 0;
    }
    if (p <= 0.0) {
        return min;
    }
    if (p >= 1.0) {
        return max;
    }
    const double target = p * static_cast<double>(count);
    uint64_t cum = 0;
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        const uint64_t c = buckets[i];
        if (c == 0) {
            continue;
        }
        const uint64_t prev = cum;
        cum += c;
        if (static_cast<double>(cum) >= target) {
            const uint64_t lower = (i == 0) ? 0ULL : (BOUNDS[i - 1] + 1ULL);
            const uint64_t upper = (i < BOUNDS.size()) ? BOUNDS[i] : max;
            const uint64_t eff_low = std::max(lower, min);
            const uint64_t eff_high = std::min(upper, max);
            if (eff_high <= eff_low) {
                return eff_low;
            }
            const double frac = std::clamp((target - static_cast<double>(prev)) / static_cast<double>(c), 0.0, 1.0);
            const uint64_t est =
                    eff_low + static_cast<uint64_t>(std::llround(frac * static_cast<double>(eff_high - eff_low)));
            return std::clamp(est, min, max);
        }
    }
    return max;
}

RowCountHistogram RowCountHistogram::diff(const RowCountHistogram& earlier) const noexcept {
    RowCountHistogram out;
    if (count <= earlier.count) {
        return out;
    }
    out.count = count - earlier.count;
    out.sum = sum >= earlier.sum ? (sum - earlier.sum) : 0;
    bool found_any = false;
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
        const uint64_t d = buckets[i] >= earlier.buckets[i] ? (buckets[i] - earlier.buckets[i]) : 0;
        out.buckets[i] = d;
        if (d > 0) {
            const uint64_t lo = (i == 0) ? 0ULL : (BOUNDS[i - 1] + 1ULL);
            const uint64_t hi = (i < BOUNDS.size()) ? BOUNDS[i] : max;
            const uint64_t eff_lo = std::max(lo, min);
            const uint64_t eff_hi = std::min(hi, max);
            if (!found_any) {
                out.min = eff_lo;
                found_any = true;
            }
            out.max = std::max(out.max, eff_hi);
        }
    }
    if (!found_any) {
        out.reset();
    } else if (out.min > out.max) {
        out.min = out.max;
    }
    return out;
}

void CompactionStats::merge(const CompactionStats& other) noexcept {
    count += other.count;
    input_segments += other.input_segments;
    output_segments += other.output_segments;
    latency.merge(other.latency);
}

void CompactionStats::reset() noexcept {
    count = 0;
    input_segments = 0;
    output_segments = 0;
    latency.reset();
}

void StreamWriteStats::merge(const StreamWriteStats& other) {
    data_segments_written += other.data_segments_written;
    staged_segments_written += other.staged_segments_written;
    small_segments_written += other.small_segments_written;
    rows_per_segment.merge(other.rows_per_segment);
    incomplete_chain_loads += other.incomplete_chain_loads;
    incomplete_chain_hops += other.incomplete_chain_hops;
    incomplete_iteration_loads += other.incomplete_iteration_loads;
    incomplete_iteration_segments += other.incomplete_iteration_segments;
    for (const auto& [k, v] : other.compactions) {
        compactions[k].merge(v);
    }
    for (const auto& [k, v] : other.conflicts) {
        conflicts[k] += v;
    }
}

void StreamWriteStats::reset() {
    data_segments_written = 0;
    staged_segments_written = 0;
    small_segments_written = 0;
    rows_per_segment.reset();
    incomplete_chain_loads = 0;
    incomplete_chain_hops = 0;
    incomplete_iteration_loads = 0;
    incomplete_iteration_segments = 0;
    compactions.clear();
    conflicts.clear();
}

// ---------------------------------------------------------------------------
// QueryStatsCollector & CollectorSession
// ---------------------------------------------------------------------------

QueryStatsCollector::QueryStatsCollector(CollectorOptions options) : impl_(std::make_shared<Impl>(options)) {}

QueryStatsCollector::~QueryStatsCollector() = default;

QueryStatsSnapshot QueryStatsCollector::snapshot() const { return impl_->snapshot(); }

void QueryStatsCollector::reset() { impl_->reset(); }

const CollectorOptions& QueryStatsCollector::options() const noexcept { return impl_->options_; }

CollectorSession::CollectorSession(std::shared_ptr<QueryStatsCollector> collector, bool auto_enter) :
    collector_(std::move(collector)) {
    util::check(collector_ != nullptr, "CollectorSession requires a non-null QueryStatsCollector");
    if (auto_enter) {
        enter();
    }
}

CollectorSession::CollectorSession(CollectorOptions options, bool auto_enter) :
    CollectorSession(std::make_shared<QueryStatsCollector>(options), auto_enter) {}

CollectorSession::~CollectorSession() {
    if (entered_) {
        exit();
    }
}

void CollectorSession::enter() {
    if (entered_) {
        return;
    }
    RequestState new_state;
    if (const auto* cur = current_request_state()) {
        new_state = *cur;
    }
    new_state.collectors.push_back(CollectorBinding{collector_->impl_, collector_->options().isolate});
    prev_ctx_ = push_request_state(std::move(new_state));
    detail::active_collector_count.fetch_add(1, std::memory_order_relaxed);
    entered_ = true;
}

void CollectorSession::exit() {
    if (!entered_) {
        return;
    }
    entered_ = false;
    detail::active_collector_count.fetch_sub(1, std::memory_order_relaxed);
    folly::RequestContext::setContext(std::static_pointer_cast<folly::RequestContext>(std::move(prev_ctx_)));
}

QueryStatsSnapshot CollectorSession::snapshot() const { return collector_->snapshot(); }

// ---------------------------------------------------------------------------
// ContextTagScope, CapturedContext, RestoredContextGuard
// ---------------------------------------------------------------------------

ContextTagScope::ContextTagScope(
        std::string_view library, std::string_view symbol, std::optional<StorageOp> op,
        std::optional<entity::KeyType> key_type
) {
    if (!is_active()) {
        return;
    }
    RequestState new_state;
    if (const auto* cur = current_request_state()) {
        new_state = *cur;
    }
    if (!library.empty()) {
        new_state.library = std::string(library);
    }
    if (!symbol.empty()) {
        new_state.symbol = std::string(symbol);
    }
    if (op.has_value()) {
        new_state.current_op = op;
    }
    if (key_type.has_value()) {
        new_state.current_key_type = key_type;
    }
    prev_ctx_ = push_request_state(std::move(new_state));
    active_ = true;
}

ContextTagScope::~ContextTagScope() {
    if (active_) {
        folly::RequestContext::setContext(std::static_pointer_cast<folly::RequestContext>(std::move(prev_ctx_)));
    }
}

CapturedContext capture_context() {
    if (!is_active()) {
        return {};
    }
    return CapturedContext{folly::RequestContext::saveContext()};
}

RestoredContextGuard::RestoredContextGuard(const CapturedContext& ctx) {
    if (ctx.ctx_) {
        prev_ = folly::RequestContext::setContext(std::static_pointer_cast<folly::RequestContext>(ctx.ctx_));
        active_ = true;
    }
}

RestoredContextGuard::~RestoredContextGuard() {
    if (active_) {
        folly::RequestContext::setContext(std::static_pointer_cast<folly::RequestContext>(std::move(prev_)));
    }
}

// ---------------------------------------------------------------------------
// Recording functions
// ---------------------------------------------------------------------------

void record_storage_op(
        StorageBackend backend, StorageOp op, entity::KeyType key_type, StorageOutcome outcome, uint64_t duration_ns,
        uint64_t bytes, uint64_t items, std::string_view library, std::string_view symbol
) {
    if (!is_active()) {
        return;
    }
    const uint16_t packed = pack_backend_op(backend, op);
    const std::string& kt_str = key_type_name_cached(key_type);
    const bool is_s3_list = (backend == StorageBackend::S3 || backend == StorageBackend::NfsBacked ||
                             backend == StorageBackend::GcpXml) &&
                            (op == StorageOp::List || op == StorageOp::Scan || op == StorageOp::VisitSizes ||
                             op == StorageOp::VisitMetadata);
    const auto legacy_task = is_s3_list ? std::nullopt : to_legacy_task_type(backend, op);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library(library, state);
        const auto eff_sym = resolve_symbol(symbol, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            apply_storage_op_to_stats(b.storage[packed][kt_str], outcome, duration_ns, bytes, items);

            if (opts.by_library && !eff_lib.empty()) {
                auto& lb = b.by_library[std::string(eff_lib)];
                apply_storage_op_to_stats(lb.storage[packed][kt_str], outcome, duration_ns, bytes, items);
                if (opts.by_symbol && !eff_sym.empty()) {
                    auto& lsym = find_or_insert_symbol(lb.by_symbol, lb.dropped_symbols, eff_sym, opts.max_symbols);
                    apply_storage_op_to_symbol(lsym, op, outcome, duration_ns, bytes);
                }
            }

            if (opts.by_symbol && !eff_sym.empty()) {
                auto& gsym = find_or_insert_symbol(b.by_symbol, b.dropped_symbols, eff_sym, opts.max_symbols);
                apply_storage_op_to_symbol(gsym, op, outcome, duration_ns, bytes);
            }

            if (legacy_task.has_value() && key_type != entity::KeyType::UNDEFINED) {
                auto& leg = b.legacy[static_cast<size_t>(*legacy_task)][static_cast<size_t>(key_type)];
                leg.count += 1;
                leg.total_time_ns += duration_ns;
                leg.size_bytes += bytes;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library(library, current_request_state());
        sink.on_storage_op(backend, op, kt_str, outcome, duration_ns, bytes, items, eff_lib);
    });
}

void record_storage_batch_delete(
        StorageBackend backend, std::span<const BatchDeleteKeyInfo> keys, StorageOutcome outcome, uint64_t duration_ns,
        uint64_t bytes, std::string_view library, std::string_view fallback_symbol
) {
    if (!is_active() || keys.empty()) {
        return;
    }

    std::array<bool, static_cast<size_t>(entity::KeyType::UNDEFINED)> present{};
    size_t distinct_count = 0;
    entity::KeyType single_kt = entity::KeyType::UNDEFINED;
    for (const auto& info : keys) {
        const auto idx = static_cast<size_t>(info.key_type);
        if (idx < present.size() && !present[idx]) {
            present[idx] = true;
            single_kt = info.key_type;
            ++distinct_count;
        }
    }

    const std::string kt_str = distinct_count == 1 ? key_type_name(single_kt) : std::string(MIXED_KEY_TYPE_LABEL);
    const uint16_t packed = pack_backend_op(backend, StorageOp::DeleteBatch);
    const uint64_t items = static_cast<uint64_t>(keys.size());
    const bool is_s3_family =
            backend == StorageBackend::S3 || backend == StorageBackend::NfsBacked || backend == StorageBackend::GcpXml;

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library(library, state);
        const auto eff_fallback_sym = resolve_symbol(fallback_symbol, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            apply_storage_op_to_stats(b.storage[packed][kt_str], outcome, duration_ns, bytes, items);

            LibraryBucket* lb_ptr = nullptr;
            if (opts.by_library && !eff_lib.empty()) {
                lb_ptr = &b.by_library[std::string(eff_lib)];
                apply_storage_op_to_stats(lb_ptr->storage[packed][kt_str], outcome, duration_ns, bytes, items);
            }

            if (opts.by_symbol) {
                ankerl::unordered_dense::set<std::string_view> distinct_syms;
                for (const auto& info : keys) {
                    const std::string_view sym = !info.symbol.empty() ? info.symbol : eff_fallback_sym;
                    if (!sym.empty()) {
                        distinct_syms.insert(sym);
                    }
                }
                if (!distinct_syms.empty()) {
                    const uint64_t share_ns = duration_ns / static_cast<uint64_t>(distinct_syms.size());
                    for (const auto sym : distinct_syms) {
                        auto& gsym = find_or_insert_symbol(b.by_symbol, b.dropped_symbols, sym, opts.max_symbols);
                        apply_storage_op_to_symbol(gsym, StorageOp::DeleteBatch, outcome, share_ns, 0);
                        if (lb_ptr != nullptr) {
                            auto& lsym = find_or_insert_symbol(
                                    lb_ptr->by_symbol, lb_ptr->dropped_symbols, sym, opts.max_symbols
                            );
                            apply_storage_op_to_symbol(lsym, StorageOp::DeleteBatch, outcome, share_ns, 0);
                        }
                    }
                }
            }

            // Legacy S3_DeleteObjects counts once per distinct key type in the batch
            // (matches historical behavior verified by test_bulk_delete.py & test_query_stats.py).
            if (is_s3_family) {
                for (size_t i = 0; i < present.size(); ++i) {
                    if (present[i]) {
                        auto& leg = b.legacy[static_cast<size_t>(TaskType::S3_DeleteObjects)][i];
                        leg.count += 1;
                        leg.total_time_ns += duration_ns;
                    }
                }
            } else if (backend == StorageBackend::Memory) {
                // MemoryStorage::do_remove already records per-key counts and deleted bytes via
                // record_legacy_memory_delete; distribute elapsed duration across present key types.
                if (distinct_count > 0) {
                    const uint64_t share_ns = duration_ns / static_cast<uint64_t>(distinct_count);
                    for (size_t i = 0; i < present.size(); ++i) {
                        if (present[i]) {
                            b.legacy[static_cast<size_t>(TaskType::Memory_DeleteObject)][i].total_time_ns += share_ns;
                        }
                    }
                }
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library(library, current_request_state());
        sink.on_storage_op(backend, StorageOp::DeleteBatch, kt_str, outcome, duration_ns, bytes, items, eff_lib);
    });
}

void record_storage_page(
        StorageBackend backend, StorageOp op, entity::KeyType key_type, uint64_t duration_ns, uint64_t items,
        std::string_view library, std::string_view symbol
) {
    if (!is_active()) {
        return;
    }
    const std::string& kt_str = key_type_name_cached(key_type);
    const bool is_s3_family =
            backend == StorageBackend::S3 || backend == StorageBackend::NfsBacked || backend == StorageBackend::GcpXml;
    (void)items;
    (void)symbol;

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const StorageOp eff_op = (state != nullptr && state->current_op.has_value()) ? *state->current_op : op;
        const uint16_t packed = pack_backend_op(backend, eff_op);
        const auto eff_lib = resolve_library(library, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            b.storage[packed][kt_str].pages += 1;

            if (opts.by_library && !eff_lib.empty()) {
                b.by_library[std::string(eff_lib)].storage[packed][kt_str].pages += 1;
            }

            if (is_s3_family && key_type != entity::KeyType::UNDEFINED) {
                auto& leg = b.legacy[static_cast<size_t>(TaskType::S3_ListObjectsV2)][static_cast<size_t>(key_type)];
                leg.count += 1;
                leg.total_time_ns += duration_ns;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto* state = current_request_state();
        const StorageOp eff_op = (state != nullptr && state->current_op.has_value()) ? *state->current_op : op;
        const auto eff_lib = resolve_library(library, state);
        sink.on_storage_page(backend, eff_op, eff_lib);
    });
}

namespace {
thread_local ScopedDeletedBytesTracker* tl_deleted_bytes_tracker = nullptr;
} // namespace

ScopedDeletedBytesTracker::ScopedDeletedBytesTracker() noexcept : prev_(tl_deleted_bytes_tracker) {
    tl_deleted_bytes_tracker = this;
}

ScopedDeletedBytesTracker::~ScopedDeletedBytesTracker() noexcept { tl_deleted_bytes_tracker = prev_; }

void note_deleted_bytes(uint64_t bytes) noexcept {
    if (tl_deleted_bytes_tracker != nullptr) {
        tl_deleted_bytes_tracker->bytes_ += bytes;
    }
}

void record_partial_delete_retry(
        StorageBackend backend, std::string_view key_type_label, std::string_view library, std::string_view symbol
) {
    if (!is_active()) {
        return;
    }
    const uint16_t packed = pack_backend_op(backend, StorageOp::DeleteBatch);
    const std::string kt_str(key_type_label);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library(library, state);
        const auto& opts = collector.options_;
        (void)symbol;

        collector.mutate_local([&](StatsBucket& b) {
            b.storage[packed][kt_str].partial_delete_retries += 1;
            if (opts.by_library && !eff_lib.empty()) {
                b.by_library[std::string(eff_lib)].storage[packed][kt_str].partial_delete_retries += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library(library, current_request_state());
        sink.on_partial_delete_retry(backend, eff_lib);
    });
}

void record_legacy_memory_delete(entity::KeyType key_type, uint64_t bytes) {
    if (!is_active() || key_type == entity::KeyType::UNDEFINED) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& leg = b.legacy[static_cast<size_t>(TaskType::Memory_DeleteObject)][static_cast<size_t>(key_type)];
            leg.count += 1;
            leg.size_bytes += bytes;
        });
    });
}

void record_http_attempt(
        std::string_view service, std::string_view operation, HttpStatusClass status_class,
        uint64_t attempt_duration_ns, uint64_t new_connections, uint64_t backoff_ns, bool throttled,
        const HttpAttemptTimings& timings
) {
    if (!is_active()) {
        return;
    }
    const std::string svc = normalize_service_name(service);
    const std::string op(operation);

    auto apply_attempt = [&](HttpOpStats& st) {
        st.attempts += 1;
        st.new_connections += new_connections;
        st.backoff_time_ns += backoff_ns;
        st.attempt_latency.record(attempt_duration_ns);
        if (throttled) {
            st.throttled += 1;
        }
        if (timings.has_timings) {
            if (timings.dns_ns > 0) {
                st.dns_latency.record(timings.dns_ns);
            }
            if (timings.tcp_connect_ns > 0) {
                st.tcp_connect_latency.record(timings.tcp_connect_ns);
            }
            if (timings.tls_handshake_ns > 0) {
                st.tls_handshake_latency.record(timings.tls_handshake_ns);
            }
            st.ttfb_latency.record(timings.ttfb_ns);
            st.transfer_latency.record(timings.transfer_ns);
        }
        switch (status_class) {
        case HttpStatusClass::Status2xx:
            ++st.status_2xx;
            break;
        case HttpStatusClass::Status3xx:
            ++st.status_3xx;
            break;
        case HttpStatusClass::Status4xx:
            ++st.status_4xx;
            break;
        case HttpStatusClass::Status5xx:
            ++st.status_5xx;
            break;
        case HttpStatusClass::NetworkError:
            ++st.network_errors;
            break;
        default:
            break;
        }
    };

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library({}, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            apply_attempt(b.http[svc][op]);
            if (opts.by_library && !eff_lib.empty()) {
                apply_attempt(b.by_library[std::string(eff_lib)].http[svc][op]);
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library({}, current_request_state());
        sink.on_http_attempt(svc, op, status_class, attempt_duration_ns, new_connections, eff_lib);
        if (backoff_ns > 0) {
            sink.on_http_backoff(svc, op, backoff_ns, eff_lib);
        }
        if (throttled) {
            sink.on_http_throttled(svc, op, eff_lib);
        }
        if (timings.has_timings) {
            sink.on_http_phase_timings(svc, op, timings, eff_lib);
        }
    });
}

void record_http_retry(std::string_view service, std::string_view operation, uint64_t backoff_ns) {
    if (!is_active()) {
        return;
    }
    const std::string svc = normalize_service_name(service);
    const std::string op(operation);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library({}, state);
        const auto eff_sym = resolve_symbol({}, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            auto& st = b.http[svc][op];
            st.retries += 1;
            st.backoff_time_ns += backoff_ns;

            if (opts.by_library && !eff_lib.empty()) {
                auto& lb = b.by_library[std::string(eff_lib)];
                auto& lst = lb.http[svc][op];
                lst.retries += 1;
                lst.backoff_time_ns += backoff_ns;
                if (opts.by_symbol && !eff_sym.empty()) {
                    find_or_insert_symbol(lb.by_symbol, lb.dropped_symbols, eff_sym, opts.max_symbols).http_retries +=
                            1;
                }
            }
            if (opts.by_symbol && !eff_sym.empty()) {
                find_or_insert_symbol(b.by_symbol, b.dropped_symbols, eff_sym, opts.max_symbols).http_retries += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library({}, current_request_state());
        sink.on_http_retry(svc, op, backoff_ns, eff_lib);
    });
}

void record_http_request_finish(
        std::string_view service, std::string_view operation, uint64_t total_request_duration_ns,
        uint64_t extra_backoff_ns
) {
    if (!is_active()) {
        return;
    }
    const std::string svc = normalize_service_name(service);
    const std::string op(operation);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library({}, state);
        const auto eff_sym = resolve_symbol({}, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            auto& st = b.http[svc][op];
            st.requests += 1;
            st.backoff_time_ns += extra_backoff_ns;
            st.request_latency.record(total_request_duration_ns);

            if (opts.by_library && !eff_lib.empty()) {
                auto& lb = b.by_library[std::string(eff_lib)];
                auto& lst = lb.http[svc][op];
                lst.requests += 1;
                lst.backoff_time_ns += extra_backoff_ns;
                lst.request_latency.record(total_request_duration_ns);
                if (opts.by_symbol && !eff_sym.empty()) {
                    find_or_insert_symbol(lb.by_symbol, lb.dropped_symbols, eff_sym, opts.max_symbols).http_requests +=
                            1;
                }
            }
            if (opts.by_symbol && !eff_sym.empty()) {
                find_or_insert_symbol(b.by_symbol, b.dropped_symbols, eff_sym, opts.max_symbols).http_requests += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library({}, current_request_state());
        sink.on_http_request_finish(svc, op, total_request_duration_ns, eff_lib);
        if (extra_backoff_ns > 0) {
            sink.on_http_backoff(svc, op, extra_backoff_ns, eff_lib);
        }
    });
}

void record_checksum_mismatch_retry(std::string_view service, std::string_view operation) {
    if (!is_active()) {
        return;
    }
    const std::string svc = normalize_service_name(service);
    const std::string op(operation);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library({}, state);
        const auto& opts = collector.options_;

        collector.mutate_local([&](StatsBucket& b) {
            b.http[svc][op].checksum_mismatch_retries += 1;
            if (opts.by_library && !eff_lib.empty()) {
                b.by_library[std::string(eff_lib)].http[svc][op].checksum_mismatch_retries += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library({}, current_request_state());
        sink.on_http_checksum_mismatch_retry(svc, op, eff_lib);
    });
}

void record_pool_task(PoolKind pool, uint64_t wait_ns, uint64_t run_ns) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& target = (pool == PoolKind::Cpu) ? b.thread_pools.cpu : b.thread_pools.io;
            target.tasks += 1;
            target.wait_latency.record(wait_ns);
            target.run_latency.record(run_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_pool_task(pool, wait_ns, run_ns); });
}

void record_pool_task_expired(PoolKind pool) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& target = (pool == PoolKind::Cpu) ? b.thread_pools.cpu : b.thread_pools.io;
            target.expired += 1;
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_pool_task_expired(pool); });
}

void record_semaphore_wait(uint64_t wait_ns) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            b.thread_pools.semaphore_waits += 1;
            b.thread_pools.semaphore_wait_latency.record(wait_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_semaphore_wait(wait_ns); });
}

void record_version_map_cache(bool hit, bool is_reload) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (hit) {
                b.caches.version_map.hits += 1;
            } else {
                b.caches.version_map.misses += 1;
            }
            if (is_reload) {
                b.caches.version_map.reloads += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_version_map_cache(hit, is_reload); });
}

void record_version_map_chain_hops(uint64_t hops) {
    if (!is_active() || hops == 0) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) { b.caches.version_map.chain_hops += hops; });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_version_map_chain_hops(hops); });
}

void record_version_map_ref_retry() {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) { b.caches.version_map.ref_retries += 1; });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_version_map_ref_retry(); });
}

void record_dedup_lookup(bool hit, uint64_t bytes_saved) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (hit) {
                b.caches.dedup_map.hits += 1;
                b.caches.dedup_map.bytes_saved += bytes_saved;
            } else {
                b.caches.dedup_map.misses += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_dedup_lookup(hit, bytes_saved); });
}

void record_config_cache(bool hit) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (hit) {
                b.caches.config_cache.hits += 1;
            } else {
                b.caches.config_cache.misses += 1;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_config_cache(hit); });
}

void record_codec_encode(uint64_t duration_ns, uint64_t uncompressed_bytes, uint64_t compressed_bytes) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            b.codec.encodes += 1;
            b.codec.encode_uncompressed_bytes += uncompressed_bytes;
            b.codec.encode_compressed_bytes += compressed_bytes;
            b.codec.encode_latency.record(duration_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        sink.on_codec_encode(duration_ns, uncompressed_bytes, compressed_bytes);
    });
}

void record_codec_decode(uint64_t duration_ns, uint64_t compressed_bytes, uint64_t uncompressed_bytes) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            b.codec.decodes += 1;
            b.codec.decode_compressed_bytes += compressed_bytes;
            b.codec.decode_uncompressed_bytes += uncompressed_bytes;
            b.codec.decode_latency.record(duration_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        sink.on_codec_decode(duration_ns, compressed_bytes, uncompressed_bytes);
    });
}

void record_query_pruning(
        uint64_t segments_total, uint64_t segments_pruned_index, uint64_t segments_pruned_column_stats,
        uint64_t segments_read, uint64_t columns_total, uint64_t columns_projected, uint64_t rows_total,
        uint64_t rows_selected
) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            b.query.queries += 1;
            b.query.segments_total += segments_total;
            b.query.segments_pruned_index += segments_pruned_index;
            b.query.segments_pruned_column_stats += segments_pruned_column_stats;
            b.query.segments_read += segments_read;
            b.query.columns_total += columns_total;
            b.query.columns_projected += columns_projected;
            b.query.rows_total += rows_total;
            b.query.rows_selected += rows_selected;
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        sink.on_query_pruning(
                segments_total,
                segments_pruned_index,
                segments_pruned_column_stats,
                segments_read,
                columns_total,
                columns_projected,
                rows_total,
                rows_selected
        );
    });
}

void record_lock_event(
        LockKind kind, bool acquired, uint64_t conflicts, uint64_t retries, bool timed_out, uint64_t wait_ns
) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(kind);
    if (idx >= static_cast<size_t>(LockKind::COUNT)) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& lk = b.locks[idx];
            if (acquired) {
                lk.acquires += 1;
            }
            lk.conflicts += conflicts;
            lk.retries += retries;
            if (timed_out) {
                lk.timeouts += 1;
            }
            lk.wait_latency.record(wait_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        sink.on_lock_event(kind, acquired, conflicts, retries, timed_out, wait_ns);
    });
}

namespace {
thread_local bool tl_in_api_op = false;
} // namespace

void record_api_op(ApiOp op, bool ok, uint64_t duration_ns, std::string_view library) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(op);
    if (idx >= static_cast<size_t>(ApiOp::COUNT)) {
        return;
    }
    const auto op_key = static_cast<uint8_t>(op);
    auto apply = [&](ApiOpStats& st) {
        if (ok) {
            ++st.ok;
        } else {
            ++st.error;
            st.error_latency.record(duration_ns);
        }
        st.latency.record(duration_ns);
    };

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState* state) {
        const auto eff_lib = resolve_library(library, state);
        const auto& opts = collector.options_;
        collector.mutate_local([&](StatsBucket& b) {
            apply(b.api[op_key]);
            if (opts.by_library && !eff_lib.empty()) {
                apply(b.by_library[std::string(eff_lib)].api[op_key]);
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        const auto eff_lib = resolve_library(library, current_request_state());
        sink.on_api_op(op, ok, duration_ns, eff_lib);
    });
}

void ApiOpScope::enter() noexcept {
    if (tl_in_api_op) {
        return;
    }
    tl_in_api_op = true;
    active_ = true;
    uncaught_on_enter_ = std::uncaught_exceptions();
    start_ = std::chrono::steady_clock::now();
}

void ApiOpScope::exit() noexcept {
    tl_in_api_op = false;
    active_ = false;
    const auto end = std::chrono::steady_clock::now();
    const uint64_t duration_ns = static_cast<uint64_t>(
            std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count())
    );
    const bool ok = !forced_error_ && (std::uncaught_exceptions() == uncaught_on_enter_);
    try {
        record_api_op(op_, ok, duration_ns, library_);
    } catch (...) {
    }
}

void record_boundary_op(BoundaryPhase phase, uint64_t duration_ns) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(phase);
    if (idx >= static_cast<size_t>(BoundaryPhase::COUNT)) {
        return;
    }
    const auto phase_key = static_cast<uint8_t>(phase);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& st = b.boundary[phase_key];
            st.count += 1;
            st.latency.record(duration_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_boundary_op(phase, duration_ns); });
}

void BoundaryOpScope::enter() noexcept {
    active_ = true;
    start_ = std::chrono::steady_clock::now();
}

void BoundaryOpScope::exit() noexcept {
    active_ = false;
    const auto end = std::chrono::steady_clock::now();
    const uint64_t duration_ns = static_cast<uint64_t>(
            std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count())
    );
    try {
        record_boundary_op(phase_, duration_ns);
    } catch (...) {
    }
}

void record_clause_exec(ClauseKind kind, uint64_t duration_ns, uint64_t rows_in, uint64_t rows_out) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(kind);
    if (idx >= static_cast<size_t>(ClauseKind::COUNT)) {
        return;
    }
    const auto key = static_cast<uint8_t>(kind);

    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            auto& st = b.clauses[key];
            st.executions += 1;
            st.rows_in += rows_in;
            st.rows_out += rows_out;
            st.latency.record(duration_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_clause_exec(kind, duration_ns, rows_in, rows_out); });
}

void record_admission_unit(bool waited, uint64_t wait_ns) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.admission.has_value()) {
                b.admission.emplace();
            }
            b.admission->units_admitted += 1;
            if (waited) {
                b.admission->residency_waits += 1;
                b.admission->residency_wait_latency.record(wait_ns);
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_admission_unit(waited, wait_ns); });
}

void record_admission_segment(bool waited, uint64_t wait_ns) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.admission.has_value()) {
                b.admission.emplace();
            }
            b.admission->segments_dispatched += 1;
            if (waited) {
                b.admission->read_window_waits += 1;
                b.admission->read_window_wait_latency.record(wait_ns);
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_admission_segment(waited, wait_ns); });
}

void record_segment_written(uint64_t rows, bool staged) {
    if (!is_active()) {
        return;
    }
    const bool is_small = (rows > 0 && rows < StreamWriteStats::SMALL_SEGMENT_ROW_THRESHOLD);
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            if (staged) {
                b.stream->staged_segments_written += 1;
            } else {
                b.stream->data_segments_written += 1;
            }
            if (is_small) {
                b.stream->small_segments_written += 1;
            }
            b.stream->rows_per_segment.record(rows);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_segment_written(rows, staged); });
}

void record_incomplete_chain_load(uint64_t segments_or_hops, bool via_iteration) {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            if (via_iteration) {
                b.stream->incomplete_iteration_loads += 1;
                b.stream->incomplete_iteration_segments += segments_or_hops;
            } else {
                b.stream->incomplete_chain_loads += 1;
                b.stream->incomplete_chain_hops += segments_or_hops;
            }
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_incomplete_chain_load(segments_or_hops, via_iteration); });
}

void record_compaction(CompactionKind kind, uint64_t input_segments, uint64_t output_segments, uint64_t duration_ns) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(kind);
    if (idx >= static_cast<size_t>(CompactionKind::COUNT)) {
        return;
    }
    const auto key = static_cast<uint8_t>(kind);
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            auto& st = b.stream->compactions[key];
            st.count += 1;
            st.input_segments += input_segments;
            st.output_segments += output_segments;
            st.latency.record(duration_ns);
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) {
        sink.on_compaction(kind, input_segments, output_segments, duration_ns);
    });
}

void record_write_conflict(WriteConflictKind kind) {
    if (!is_active()) {
        return;
    }
    const auto idx = static_cast<size_t>(kind);
    if (idx >= static_cast<size_t>(WriteConflictKind::COUNT)) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            b.stream->conflicts[idx] += 1;
        });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_write_conflict(kind); });
}

namespace {

struct PoolGaugeRegistry {
    std::mutex mutex;
    PoolGaugeProvider provider;
    std::atomic<bool> has_provider{false};
};

// Intentionally immortal to avoid static destruction order fiasco when
// TaskSchedulerPtrWrapper::~TaskSchedulerPtrWrapper() unregisters at process exit.
PoolGaugeRegistry& pool_gauge_registry() {
    static auto* reg = new PoolGaugeRegistry();
    return *reg;
}

struct LmdbGaugeRegistry {
    std::mutex mutex;
    ankerl::unordered_dense::map<uint64_t, LmdbEnvGaugeFn> providers;
    std::atomic<uint64_t> next_id{1};
    std::atomic<size_t> count{0};
};

LmdbGaugeRegistry& lmdb_gauge_registry() {
    static auto* reg = new LmdbGaugeRegistry();
    return *reg;
}

} // namespace

void record_lmdb_map_full() {
    if (!is_active()) {
        return;
    }
    dispatch_to_active_collectors([&](QueryStatsCollector::Impl& collector, const RequestState*) {
        collector.mutate_local([&](StatsBucket& b) { b.lmdb_map_full_errors += 1; });
    });

    dispatch_to_active_sink([&](StatsSink& sink) { sink.on_lmdb_map_full(); });
}

void set_pool_gauge_provider(PoolGaugeProvider provider) {
    auto& reg = pool_gauge_registry();
    std::lock_guard<std::mutex> lock(reg.mutex);
    reg.provider = std::move(provider);
    reg.has_provider.store(static_cast<bool>(reg.provider), std::memory_order_release);
}

LmdbGaugeHandle register_lmdb_env_gauge(LmdbEnvGaugeFn fn) {
    if (!fn) {
        return {};
    }
    auto& reg = lmdb_gauge_registry();
    const uint64_t id = reg.next_id.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(reg.mutex);
    reg.providers.emplace(id, std::move(fn));
    reg.count.store(reg.providers.size(), std::memory_order_release);
    return LmdbGaugeHandle{id};
}

void LmdbGaugeHandle::reset() noexcept {
    if (id_ == 0) {
        return;
    }
    const uint64_t id = id_;
    id_ = 0;
    try {
        auto& reg = lmdb_gauge_registry();
        std::lock_guard<std::mutex> lock(reg.mutex);
        reg.providers.erase(id);
        reg.count.store(reg.providers.size(), std::memory_order_release);
    } catch (...) {
    }
}

ResourceStats sample_live_resources() {
    ResourceStats res;
    auto& pool_reg = pool_gauge_registry();
    if (pool_reg.has_provider.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(pool_reg.mutex);
        if (pool_reg.provider) {
            pool_reg.provider(res.cpu_pool, res.io_pool);
            res.has_pool_gauges = true;
        }
    }
    res.write_semaphore_in_flight = static_cast<uint64_t>(
            std::max<int64_t>(0, detail::write_semaphore_in_flight.load(std::memory_order_relaxed))
    );
    res.write_semaphore_limit = detail::write_semaphore_limit.load(std::memory_order_relaxed);
    res.version_map_entries =
            static_cast<uint64_t>(std::max<int64_t>(0, detail::version_map_entries.load(std::memory_order_relaxed)));
    res.config_cache_libraries =
            static_cast<uint64_t>(std::max<int64_t>(0, detail::config_cache_libraries.load(std::memory_order_relaxed)));
    res.config_cache_storages =
            static_cast<uint64_t>(std::max<int64_t>(0, detail::config_cache_storages.load(std::memory_order_relaxed)));

    auto& lmdb_reg = lmdb_gauge_registry();
    if (lmdb_reg.count.load(std::memory_order_acquire) > 0) {
        std::lock_guard<std::mutex> lock(lmdb_reg.mutex);
        for (const auto& [_, fn] : lmdb_reg.providers) {
            uint64_t map_size = 0;
            uint64_t used = 0;
            uint64_t readers = 0;
            fn(map_size, used, readers);
            res.lmdb.open_envs += 1;
            res.lmdb.map_size_bytes += map_size;
            res.lmdb.used_bytes += used;
            res.lmdb.active_readers += readers;
        }
    }
    return res;
}

// ---------------------------------------------------------------------------
// StatsSink & Lock-Free Thread-Local PrometheusStatsSink (prometheus::Collectable)
// ---------------------------------------------------------------------------

namespace {

constexpr std::array<double, 15> DEFAULT_PROM_BUCKETS =
        {0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0};

constexpr std::array<double, 16> FAST_PROM_BUCKETS =
        {0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0};

template<size_t N>
struct PromHistogram {
    std::array<uint64_t, N + 1> buckets{};
    uint64_t count{0};
    double sum_sec{0.0};

    inline void observe_ns(uint64_t duration_ns, const std::array<double, N>& bounds) noexcept {
        const double sec = static_cast<double>(duration_ns) * 1e-9;
        size_t idx = 0;
        while (idx < N && sec > bounds[idx]) {
            ++idx;
        }
        ++buckets[idx];
        ++count;
        sum_sec += sec;
    }

    void merge(const PromHistogram& other) noexcept {
        if (other.count == 0) {
            return;
        }
        for (size_t i = 0; i <= N; ++i) {
            buckets[i] += other.buckets[i];
        }
        count += other.count;
        sum_sec += other.sum_sec;
    }

    [[nodiscard]] prometheus::ClientMetric::Histogram to_client_histogram(const std::array<double, N>& bounds) const {
        prometheus::ClientMetric::Histogram h;
        h.sample_count = count;
        h.sample_sum = sum_sec;
        h.bucket.reserve(N + 1);
        uint64_t cumulative = 0;
        for (size_t i = 0; i < N; ++i) {
            cumulative += buckets[i];
            h.bucket.push_back(prometheus::ClientMetric::Bucket{cumulative, bounds[i]});
        }
        cumulative += buckets[N];
        h.bucket.push_back(prometheus::ClientMetric::Bucket{cumulative, std::numeric_limits<double>::infinity()});
        return h;
    }
};

using DefaultPromHistogram = PromHistogram<DEFAULT_PROM_BUCKETS.size()>;
using FastPromHistogram = PromHistogram<FAST_PROM_BUCKETS.size()>;

struct PromStringHash {
    using is_transparent = void;
    using is_avalanching = void;

    [[nodiscard]] uint64_t operator()(std::string_view sv) const noexcept {
        return ankerl::unordered_dense::hash<std::string_view>{}(sv);
    }
    [[nodiscard]] uint64_t operator()(const std::string& s) const noexcept {
        return ankerl::unordered_dense::hash<std::string_view>{}(s);
    }
};

template<typename V>
using PromStringMap = ankerl::unordered_dense::map<std::string, V, PromStringHash, std::equal_to<>>;

template<typename V>
inline V& prom_find_or_emplace(PromStringMap<V>& map, std::string_view key) {
    if (auto it = map.find(key); it != map.end()) {
        return it->second;
    }
    return map.try_emplace(std::string(key)).first->second;
}

constexpr uint32_t pack_storage_op_outcome(StorageBackend backend, StorageOp op, StorageOutcome outcome) noexcept {
    return (static_cast<uint32_t>(backend) << 16U) | (static_cast<uint32_t>(op) << 8U) | static_cast<uint32_t>(outcome);
}

struct PromStorageOpEntry {
    uint64_t ops{0};
    DefaultPromHistogram dur;

    void merge(const PromStorageOpEntry& other) noexcept {
        ops += other.ops;
        dur.merge(other.dur);
    }
};

struct PromHttpEntry {
    std::array<uint64_t, static_cast<size_t>(HttpStatusClass::COUNT)> attempts{};
    uint64_t retries{0};
    uint64_t throttled{0};
    uint64_t new_connections{0};
    uint64_t checksum_mismatch_retries{0};
    uint64_t backoff_ns{0};
    DefaultPromHistogram req_dur;
    // 0=dns, 1=tcp_connect, 2=tls_handshake, 3=ttfb, 4=transfer
    std::array<FastPromHistogram, 5> phase_dur{};

    void merge(const PromHttpEntry& other) noexcept {
        for (size_t i = 0; i < attempts.size(); ++i) {
            attempts[i] += other.attempts[i];
        }
        retries += other.retries;
        throttled += other.throttled;
        new_connections += other.new_connections;
        checksum_mismatch_retries += other.checksum_mismatch_retries;
        backoff_ns += other.backoff_ns;
        req_dur.merge(other.req_dur);
        for (size_t i = 0; i < phase_dur.size(); ++i) {
            phase_dur[i].merge(other.phase_dur[i]);
        }
    }
};

constexpr uint8_t pack_api_op_outcome(ApiOp op, bool ok) noexcept {
    return static_cast<uint8_t>((static_cast<uint8_t>(op) << 1U) | (ok ? 0U : 1U));
}

struct PromLibraryData {
    ankerl::unordered_dense::map<uint32_t, PromStorageOpEntry> storage_ops;
    ankerl::unordered_dense::map<uint16_t, uint64_t> storage_bytes;
    ankerl::unordered_dense::map<uint16_t, uint64_t> storage_pages;
    std::array<uint64_t, static_cast<size_t>(StorageBackend::COUNT)> partial_delete_retries{};
    PromStringMap<PromStringMap<PromHttpEntry>> http;
    ankerl::unordered_dense::map<uint8_t, PromStorageOpEntry> api_ops;

    void merge(const PromLibraryData& other) {
        for (const auto& [k, v] : other.storage_ops) {
            storage_ops[k].merge(v);
        }
        for (const auto& [k, v] : other.storage_bytes) {
            storage_bytes[k] += v;
        }
        for (const auto& [k, v] : other.storage_pages) {
            storage_pages[k] += v;
        }
        for (size_t i = 0; i < partial_delete_retries.size(); ++i) {
            partial_delete_retries[i] += other.partial_delete_retries[i];
        }
        for (const auto& [svc, ops] : other.http) {
            auto& dst_ops = http[svc];
            for (const auto& [op, entry] : ops) {
                dst_ops[op].merge(entry);
            }
        }
        for (const auto& [k, v] : other.api_ops) {
            api_ops[k].merge(v);
        }
    }
};

struct PromClauseEntry {
    uint64_t executions{0};
    uint64_t rows_in{0};
    uint64_t rows_out{0};
    FastPromHistogram dur;

    void merge(const PromClauseEntry& other) noexcept {
        executions += other.executions;
        rows_in += other.rows_in;
        rows_out += other.rows_out;
        dur.merge(other.dur);
    }
};

struct PromCompactionEntry {
    uint64_t count{0};
    uint64_t input_segments{0};
    uint64_t output_segments{0};
    DefaultPromHistogram dur;

    void merge(const PromCompactionEntry& other) noexcept {
        count += other.count;
        input_segments += other.input_segments;
        output_segments += other.output_segments;
        dur.merge(other.dur);
    }
};

struct PromStreamBucket {
    uint64_t data_segments_written{0};
    uint64_t staged_segments_written{0};
    uint64_t small_segments_written{0};
    RowCountHistogram rows_per_segment;
    uint64_t incomplete_chain_loads{0};
    uint64_t incomplete_chain_hops{0};
    uint64_t incomplete_iteration_loads{0};
    uint64_t incomplete_iteration_segments{0};
    ankerl::unordered_dense::map<uint8_t, PromCompactionEntry> compactions;
    std::array<uint64_t, static_cast<size_t>(WriteConflictKind::COUNT)> conflicts{};

    void merge(const PromStreamBucket& other) {
        data_segments_written += other.data_segments_written;
        staged_segments_written += other.staged_segments_written;
        small_segments_written += other.small_segments_written;
        rows_per_segment.merge(other.rows_per_segment);
        incomplete_chain_loads += other.incomplete_chain_loads;
        incomplete_chain_hops += other.incomplete_chain_hops;
        incomplete_iteration_loads += other.incomplete_iteration_loads;
        incomplete_iteration_segments += other.incomplete_iteration_segments;
        for (const auto& [k, v] : other.compactions) {
            compactions[k].merge(v);
        }
        for (size_t i = 0; i < conflicts.size(); ++i) {
            conflicts[i] += other.conflicts[i];
        }
    }
};

struct PromBucket {
    PromLibraryData global_lib;
    PromStringMap<PromLibraryData> by_library;

    std::array<uint64_t, static_cast<size_t>(PoolKind::COUNT)> pool_tasks{};
    std::array<uint64_t, static_cast<size_t>(PoolKind::COUNT)> pool_expired{};
    std::array<FastPromHistogram, static_cast<size_t>(PoolKind::COUNT)> pool_wait{};
    std::array<FastPromHistogram, static_cast<size_t>(PoolKind::COUNT)> pool_run{};
    uint64_t semaphore_waits{0};
    FastPromHistogram semaphore_wait{};

    // cache idx: 0=version_map, 1=dedup_map, 2=config_cache; result idx: 0=miss, 1=hit
    std::array<std::array<uint64_t, 2>, 3> cache_lookups{};
    uint64_t version_map_reloads{0};
    uint64_t version_map_chain_hops{0};
    uint64_t version_map_ref_retries{0};
    uint64_t dedup_bytes_saved{0};

    // op idx: 0=encode, 1=decode; stage idx: 0=uncompressed, 1=compressed
    std::array<uint64_t, 2> codec_ops{};
    std::array<std::array<uint64_t, 2>, 2> codec_bytes{};
    std::array<FastPromHistogram, 2> codec_dur{};

    uint64_t query_reads{0};
    // 0=total, 1=pruned_index, 2=pruned_column_stats, 3=read
    std::array<uint64_t, 4> query_segments{};
    // 0=total, 1=projected
    std::array<uint64_t, 2> query_columns{};
    // 0=total, 1=selected
    std::array<uint64_t, 2> query_rows{};

    // event idx: 0=acquire, 1=conflict, 2=retry, 3=timeout
    std::array<std::array<uint64_t, 4>, static_cast<size_t>(LockKind::COUNT)> lock_events{};
    std::array<FastPromHistogram, static_cast<size_t>(LockKind::COUNT)> lock_wait{};

    std::array<uint64_t, static_cast<size_t>(BoundaryPhase::COUNT)> boundary_ops{};
    std::array<FastPromHistogram, static_cast<size_t>(BoundaryPhase::COUNT)> boundary_dur{};

    ankerl::unordered_dense::map<uint8_t, PromClauseEntry> clauses;
    uint64_t admission_units{0};
    uint64_t admission_residency_waits{0};
    uint64_t admission_segments{0};
    uint64_t admission_read_window_waits{0};
    FastPromHistogram admission_residency_wait;
    FastPromHistogram admission_read_window_wait;

    std::optional<PromStreamBucket> stream;
    uint64_t lmdb_map_full_errors{0};

    void merge(const PromBucket& other) {
        global_lib.merge(other.global_lib);
        for (const auto& [lib, data] : other.by_library) {
            by_library[lib].merge(data);
        }
        for (size_t i = 0; i < pool_tasks.size(); ++i) {
            pool_tasks[i] += other.pool_tasks[i];
            pool_expired[i] += other.pool_expired[i];
            pool_wait[i].merge(other.pool_wait[i]);
            pool_run[i].merge(other.pool_run[i]);
        }
        semaphore_waits += other.semaphore_waits;
        semaphore_wait.merge(other.semaphore_wait);

        for (size_t c = 0; c < cache_lookups.size(); ++c) {
            cache_lookups[c][0] += other.cache_lookups[c][0];
            cache_lookups[c][1] += other.cache_lookups[c][1];
        }
        version_map_reloads += other.version_map_reloads;
        version_map_chain_hops += other.version_map_chain_hops;
        version_map_ref_retries += other.version_map_ref_retries;
        dedup_bytes_saved += other.dedup_bytes_saved;

        for (size_t i = 0; i < 2; ++i) {
            codec_ops[i] += other.codec_ops[i];
            codec_bytes[i][0] += other.codec_bytes[i][0];
            codec_bytes[i][1] += other.codec_bytes[i][1];
            codec_dur[i].merge(other.codec_dur[i]);
        }

        query_reads += other.query_reads;
        for (size_t i = 0; i < query_segments.size(); ++i) {
            query_segments[i] += other.query_segments[i];
        }
        query_columns[0] += other.query_columns[0];
        query_columns[1] += other.query_columns[1];
        query_rows[0] += other.query_rows[0];
        query_rows[1] += other.query_rows[1];

        for (size_t k = 0; k < lock_events.size(); ++k) {
            for (size_t e = 0; e < 4; ++e) {
                lock_events[k][e] += other.lock_events[k][e];
            }
            lock_wait[k].merge(other.lock_wait[k]);
        }

        for (size_t p = 0; p < boundary_ops.size(); ++p) {
            boundary_ops[p] += other.boundary_ops[p];
            boundary_dur[p].merge(other.boundary_dur[p]);
        }

        for (const auto& [k, v] : other.clauses) {
            clauses[k].merge(v);
        }
        admission_units += other.admission_units;
        admission_residency_waits += other.admission_residency_waits;
        admission_segments += other.admission_segments;
        admission_read_window_waits += other.admission_read_window_waits;
        admission_residency_wait.merge(other.admission_residency_wait);
        admission_read_window_wait.merge(other.admission_read_window_wait);

        if (other.stream.has_value()) {
            if (!stream.has_value()) {
                stream.emplace();
            }
            stream->merge(*other.stream);
        }
        lmdb_map_full_errors += other.lmdb_map_full_errors;
    }
};

struct PromSinkTlsTag {};

prometheus::ClientMetric make_counter_metric(std::vector<prometheus::ClientMetric::Label> labels, double value) {
    prometheus::ClientMetric m;
    m.label = std::move(labels);
    m.counter.value = value;
    return m;
}

prometheus::ClientMetric make_gauge_metric(std::vector<prometheus::ClientMetric::Label> labels, double value) {
    prometheus::ClientMetric m;
    m.label = std::move(labels);
    m.gauge.value = value;
    return m;
}

template<size_t N>
prometheus::ClientMetric make_histogram_metric(
        std::vector<prometheus::ClientMetric::Label> labels, const PromHistogram<N>& hist,
        const std::array<double, N>& bounds
) {
    prometheus::ClientMetric m;
    m.label = std::move(labels);
    m.histogram = hist.to_client_histogram(bounds);
    return m;
}

void push_family_if_nonempty(
        std::vector<prometheus::MetricFamily>& out, const char* name, const char* help, prometheus::MetricType type,
        std::vector<prometheus::ClientMetric> metrics
) {
    if (metrics.empty()) {
        return;
    }
    prometheus::MetricFamily f;
    f.name = name;
    f.help = help;
    f.type = type;
    f.metric = std::move(metrics);
    out.push_back(std::move(f));
}

// Per-thread lock-free-contending storage for PrometheusStatsSink, aggregated only on
// scrape (`Collect()`) or `PrometheusInstance::push()`. Never emits a `symbol` label.
class PromCollectableState final : public prometheus::Collectable {
  public:
    struct RetiredState {
        mutable std::mutex mutex;
        PromBucket bucket;
        std::atomic<bool> alive{true};
    };

    struct ThreadLocalEntry {
        std::shared_ptr<RetiredState> retired_;
        std::mutex mutex_;
        PromBucket bucket_;

        explicit ThreadLocalEntry(std::shared_ptr<RetiredState> retired) : retired_(std::move(retired)) {}

        ~ThreadLocalEntry() {
            if (retired_ && retired_->alive.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lock(retired_->mutex);
                if (retired_->alive.load(std::memory_order_relaxed)) {
                    retired_->bucket.merge(bucket_);
                }
            }
        }
    };

    explicit PromCollectableState(bool include_library) :
        include_library_(include_library),
        retired_(std::make_shared<RetiredState>()),
        tls_([retired = retired_]() { return std::make_unique<ThreadLocalEntry>(retired); }) {}

    ~PromCollectableState() override { retired_->alive.store(false, std::memory_order_release); }

    template<typename Fn>
    inline void mutate_local(Fn&& fn) {
        auto& entry = *(*tls_);
        std::lock_guard<std::mutex> lock(entry.mutex_);
        fn(entry.bucket_);
    }

    inline PromLibraryData& lib_data(PromBucket& b, std::string_view library) const {
        if (!include_library_) {
            return b.global_lib;
        }
        return prom_find_or_emplace(b.by_library, library);
    }

    std::vector<prometheus::MetricFamily> Collect() const override {
        PromBucket merged;
        {
            auto accessor = tls_.accessAllThreads();
            for (const auto& entry_ptr : accessor) {
                if (!entry_ptr) {
                    continue;
                }
                std::lock_guard<std::mutex> lock(entry_ptr->mutex_);
                merged.merge(entry_ptr->bucket_);
            }
        }
        {
            std::lock_guard<std::mutex> lock(retired_->mutex);
            merged.merge(retired_->bucket);
        }

        std::vector<prometheus::MetricFamily> out;
        out.reserve(30);

        // Storage & HTTP metrics
        std::vector<prometheus::ClientMetric> storage_ops_m;
        std::vector<prometheus::ClientMetric> storage_bytes_m;
        std::vector<prometheus::ClientMetric> storage_dur_m;
        std::vector<prometheus::ClientMetric> storage_pages_m;
        std::vector<prometheus::ClientMetric> storage_pdr_m;
        std::vector<prometheus::ClientMetric> http_attempts_m;
        std::vector<prometheus::ClientMetric> http_retries_m;
        std::vector<prometheus::ClientMetric> http_dur_m;
        std::vector<prometheus::ClientMetric> http_new_conns_m;
        std::vector<prometheus::ClientMetric> http_csum_retries_m;
        std::vector<prometheus::ClientMetric> http_backoff_m;
        std::vector<prometheus::ClientMetric> http_throttled_m;
        std::vector<prometheus::ClientMetric> http_phase_dur_m;
        std::vector<prometheus::ClientMetric> api_ops_m;
        std::vector<prometheus::ClientMetric> api_dur_m;

        auto emit_lib_data = [&](const std::string* lib_name, const PromLibraryData& ld) {
            for (const auto& [packed, entry] : ld.storage_ops) {
                if (entry.ops == 0) {
                    continue;
                }
                const auto backend = static_cast<StorageBackend>((packed >> 16U) & 0xFFU);
                const auto op = static_cast<StorageOp>((packed >> 8U) & 0xFFU);
                const auto outcome = static_cast<StorageOutcome>(packed & 0xFFU);
                std::vector<prometheus::ClientMetric::Label> labels;
                labels.reserve(lib_name ? 4 : 3);
                labels.push_back({"backend", to_string(backend)});
                if (lib_name) {
                    labels.push_back({"library", *lib_name});
                }
                labels.push_back({"op", to_string(op)});
                labels.push_back({"outcome", to_string(outcome)});
                storage_ops_m.push_back(make_counter_metric(labels, static_cast<double>(entry.ops)));
                storage_dur_m.push_back(make_histogram_metric(std::move(labels), entry.dur, DEFAULT_PROM_BUCKETS));
            }
            for (const auto& [packed, bytes] : ld.storage_bytes) {
                if (bytes == 0) {
                    continue;
                }
                const auto backend = static_cast<StorageBackend>((packed >> 8U) & 0xFFU);
                const auto op = static_cast<StorageOp>(packed & 0xFFU);
                std::vector<prometheus::ClientMetric::Label> labels;
                labels.reserve(lib_name ? 3 : 2);
                labels.push_back({"backend", to_string(backend)});
                if (lib_name) {
                    labels.push_back({"library", *lib_name});
                }
                labels.push_back({"op", to_string(op)});
                storage_bytes_m.push_back(make_counter_metric(std::move(labels), static_cast<double>(bytes)));
            }
            for (const auto& [packed, pages] : ld.storage_pages) {
                if (pages == 0) {
                    continue;
                }
                const auto backend = static_cast<StorageBackend>((packed >> 8U) & 0xFFU);
                const auto op = static_cast<StorageOp>(packed & 0xFFU);
                std::vector<prometheus::ClientMetric::Label> labels;
                labels.reserve(lib_name ? 3 : 2);
                labels.push_back({"backend", to_string(backend)});
                if (lib_name) {
                    labels.push_back({"library", *lib_name});
                }
                labels.push_back({"op", to_string(op)});
                storage_pages_m.push_back(make_counter_metric(std::move(labels), static_cast<double>(pages)));
            }
            for (size_t b_idx = 0; b_idx < ld.partial_delete_retries.size(); ++b_idx) {
                const uint64_t val = ld.partial_delete_retries[b_idx];
                if (val == 0) {
                    continue;
                }
                std::vector<prometheus::ClientMetric::Label> labels;
                labels.reserve(lib_name ? 2 : 1);
                labels.push_back({"backend", to_string(static_cast<StorageBackend>(b_idx))});
                if (lib_name) {
                    labels.push_back({"library", *lib_name});
                }
                storage_pdr_m.push_back(make_counter_metric(std::move(labels), static_cast<double>(val)));
            }
            for (const auto& [svc, ops] : ld.http) {
                for (const auto& [op, entry] : ops) {
                    auto base_labels = [&]() {
                        std::vector<prometheus::ClientMetric::Label> l;
                        l.reserve(lib_name ? 3 : 2);
                        if (lib_name) {
                            l.push_back({"library", *lib_name});
                        }
                        l.push_back({"operation", op});
                        l.push_back({"service", svc});
                        return l;
                    };
                    for (size_t s_idx = 0; s_idx < entry.attempts.size(); ++s_idx) {
                        const uint64_t cnt = entry.attempts[s_idx];
                        if (cnt == 0) {
                            continue;
                        }
                        auto labels = base_labels();
                        labels.push_back({"status_class", to_string(static_cast<HttpStatusClass>(s_idx))});
                        http_attempts_m.push_back(make_counter_metric(std::move(labels), static_cast<double>(cnt)));
                    }
                    if (entry.retries > 0) {
                        http_retries_m.push_back(make_counter_metric(base_labels(), static_cast<double>(entry.retries))
                        );
                    }
                    if (entry.req_dur.count > 0) {
                        http_dur_m.push_back(make_histogram_metric(base_labels(), entry.req_dur, DEFAULT_PROM_BUCKETS));
                    }
                    if (entry.new_connections > 0) {
                        http_new_conns_m.push_back(
                                make_counter_metric(base_labels(), static_cast<double>(entry.new_connections))
                        );
                    }
                    if (entry.checksum_mismatch_retries > 0) {
                        http_csum_retries_m.push_back(
                                make_counter_metric(base_labels(), static_cast<double>(entry.checksum_mismatch_retries))
                        );
                    }
                    if (entry.backoff_ns > 0) {
                        http_backoff_m.push_back(
                                make_counter_metric(base_labels(), static_cast<double>(entry.backoff_ns) * 1e-9)
                        );
                    }
                    if (entry.throttled > 0) {
                        http_throttled_m.push_back(
                                make_counter_metric(base_labels(), static_cast<double>(entry.throttled))
                        );
                    }
                    static constexpr std::array<const char*, 5> HTTP_PHASES = {
                            "dns", "tcp_connect", "tls_handshake", "ttfb", "transfer"
                    };
                    for (size_t p_idx = 0; p_idx < HTTP_PHASES.size(); ++p_idx) {
                        if (entry.phase_dur[p_idx].count == 0) {
                            continue;
                        }
                        std::vector<prometheus::ClientMetric::Label> labels;
                        labels.reserve(lib_name ? 4 : 3);
                        if (lib_name) {
                            labels.push_back({"library", *lib_name});
                        }
                        labels.push_back({"operation", op});
                        labels.push_back({"phase", HTTP_PHASES[p_idx]});
                        labels.push_back({"service", svc});
                        http_phase_dur_m.push_back(
                                make_histogram_metric(std::move(labels), entry.phase_dur[p_idx], FAST_PROM_BUCKETS)
                        );
                    }
                }
            }
            for (const auto& [packed, entry] : ld.api_ops) {
                if (entry.ops == 0) {
                    continue;
                }
                const auto op = static_cast<ApiOp>(packed >> 1U);
                const bool ok = (packed & 1U) == 0U;
                std::vector<prometheus::ClientMetric::Label> labels;
                labels.reserve(lib_name ? 3 : 2);
                if (lib_name) {
                    labels.push_back({"library", *lib_name});
                }
                labels.push_back({"op", to_string(op)});
                labels.push_back({"outcome", ok ? "ok" : "error"});
                api_ops_m.push_back(make_counter_metric(labels, static_cast<double>(entry.ops)));
                api_dur_m.push_back(make_histogram_metric(std::move(labels), entry.dur, DEFAULT_PROM_BUCKETS));
            }
        };

        if (include_library_) {
            for (const auto& [lib, ld] : merged.by_library) {
                emit_lib_data(&lib, ld);
            }
        } else {
            emit_lib_data(nullptr, merged.global_lib);
        }

        push_family_if_nonempty(
                out,
                "arcticdb_storage_ops_total",
                "Total ArcticDB storage operations",
                prometheus::MetricType::Counter,
                std::move(storage_ops_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_storage_bytes_total",
                "Total bytes transferred by ArcticDB storage operations",
                prometheus::MetricType::Counter,
                std::move(storage_bytes_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_storage_op_duration_seconds",
                "Latency of ArcticDB storage operations in seconds",
                prometheus::MetricType::Histogram,
                std::move(storage_dur_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_storage_pages_total",
                "Total paginated list/scan pages fetched from storage",
                prometheus::MetricType::Counter,
                std::move(storage_pages_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_storage_partial_delete_retries_total",
                "Total partial batch-delete retries",
                prometheus::MetricType::Counter,
                std::move(storage_pdr_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_attempts_total",
                "Total ArcticDB HTTP request attempts",
                prometheus::MetricType::Counter,
                std::move(http_attempts_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_retries_total",
                "Total ArcticDB HTTP request retries",
                prometheus::MetricType::Counter,
                std::move(http_retries_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_request_duration_seconds",
                "Latency of ArcticDB HTTP requests in seconds",
                prometheus::MetricType::Histogram,
                std::move(http_dur_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_new_connections_total",
                "Total new TCP/TLS connections established for HTTP requests",
                prometheus::MetricType::Counter,
                std::move(http_new_conns_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_checksum_mismatch_retries_total",
                "Total HTTP retries triggered by checksum mismatches",
                prometheus::MetricType::Counter,
                std::move(http_csum_retries_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_backoff_seconds_total",
                "Total time spent in HTTP retry backoff in seconds",
                prometheus::MetricType::Counter,
                std::move(http_backoff_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_throttled_total",
                "Total throttled HTTP responses (HTTP 429 / 503)",
                prometheus::MetricType::Counter,
                std::move(http_throttled_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_http_phase_duration_seconds",
                "Sub-phase latency of HTTP attempts (dns, tcp_connect, tls_handshake, ttfb, transfer) in seconds",
                prometheus::MetricType::Histogram,
                std::move(http_phase_dur_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_api_ops_total",
                "Total top-level ArcticDB Library API operations by op and outcome",
                prometheus::MetricType::Counter,
                std::move(api_ops_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_api_op_duration_seconds",
                "End-to-end latency of top-level ArcticDB Library API operations in seconds",
                prometheus::MetricType::Histogram,
                std::move(api_dur_m)
        );

        // Thread pools & backpressure
        std::vector<prometheus::ClientMetric> pool_tasks_m;
        std::vector<prometheus::ClientMetric> pool_expired_m;
        std::vector<prometheus::ClientMetric> pool_wait_m;
        std::vector<prometheus::ClientMetric> pool_run_m;
        for (size_t i = 0; i < merged.pool_tasks.size(); ++i) {
            const char* pool_str = to_string(static_cast<PoolKind>(i));
            if (merged.pool_tasks[i] > 0) {
                pool_tasks_m.push_back(
                        make_counter_metric({{"pool", pool_str}}, static_cast<double>(merged.pool_tasks[i]))
                );
            }
            if (merged.pool_expired[i] > 0) {
                pool_expired_m.push_back(
                        make_counter_metric({{"pool", pool_str}}, static_cast<double>(merged.pool_expired[i]))
                );
            }
            if (merged.pool_wait[i].count > 0) {
                pool_wait_m.push_back(
                        make_histogram_metric({{"pool", pool_str}}, merged.pool_wait[i], FAST_PROM_BUCKETS)
                );
            }
            if (merged.pool_run[i].count > 0) {
                pool_run_m.push_back(make_histogram_metric({{"pool", pool_str}}, merged.pool_run[i], FAST_PROM_BUCKETS)
                );
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_pool_tasks_total",
                "Total tasks executed on ArcticDB CPU/IO thread pools",
                prometheus::MetricType::Counter,
                std::move(pool_tasks_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_pool_tasks_expired_total",
                "Total tasks expired before execution on ArcticDB thread pools",
                prometheus::MetricType::Counter,
                std::move(pool_expired_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_pool_task_wait_duration_seconds",
                "Queue wait latency of ArcticDB thread pool tasks in seconds",
                prometheus::MetricType::Histogram,
                std::move(pool_wait_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_pool_task_run_duration_seconds",
                "Execution latency of ArcticDB thread pool tasks in seconds",
                prometheus::MetricType::Histogram,
                std::move(pool_run_m)
        );
        if (merged.semaphore_waits > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_async_store_semaphore_waits_total",
                    "Total async store write backpressure semaphore waits",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.semaphore_waits))}
            );
        }
        if (merged.semaphore_wait.count > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_async_store_semaphore_wait_duration_seconds",
                    "Wait latency on async store write backpressure semaphore in seconds",
                    prometheus::MetricType::Histogram,
                    {make_histogram_metric({}, merged.semaphore_wait, FAST_PROM_BUCKETS)}
            );
        }

        // Caches
        static constexpr std::array<const char*, 3> CACHE_NAMES = {"version_map", "dedup_map", "config_cache"};
        std::vector<prometheus::ClientMetric> cache_lookups_m;
        for (size_t c = 0; c < CACHE_NAMES.size(); ++c) {
            if (merged.cache_lookups[c][1] > 0) {
                cache_lookups_m.push_back(make_counter_metric(
                        {{"cache", CACHE_NAMES[c]}, {"result", "hit"}}, static_cast<double>(merged.cache_lookups[c][1])
                ));
            }
            if (merged.cache_lookups[c][0] > 0) {
                cache_lookups_m.push_back(make_counter_metric(
                        {{"cache", CACHE_NAMES[c]}, {"result", "miss"}}, static_cast<double>(merged.cache_lookups[c][0])
                ));
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_cache_lookups_total",
                "Total ArcticDB internal cache lookups by cache and result",
                prometheus::MetricType::Counter,
                std::move(cache_lookups_m)
        );
        if (merged.version_map_reloads > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_version_map_reloads_total",
                    "Total VersionMap cache reloads from storage",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.version_map_reloads))}
            );
        }
        if (merged.version_map_chain_hops > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_version_map_chain_hops_total",
                    "Total version chain hops traversed during VersionMap lookups",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.version_map_chain_hops))}
            );
        }
        if (merged.version_map_ref_retries > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_version_map_ref_retries_total",
                    "Total VersionMap VERSION_REF optimistic read retries",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.version_map_ref_retries))}
            );
        }
        if (merged.dedup_bytes_saved > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_dedup_bytes_saved_total",
                    "Total storage bytes saved by DeDupMap hits",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.dedup_bytes_saved))}
            );
        }

        // Codec
        static constexpr std::array<const char*, 2> CODEC_OPS = {"encode", "decode"};
        std::vector<prometheus::ClientMetric> codec_ops_m;
        std::vector<prometheus::ClientMetric> codec_bytes_m;
        std::vector<prometheus::ClientMetric> codec_dur_m;
        for (size_t i = 0; i < 2; ++i) {
            if (merged.codec_ops[i] > 0) {
                codec_ops_m.push_back(
                        make_counter_metric({{"op", CODEC_OPS[i]}}, static_cast<double>(merged.codec_ops[i]))
                );
            }
            if (merged.codec_bytes[i][0] > 0) {
                codec_bytes_m.push_back(make_counter_metric(
                        {{"op", CODEC_OPS[i]}, {"stage", "uncompressed"}}, static_cast<double>(merged.codec_bytes[i][0])
                ));
            }
            if (merged.codec_bytes[i][1] > 0) {
                codec_bytes_m.push_back(make_counter_metric(
                        {{"op", CODEC_OPS[i]}, {"stage", "compressed"}}, static_cast<double>(merged.codec_bytes[i][1])
                ));
            }
            if (merged.codec_dur[i].count > 0) {
                codec_dur_m.push_back(
                        make_histogram_metric({{"op", CODEC_OPS[i]}}, merged.codec_dur[i], FAST_PROM_BUCKETS)
                );
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_codec_ops_total",
                "Total ArcticDB codec encode/decode operations",
                prometheus::MetricType::Counter,
                std::move(codec_ops_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_codec_bytes_total",
                "Total uncompressed and compressed bytes processed by ArcticDB codec",
                prometheus::MetricType::Counter,
                std::move(codec_bytes_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_codec_duration_seconds",
                "Latency of ArcticDB codec encode/decode operations in seconds",
                prometheus::MetricType::Histogram,
                std::move(codec_dur_m)
        );

        // Query pruning & projection
        if (merged.query_reads > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_query_reads_total",
                    "Total indexed query reads evaluated by ArcticDB",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.query_reads))}
            );
        }
        static constexpr std::array<const char*, 4> SEG_OUTCOMES = {
                "total", "pruned_index", "pruned_column_stats", "read"
        };
        std::vector<prometheus::ClientMetric> q_seg_m;
        for (size_t i = 0; i < SEG_OUTCOMES.size(); ++i) {
            if (merged.query_segments[i] > 0) {
                q_seg_m.push_back(make_counter_metric(
                        {{"outcome", SEG_OUTCOMES[i]}}, static_cast<double>(merged.query_segments[i])
                ));
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_query_segments_total",
                "Total segments evaluated, pruned (index/column_stats), and read by queries",
                prometheus::MetricType::Counter,
                std::move(q_seg_m)
        );
        std::vector<prometheus::ClientMetric> q_col_m;
        if (merged.query_columns[0] > 0) {
            q_col_m.push_back(make_counter_metric({{"kind", "total"}}, static_cast<double>(merged.query_columns[0])));
        }
        if (merged.query_columns[1] > 0) {
            q_col_m.push_back(make_counter_metric({{"kind", "projected"}}, static_cast<double>(merged.query_columns[1]))
            );
        }
        push_family_if_nonempty(
                out,
                "arcticdb_query_columns_total",
                "Total and projected column counts across queries",
                prometheus::MetricType::Counter,
                std::move(q_col_m)
        );
        std::vector<prometheus::ClientMetric> q_row_m;
        if (merged.query_rows[0] > 0) {
            q_row_m.push_back(make_counter_metric({{"kind", "total"}}, static_cast<double>(merged.query_rows[0])));
        }
        if (merged.query_rows[1] > 0) {
            q_row_m.push_back(make_counter_metric({{"kind", "selected"}}, static_cast<double>(merged.query_rows[1])));
        }
        push_family_if_nonempty(
                out,
                "arcticdb_query_rows_total",
                "Total and selected row counts across queries",
                prometheus::MetricType::Counter,
                std::move(q_row_m)
        );

        // Distributed locks
        static constexpr std::array<const char*, 4> LOCK_EVENTS = {"acquire", "conflict", "retry", "timeout"};
        std::vector<prometheus::ClientMetric> lock_events_m;
        std::vector<prometheus::ClientMetric> lock_wait_m;
        for (size_t k = 0; k < merged.lock_events.size(); ++k) {
            const char* kind_str = to_string(static_cast<LockKind>(k));
            for (size_t e = 0; e < LOCK_EVENTS.size(); ++e) {
                if (merged.lock_events[k][e] > 0) {
                    lock_events_m.push_back(make_counter_metric(
                            {{"event", LOCK_EVENTS[e]}, {"kind", kind_str}},
                            static_cast<double>(merged.lock_events[k][e])
                    ));
                }
            }
            if (merged.lock_wait[k].count > 0) {
                lock_wait_m.push_back(
                        make_histogram_metric({{"kind", kind_str}}, merged.lock_wait[k], FAST_PROM_BUCKETS)
                );
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_lock_events_total",
                "Total ArcticDB distributed lock events (acquire, conflict, retry, timeout)",
                prometheus::MetricType::Counter,
                std::move(lock_events_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_lock_wait_duration_seconds",
                "Wait latency of ArcticDB distributed lock operations in seconds",
                prometheus::MetricType::Histogram,
                std::move(lock_wait_m)
        );

        // Python / Arrow boundary & GIL wait
        std::vector<prometheus::ClientMetric> boundary_ops_m;
        std::vector<prometheus::ClientMetric> boundary_dur_m;
        for (size_t p = 0; p < merged.boundary_ops.size(); ++p) {
            const char* phase_str = to_string(static_cast<BoundaryPhase>(p));
            if (merged.boundary_ops[p] > 0) {
                boundary_ops_m.push_back(
                        make_counter_metric({{"phase", phase_str}}, static_cast<double>(merged.boundary_ops[p]))
                );
            }
            if (merged.boundary_dur[p].count > 0) {
                boundary_dur_m.push_back(
                        make_histogram_metric({{"phase", phase_str}}, merged.boundary_dur[p], FAST_PROM_BUCKETS)
                );
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_boundary_ops_total",
                "Total Python/Arrow boundary and GIL acquisition events by phase",
                prometheus::MetricType::Counter,
                std::move(boundary_ops_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_boundary_duration_seconds",
                "Latency of Python/Arrow boundary conversions and GIL wait in seconds",
                prometheus::MetricType::Histogram,
                std::move(boundary_dur_m)
        );

        // QueryBuilder processing clauses & read admission backpressure
        std::vector<prometheus::ClientMetric> clause_exec_m;
        std::vector<prometheus::ClientMetric> clause_rows_m;
        std::vector<prometheus::ClientMetric> clause_dur_m;
        for (const auto& [k, entry] : merged.clauses) {
            if (entry.executions == 0) {
                continue;
            }
            const char* clause_str = to_string(static_cast<ClauseKind>(k));
            clause_exec_m.push_back(make_counter_metric({{"clause", clause_str}}, static_cast<double>(entry.executions))
            );
            if (entry.rows_in > 0) {
                clause_rows_m.push_back(make_counter_metric(
                        {{"clause", clause_str}, {"direction", "in"}}, static_cast<double>(entry.rows_in)
                ));
            }
            if (entry.rows_out > 0) {
                clause_rows_m.push_back(make_counter_metric(
                        {{"clause", clause_str}, {"direction", "out"}}, static_cast<double>(entry.rows_out)
                ));
            }
            if (entry.dur.count > 0) {
                clause_dur_m.push_back(make_histogram_metric({{"clause", clause_str}}, entry.dur, FAST_PROM_BUCKETS));
            }
        }
        push_family_if_nonempty(
                out,
                "arcticdb_processing_clause_executions_total",
                "Total QueryBuilder processing clause executions by clause kind",
                prometheus::MetricType::Counter,
                std::move(clause_exec_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_processing_clause_rows_total",
                "Total rows entering and leaving QueryBuilder processing clauses",
                prometheus::MetricType::Counter,
                std::move(clause_rows_m)
        );
        push_family_if_nonempty(
                out,
                "arcticdb_processing_clause_duration_seconds",
                "Execution latency of QueryBuilder processing clauses in seconds",
                prometheus::MetricType::Histogram,
                std::move(clause_dur_m)
        );

        std::vector<prometheus::ClientMetric> adm_events_m;
        if (merged.admission_units > 0) {
            adm_events_m.push_back(
                    make_counter_metric({{"kind", "unit_admitted"}}, static_cast<double>(merged.admission_units))
            );
        }
        if (merged.admission_residency_waits > 0) {
            adm_events_m.push_back(make_counter_metric(
                    {{"kind", "residency_wait"}}, static_cast<double>(merged.admission_residency_waits)
            ));
        }
        if (merged.admission_segments > 0) {
            adm_events_m.push_back(make_counter_metric(
                    {{"kind", "segment_dispatched"}}, static_cast<double>(merged.admission_segments)
            ));
        }
        if (merged.admission_read_window_waits > 0) {
            adm_events_m.push_back(make_counter_metric(
                    {{"kind", "read_window_wait"}}, static_cast<double>(merged.admission_read_window_waits)
            ));
        }
        push_family_if_nonempty(
                out,
                "arcticdb_admission_events_total",
                "Total ProcessingUnitAdmissionHandler unit/segment admissions and backpressure waits",
                prometheus::MetricType::Counter,
                std::move(adm_events_m)
        );

        std::vector<prometheus::ClientMetric> adm_wait_m;
        if (merged.admission_residency_wait.count > 0) {
            adm_wait_m.push_back(
                    make_histogram_metric({{"kind", "residency"}}, merged.admission_residency_wait, FAST_PROM_BUCKETS)
            );
        }
        if (merged.admission_read_window_wait.count > 0) {
            adm_wait_m.push_back(make_histogram_metric(
                    {{"kind", "read_window"}}, merged.admission_read_window_wait, FAST_PROM_BUCKETS
            ));
        }
        push_family_if_nonempty(
                out,
                "arcticdb_admission_wait_duration_seconds",
                "Backpressure wait latency in ProcessingUnitAdmissionHandler (residency budget and read window)",
                prometheus::MetricType::Histogram,
                std::move(adm_wait_m)
        );

        if (merged.stream.has_value()) {
            const auto& sb = *merged.stream;
            std::vector<prometheus::ClientMetric> segs_written_m;
            if (sb.data_segments_written > 0) {
                segs_written_m.push_back(
                        make_counter_metric({{"kind", "data"}}, static_cast<double>(sb.data_segments_written))
                );
            }
            if (sb.staged_segments_written > 0) {
                segs_written_m.push_back(
                        make_counter_metric({{"kind", "staged"}}, static_cast<double>(sb.staged_segments_written))
                );
            }
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_segments_written_total",
                    "Total data and staged segments encoded for writing",
                    prometheus::MetricType::Counter,
                    std::move(segs_written_m)
            );
            if (sb.small_segments_written > 0) {
                push_family_if_nonempty(
                        out,
                        "arcticdb_stream_small_segments_written_total",
                        "Total written segments with fewer than 1000 rows (fragmentation indicator)",
                        prometheus::MetricType::Counter,
                        {make_counter_metric({}, static_cast<double>(sb.small_segments_written))}
                );
            }
            if (sb.rows_per_segment.count > 0) {
                prometheus::ClientMetric m;
                m.histogram.sample_count = sb.rows_per_segment.count;
                m.histogram.sample_sum = static_cast<double>(sb.rows_per_segment.sum);
                m.histogram.bucket.reserve(RowCountHistogram::NUM_BUCKETS);
                uint64_t cumulative = 0;
                for (size_t i = 0; i < RowCountHistogram::BOUNDS.size(); ++i) {
                    cumulative += sb.rows_per_segment.buckets[i];
                    m.histogram.bucket.push_back(prometheus::ClientMetric::Bucket{
                            cumulative, static_cast<double>(RowCountHistogram::BOUNDS[i])
                    });
                }
                cumulative += sb.rows_per_segment.buckets[RowCountHistogram::BOUNDS.size()];
                m.histogram.bucket.push_back(
                        prometheus::ClientMetric::Bucket{cumulative, std::numeric_limits<double>::infinity()}
                );
                push_family_if_nonempty(
                        out,
                        "arcticdb_stream_segment_rows",
                        "Row count distribution of written data and staged segments",
                        prometheus::MetricType::Histogram,
                        {std::move(m)}
                );
            }

            std::vector<prometheus::ClientMetric> inc_loads_m;
            std::vector<prometheus::ClientMetric> inc_hops_m;
            if (sb.incomplete_chain_loads > 0) {
                inc_loads_m.push_back(
                        make_counter_metric({{"mode", "list"}}, static_cast<double>(sb.incomplete_chain_loads))
                );
            }
            if (sb.incomplete_iteration_loads > 0) {
                inc_loads_m.push_back(
                        make_counter_metric({{"mode", "iteration"}}, static_cast<double>(sb.incomplete_iteration_loads))
                );
            }
            if (sb.incomplete_chain_hops > 0) {
                inc_hops_m.push_back(
                        make_counter_metric({{"mode", "list"}}, static_cast<double>(sb.incomplete_chain_hops))
                );
            }
            if (sb.incomplete_iteration_segments > 0) {
                inc_hops_m.push_back(make_counter_metric(
                        {{"mode", "iteration"}}, static_cast<double>(sb.incomplete_iteration_segments)
                ));
            }
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_incomplete_chain_loads_total",
                    "Total incomplete/staged segment list or iteration loads",
                    prometheus::MetricType::Counter,
                    std::move(inc_loads_m)
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_incomplete_chain_hops_total",
                    "Total incomplete append chain hops or staged segments traversed",
                    prometheus::MetricType::Counter,
                    std::move(inc_hops_m)
            );

            std::vector<prometheus::ClientMetric> comp_cnt_m;
            std::vector<prometheus::ClientMetric> comp_segs_m;
            std::vector<prometheus::ClientMetric> comp_dur_m;
            for (const auto& [k, entry] : sb.compactions) {
                if (entry.count == 0) {
                    continue;
                }
                const char* kind_str = to_string(static_cast<CompactionKind>(k));
                comp_cnt_m.push_back(make_counter_metric({{"kind", kind_str}}, static_cast<double>(entry.count)));
                if (entry.input_segments > 0) {
                    comp_segs_m.push_back(make_counter_metric(
                            {{"kind", kind_str}, {"direction", "in"}}, static_cast<double>(entry.input_segments)
                    ));
                }
                if (entry.output_segments > 0) {
                    comp_segs_m.push_back(make_counter_metric(
                            {{"kind", kind_str}, {"direction", "out"}}, static_cast<double>(entry.output_segments)
                    ));
                }
                if (entry.dur.count > 0) {
                    comp_dur_m.push_back(make_histogram_metric({{"kind", kind_str}}, entry.dur, DEFAULT_PROM_BUCKETS));
                }
            }
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_compactions_total",
                    "Total ArcticDB stream/defragment/symbol-list compactions",
                    prometheus::MetricType::Counter,
                    std::move(comp_cnt_m)
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_compaction_segments_total",
                    "Total input and output segments/keys processed by compactions",
                    prometheus::MetricType::Counter,
                    std::move(comp_segs_m)
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_compaction_duration_seconds",
                    "Duration of ArcticDB compactions in seconds",
                    prometheus::MetricType::Histogram,
                    std::move(comp_dur_m)
            );

            std::vector<prometheus::ClientMetric> conf_m;
            for (size_t i = 0; i < sb.conflicts.size(); ++i) {
                if (sb.conflicts[i] > 0) {
                    conf_m.push_back(make_counter_metric(
                            {{"kind", to_string(static_cast<WriteConflictKind>(i))}},
                            static_cast<double>(sb.conflicts[i])
                    ));
                }
            }
            push_family_if_nonempty(
                    out,
                    "arcticdb_stream_write_conflicts_total",
                    "Total version/symbol-list write conflicts and optimistic load retries",
                    prometheus::MetricType::Counter,
                    std::move(conf_m)
            );
        }

        // Scrape-time live resource gauges (zero hot-path cost) & LMDB map-full counter
        const auto res = sample_live_resources();
        if (res.has_pool_gauges) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_pool_pending_tasks",
                    "Current queued pending tasks in ArcticDB CPU/IO thread pools",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({{"pool", "cpu"}}, static_cast<double>(res.cpu_pool.pending_tasks)),
                     make_gauge_metric({{"pool", "io"}}, static_cast<double>(res.io_pool.pending_tasks))}
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_pool_threads",
                    "Current active, total, and max thread counts in ArcticDB CPU/IO thread pools",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric(
                             {{"pool", "cpu"}, {"state", "active"}}, static_cast<double>(res.cpu_pool.active_threads)
                     ),
                     make_gauge_metric(
                             {{"pool", "cpu"}, {"state", "total"}}, static_cast<double>(res.cpu_pool.total_threads)
                     ),
                     make_gauge_metric(
                             {{"pool", "cpu"}, {"state", "max"}}, static_cast<double>(res.cpu_pool.max_threads)
                     ),
                     make_gauge_metric(
                             {{"pool", "io"}, {"state", "active"}}, static_cast<double>(res.io_pool.active_threads)
                     ),
                     make_gauge_metric(
                             {{"pool", "io"}, {"state", "total"}}, static_cast<double>(res.io_pool.total_threads)
                     ),
                     make_gauge_metric({{"pool", "io"}, {"state", "max"}}, static_cast<double>(res.io_pool.max_threads))
                    }
            );
        }
        if (res.write_semaphore_limit > 0 || res.write_semaphore_in_flight > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_async_store_write_semaphore_in_flight",
                    "Current in-flight compaction writes holding the async store backpressure semaphore",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({}, static_cast<double>(res.write_semaphore_in_flight))}
            );
        }
        if (res.write_semaphore_limit > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_async_store_write_semaphore_limit",
                    "Configured concurrency limit for the async store compaction write semaphore",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({}, static_cast<double>(res.write_semaphore_limit))}
            );
        }
        if (res.version_map_entries > 0 || res.config_cache_libraries > 0 || res.config_cache_storages > 0) {
            std::vector<prometheus::ClientMetric> cache_entries_m;
            if (res.version_map_entries > 0) {
                cache_entries_m.push_back(
                        make_gauge_metric({{"cache", "version_map"}}, static_cast<double>(res.version_map_entries))
                );
            }
            if (res.config_cache_libraries > 0) {
                cache_entries_m.push_back(make_gauge_metric(
                        {{"cache", "config_cache_libraries"}}, static_cast<double>(res.config_cache_libraries)
                ));
            }
            if (res.config_cache_storages > 0) {
                cache_entries_m.push_back(make_gauge_metric(
                        {{"cache", "config_cache_storages"}}, static_cast<double>(res.config_cache_storages)
                ));
            }
            push_family_if_nonempty(
                    out,
                    "arcticdb_cache_entries",
                    "Current live entry counts in ArcticDB in-memory caches",
                    prometheus::MetricType::Gauge,
                    std::move(cache_entries_m)
            );
        }
        if (res.lmdb.open_envs > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_lmdb_open_envs",
                    "Current number of open LMDB environments in this process",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({}, static_cast<double>(res.lmdb.open_envs))}
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_lmdb_map_bytes",
                    "Aggregate configured map size and used bytes across open LMDB environments",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({{"kind", "size"}}, static_cast<double>(res.lmdb.map_size_bytes)),
                     make_gauge_metric({{"kind", "used"}}, static_cast<double>(res.lmdb.used_bytes))}
            );
            push_family_if_nonempty(
                    out,
                    "arcticdb_lmdb_active_readers",
                    "Current active reader slots across open LMDB environments",
                    prometheus::MetricType::Gauge,
                    {make_gauge_metric({}, static_cast<double>(res.lmdb.active_readers))}
            );
        }
        if (merged.lmdb_map_full_errors > 0) {
            push_family_if_nonempty(
                    out,
                    "arcticdb_lmdb_map_full_errors_total",
                    "Total LMDB MDB_MAP_FULL errors encountered",
                    prometheus::MetricType::Counter,
                    {make_counter_metric({}, static_cast<double>(merged.lmdb_map_full_errors))}
            );
        }

        return out;
    }

  private:
    bool include_library_{false};
    std::shared_ptr<RetiredState> retired_;
    // Declared last so it is destroyed first (after retired_->alive = false in ~PromCollectableState).
    folly::ThreadLocal<std::unique_ptr<ThreadLocalEntry>, PromSinkTlsTag> tls_;
};

class PrometheusStatsSink final : public StatsSink {
  public:
    PrometheusStatsSink(std::shared_ptr<PrometheusInstance> prom, bool include_library) :
        prom_(prom ? std::move(prom) : PrometheusInstance::instance()),
        state_(std::make_shared<PromCollectableState>(include_library)) {
        if (prom_) {
            prom_->registerCollectable(state_);
        }
    }

    ~PrometheusStatsSink() override {
        if (prom_) {
            prom_->unregisterCollectable(state_);
        }
    }

    void on_storage_op(
            StorageBackend backend, StorageOp op, std::string_view /*key_type*/, StorageOutcome outcome,
            uint64_t duration_ns, uint64_t bytes, uint64_t /*items*/, std::string_view library
    ) override {
        const uint32_t op_key = pack_storage_op_outcome(backend, op, outcome);
        const uint16_t byte_key = pack_backend_op(backend, op);
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = ld.storage_ops[op_key];
            entry.ops += 1;
            entry.dur.observe_ns(duration_ns, DEFAULT_PROM_BUCKETS);
            if (bytes > 0) {
                ld.storage_bytes[byte_key] += bytes;
            }
        });
    }

    void on_storage_page(StorageBackend backend, StorageOp op, std::string_view library) override {
        const uint16_t key = pack_backend_op(backend, op);
        state_->mutate_local([&](PromBucket& b) { state_->lib_data(b, library).storage_pages[key] += 1; });
    }

    void on_partial_delete_retry(StorageBackend backend, std::string_view library) override {
        const auto idx = static_cast<size_t>(backend);
        if (idx >= static_cast<size_t>(StorageBackend::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) { state_->lib_data(b, library).partial_delete_retries[idx] += 1; });
    }

    void on_http_attempt(
            std::string_view service, std::string_view operation, HttpStatusClass status_class,
            uint64_t /*attempt_duration_ns*/, uint64_t new_connections, std::string_view library
    ) override {
        const auto s_idx = static_cast<size_t>(status_class);
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            if (s_idx < entry.attempts.size()) {
                entry.attempts[s_idx] += 1;
            }
            entry.new_connections += new_connections;
        });
    }

    void on_http_request_finish(
            std::string_view service, std::string_view operation, uint64_t total_request_duration_ns,
            std::string_view library
    ) override {
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            entry.req_dur.observe_ns(total_request_duration_ns, DEFAULT_PROM_BUCKETS);
        });
    }

    void on_http_retry(
            std::string_view service, std::string_view operation, uint64_t backoff_ns, std::string_view library
    ) override {
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            entry.retries += 1;
            entry.backoff_ns += backoff_ns;
        });
    }

    void on_http_backoff(
            std::string_view service, std::string_view operation, uint64_t backoff_ns, std::string_view library
    ) override {
        if (backoff_ns == 0) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            entry.backoff_ns += backoff_ns;
        });
    }

    void on_http_checksum_mismatch_retry(std::string_view service, std::string_view operation, std::string_view library)
            override {
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            entry.checksum_mismatch_retries += 1;
        });
    }

    void on_http_throttled(std::string_view service, std::string_view operation, std::string_view library) override {
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            entry.throttled += 1;
        });
    }

    void on_http_phase_timings(
            std::string_view service, std::string_view operation, const HttpAttemptTimings& timings,
            std::string_view library
    ) override {
        if (!timings.has_timings) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            auto& ld = state_->lib_data(b, library);
            auto& entry = prom_find_or_emplace(prom_find_or_emplace(ld.http, service), operation);
            if (timings.dns_ns > 0) {
                entry.phase_dur[0].observe_ns(timings.dns_ns, FAST_PROM_BUCKETS);
            }
            if (timings.tcp_connect_ns > 0) {
                entry.phase_dur[1].observe_ns(timings.tcp_connect_ns, FAST_PROM_BUCKETS);
            }
            if (timings.tls_handshake_ns > 0) {
                entry.phase_dur[2].observe_ns(timings.tls_handshake_ns, FAST_PROM_BUCKETS);
            }
            entry.phase_dur[3].observe_ns(timings.ttfb_ns, FAST_PROM_BUCKETS);
            entry.phase_dur[4].observe_ns(timings.transfer_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_pool_task(PoolKind pool, uint64_t wait_ns, uint64_t run_ns) override {
        const auto idx = static_cast<size_t>(pool);
        if (idx >= static_cast<size_t>(PoolKind::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            b.pool_tasks[idx] += 1;
            b.pool_wait[idx].observe_ns(wait_ns, FAST_PROM_BUCKETS);
            b.pool_run[idx].observe_ns(run_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_pool_task_expired(PoolKind pool) override {
        const auto idx = static_cast<size_t>(pool);
        if (idx >= static_cast<size_t>(PoolKind::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) { b.pool_expired[idx] += 1; });
    }

    void on_semaphore_wait(uint64_t wait_ns) override {
        state_->mutate_local([&](PromBucket& b) {
            b.semaphore_waits += 1;
            b.semaphore_wait.observe_ns(wait_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_version_map_cache(bool hit, bool is_reload) override {
        state_->mutate_local([&](PromBucket& b) {
            b.cache_lookups[0][hit ? 1 : 0] += 1;
            if (is_reload) {
                b.version_map_reloads += 1;
            }
        });
    }

    void on_version_map_chain_hops(uint64_t hops) override {
        if (hops == 0) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) { b.version_map_chain_hops += hops; });
    }

    void on_version_map_ref_retry() override {
        state_->mutate_local([&](PromBucket& b) { b.version_map_ref_retries += 1; });
    }

    void on_dedup_lookup(bool hit, uint64_t bytes_saved) override {
        state_->mutate_local([&](PromBucket& b) {
            b.cache_lookups[1][hit ? 1 : 0] += 1;
            if (hit) {
                b.dedup_bytes_saved += bytes_saved;
            }
        });
    }

    void on_config_cache(bool hit) override {
        state_->mutate_local([&](PromBucket& b) { b.cache_lookups[2][hit ? 1 : 0] += 1; });
    }

    void on_codec_encode(uint64_t duration_ns, uint64_t uncompressed_bytes, uint64_t compressed_bytes) override {
        state_->mutate_local([&](PromBucket& b) {
            b.codec_ops[0] += 1;
            b.codec_bytes[0][0] += uncompressed_bytes;
            b.codec_bytes[0][1] += compressed_bytes;
            b.codec_dur[0].observe_ns(duration_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_codec_decode(uint64_t duration_ns, uint64_t compressed_bytes, uint64_t uncompressed_bytes) override {
        state_->mutate_local([&](PromBucket& b) {
            b.codec_ops[1] += 1;
            b.codec_bytes[1][0] += uncompressed_bytes;
            b.codec_bytes[1][1] += compressed_bytes;
            b.codec_dur[1].observe_ns(duration_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_query_pruning(
            uint64_t segments_total, uint64_t segments_pruned_index, uint64_t segments_pruned_column_stats,
            uint64_t segments_read, uint64_t columns_total, uint64_t columns_projected, uint64_t rows_total,
            uint64_t rows_selected
    ) override {
        state_->mutate_local([&](PromBucket& b) {
            b.query_reads += 1;
            b.query_segments[0] += segments_total;
            b.query_segments[1] += segments_pruned_index;
            b.query_segments[2] += segments_pruned_column_stats;
            b.query_segments[3] += segments_read;
            b.query_columns[0] += columns_total;
            b.query_columns[1] += columns_projected;
            b.query_rows[0] += rows_total;
            b.query_rows[1] += rows_selected;
        });
    }

    void on_lock_event(
            LockKind kind, bool acquired, uint64_t conflicts, uint64_t retries, bool timed_out, uint64_t wait_ns
    ) override {
        const auto idx = static_cast<size_t>(kind);
        if (idx >= static_cast<size_t>(LockKind::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            if (acquired) {
                b.lock_events[idx][0] += 1;
            }
            b.lock_events[idx][1] += conflicts;
            b.lock_events[idx][2] += retries;
            if (timed_out) {
                b.lock_events[idx][3] += 1;
            }
            b.lock_wait[idx].observe_ns(wait_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_api_op(ApiOp op, bool ok, uint64_t duration_ns, std::string_view library) override {
        const uint8_t key = pack_api_op_outcome(op, ok);
        state_->mutate_local([&](PromBucket& b) {
            auto& entry = state_->lib_data(b, library).api_ops[key];
            entry.ops += 1;
            entry.dur.observe_ns(duration_ns, DEFAULT_PROM_BUCKETS);
        });
    }

    void on_boundary_op(BoundaryPhase phase, uint64_t duration_ns) override {
        const auto idx = static_cast<size_t>(phase);
        if (idx >= static_cast<size_t>(BoundaryPhase::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            b.boundary_ops[idx] += 1;
            b.boundary_dur[idx].observe_ns(duration_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_clause_exec(ClauseKind kind, uint64_t duration_ns, uint64_t rows_in, uint64_t rows_out) override {
        const auto key = static_cast<uint8_t>(kind);
        state_->mutate_local([&](PromBucket& b) {
            auto& entry = b.clauses[key];
            entry.executions += 1;
            entry.rows_in += rows_in;
            entry.rows_out += rows_out;
            entry.dur.observe_ns(duration_ns, FAST_PROM_BUCKETS);
        });
    }

    void on_admission_unit(bool waited, uint64_t wait_ns) override {
        state_->mutate_local([&](PromBucket& b) {
            b.admission_units += 1;
            if (waited) {
                b.admission_residency_waits += 1;
                b.admission_residency_wait.observe_ns(wait_ns, FAST_PROM_BUCKETS);
            }
        });
    }

    void on_admission_segment(bool waited, uint64_t wait_ns) override {
        state_->mutate_local([&](PromBucket& b) {
            b.admission_segments += 1;
            if (waited) {
                b.admission_read_window_waits += 1;
                b.admission_read_window_wait.observe_ns(wait_ns, FAST_PROM_BUCKETS);
            }
        });
    }

    void on_segment_written(uint64_t rows, bool staged) override {
        const bool is_small = (rows > 0 && rows < StreamWriteStats::SMALL_SEGMENT_ROW_THRESHOLD);
        state_->mutate_local([&](PromBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            if (staged) {
                b.stream->staged_segments_written += 1;
            } else {
                b.stream->data_segments_written += 1;
            }
            if (is_small) {
                b.stream->small_segments_written += 1;
            }
            b.stream->rows_per_segment.record(rows);
        });
    }

    void on_incomplete_chain_load(uint64_t segments_or_hops, bool via_iteration) override {
        state_->mutate_local([&](PromBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            if (via_iteration) {
                b.stream->incomplete_iteration_loads += 1;
                b.stream->incomplete_iteration_segments += segments_or_hops;
            } else {
                b.stream->incomplete_chain_loads += 1;
                b.stream->incomplete_chain_hops += segments_or_hops;
            }
        });
    }

    void on_compaction(CompactionKind kind, uint64_t input_segments, uint64_t output_segments, uint64_t duration_ns)
            override {
        const auto idx = static_cast<size_t>(kind);
        if (idx >= static_cast<size_t>(CompactionKind::COUNT)) {
            return;
        }
        const auto key = static_cast<uint8_t>(kind);
        state_->mutate_local([&](PromBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            auto& entry = b.stream->compactions[key];
            entry.count += 1;
            entry.input_segments += input_segments;
            entry.output_segments += output_segments;
            entry.dur.observe_ns(duration_ns, DEFAULT_PROM_BUCKETS);
        });
    }

    void on_write_conflict(WriteConflictKind kind) override {
        const auto idx = static_cast<size_t>(kind);
        if (idx >= static_cast<size_t>(WriteConflictKind::COUNT)) {
            return;
        }
        state_->mutate_local([&](PromBucket& b) {
            if (!b.stream.has_value()) {
                b.stream.emplace();
            }
            b.stream->conflicts[idx] += 1;
        });
    }

    void on_lmdb_map_full() override {
        state_->mutate_local([&](PromBucket& b) { b.lmdb_map_full_errors += 1; });
    }

  private:
    std::shared_ptr<PrometheusInstance> prom_;
    std::shared_ptr<PromCollectableState> state_;
};

} // namespace

void set_stats_sink(std::shared_ptr<StatsSink> sink) {
    ActiveSinkNode* old_node = nullptr;
    {
        std::lock_guard<std::mutex> lock(g_sink_write_mutex);
        old_node = g_active_sink.load(std::memory_order_relaxed);
        const bool had_sink = old_node != nullptr;
        const bool will_have_sink = sink != nullptr;
        ActiveSinkNode* new_node = will_have_sink ? new ActiveSinkNode{std::move(sink)} : nullptr;
        g_active_sink.store(new_node, std::memory_order_release);
        if (!had_sink && will_have_sink) {
            detail::active_collector_count.fetch_add(1, std::memory_order_relaxed);
        } else if (had_sink && !will_have_sink) {
            detail::active_collector_count.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    if (old_node != nullptr) {
        {
            auto accessor = g_sink_reader_slots.accessAllThreads();
            for (auto& slot : accessor) {
                std::lock_guard<std::mutex> guard(slot.mutex);
            }
        }
        delete old_node;
    }
}

std::shared_ptr<StatsSink> get_stats_sink() {
    std::lock_guard<std::mutex> lock(g_sink_write_mutex);
    auto* node = g_active_sink.load(std::memory_order_relaxed);
    return node ? node->sink : nullptr;
}

std::shared_ptr<StatsSink> make_prometheus_stats_sink(
        std::shared_ptr<PrometheusInstance> prometheus, bool include_library
) {
    return std::make_shared<PrometheusStatsSink>(std::move(prometheus), include_library);
}

void enable_prometheus_sink(bool include_library) {
    set_stats_sink(make_prometheus_stats_sink(nullptr, include_library));
}

void disable_prometheus_sink() { set_stats_sink(nullptr); }

// ---------------------------------------------------------------------------
// Backward-compatible QueryStats singleton & RAIIAddTime
// ---------------------------------------------------------------------------

std::shared_ptr<QueryStats> QueryStats::instance_;
std::once_flag QueryStats::init_flag_;

std::shared_ptr<QueryStats> QueryStats::instance() {
    std::call_once(init_flag_, []() { instance_ = std::make_shared<QueryStats>(); });
    return instance_;
}

QueryStats::QueryStats() : global_collector_(std::make_shared<QueryStatsCollector>(CollectorOptions{})) {
    g_global_collector_raw.store(global_collector_->impl_.get(), std::memory_order_release);
}

QueryStats::~QueryStats() {
    disable();
    g_global_collector_raw.store(nullptr, std::memory_order_release);
}

void QueryStats::reset_stats() { global_collector_->reset(); }

void QueryStats::enable() {
    bool expected = false;
    if (is_enabled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        g_global_enabled.store(true, std::memory_order_release);
        detail::active_collector_count.fetch_add(1, std::memory_order_relaxed);
    }
}

void QueryStats::disable() {
    bool expected = true;
    if (is_enabled_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        g_global_enabled.store(false, std::memory_order_release);
        detail::active_collector_count.fetch_sub(1, std::memory_order_relaxed);
    }
}

bool QueryStats::is_enabled() const { return is_enabled_.load(std::memory_order_acquire); }

void QueryStats::add(TaskType task_type, entity::KeyType key_type, StatType stat_type, uint64_t value) {
    if (!is_enabled()) {
        return;
    }
    if (task_type >= TaskType::END || key_type >= entity::KeyType::UNDEFINED) {
        return;
    }
    global_collector_->impl_->mutate_local([&](StatsBucket& b) {
        auto& leg = b.legacy[static_cast<size_t>(task_type)][static_cast<size_t>(key_type)];
        switch (stat_type) {
        case StatType::TOTAL_TIME_MS:
            leg.total_time_ns += value;
            break;
        case StatType::COUNT:
            leg.count += value;
            break;
        case StatType::SIZE_BYTES:
            leg.size_bytes += value;
            break;
        default:
            internal::raise<ErrorCode::E_INVALID_ARGUMENT>("Invalid stat type");
        }
    });
}

[[nodiscard]] std::optional<RAIIAddTime> QueryStats::add_task_count_and_time(
        TaskType task_type, entity::KeyType key_type, std::optional<TimePoint> start
) {
    if (!is_enabled()) {
        return std::nullopt;
    }
    add(task_type, key_type, StatType::COUNT, 1);
    return std::make_optional<RAIIAddTime>(task_type, key_type, start.value_or(std::chrono::steady_clock::now()));
}

QueryStats::QueryStatsOutput QueryStats::get_stats() const { return global_collector_->impl_->legacy_snapshot(); }

QueryStatsSnapshot QueryStats::get_structured_snapshot() const { return global_collector_->snapshot(); }

RAIIAddTime::RAIIAddTime(TaskType task_type, entity::KeyType key_type, TimePoint start) :
    task_type_(task_type),
    key_type_(key_type),
    start_(start),
    active_(true) {}

RAIIAddTime::RAIIAddTime(RAIIAddTime&& other) noexcept :
    task_type_(other.task_type_),
    key_type_(other.key_type_),
    start_(other.start_),
    active_(std::exchange(other.active_, false)) {}

RAIIAddTime& RAIIAddTime::operator=(RAIIAddTime&& other) noexcept {
    if (this != &other) {
        task_type_ = other.task_type_;
        key_type_ = other.key_type_;
        start_ = other.start_;
        active_ = std::exchange(other.active_, false);
    }
    return *this;
}

RAIIAddTime::~RAIIAddTime() {
    if (!active_) {
        return;
    }
    const auto elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count()
    );
    QueryStats::instance()->add(task_type_, key_type_, StatType::TOTAL_TIME_MS, elapsed_ns);
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
