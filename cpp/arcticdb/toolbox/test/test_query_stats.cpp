/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/base_task.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/storage/config_cache.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/version/admission_handler.hpp>

#include <folly/executors/QueuedImmediateExecutor.h>
#include <gtest/gtest.h>
#include <prometheus/text_serializer.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>
#include <vector>

namespace arcticdb::query_stats {
namespace {

using arcticdb::entity::KeyType;

struct SimpleTask : public async::BaseTask {
    std::function<void()> fn;
    explicit SimpleTask(std::function<void()> f) : fn(std::move(f)) {}
    void operator()() { fn(); }
};

TEST(QueryStatsCore, DisabledFastPath) {
    EXPECT_FALSE(is_active());
    record_storage_op(StorageBackend::S3, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 1000, 128, 1);
    record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 5000, 1);
    EXPECT_FALSE(is_active());
}

TEST(QueryStatsCore, LatencyHistogramAccuracyAndDiff) {
    LatencyHistogram h;
    EXPECT_TRUE(h.empty());
    EXPECT_EQ(h.percentile_ns(0.5), 0U);

    // Single sample is exact across all percentiles.
    h.record(12'345'678ULL);
    EXPECT_EQ(h.count, 1U);
    EXPECT_EQ(h.min_ns, 12'345'678ULL);
    EXPECT_EQ(h.max_ns, 12'345'678ULL);
    EXPECT_EQ(h.percentile_ns(0.5), 12'345'678ULL);
    EXPECT_EQ(h.percentile_ns(0.95), 12'345'678ULL);
    EXPECT_EQ(h.percentile_ns(0.99), 12'345'678ULL);

    // Uniform 1..1000 ms (in ns)
    LatencyHistogram h1;
    for (uint64_t ms = 1; ms <= 1000; ++ms) {
        h1.record(ms * 1'000'000ULL);
    }
    EXPECT_EQ(h1.count, 1000U);
    EXPECT_EQ(h1.min_ns, 1'000'000ULL);
    EXPECT_EQ(h1.max_ns, 1'000'000'000ULL);
    EXPECT_NEAR(h1.mean_ms(), 500.5, 0.01);
    EXPECT_NEAR(h1.p50_ms(), 500.0, 40.0);
    EXPECT_NEAR(h1.p95_ms(), 950.0, 50.0);
    EXPECT_NEAR(h1.p99_ms(), 990.0, 50.0);

    // Sub-microsecond and overflow (>274s) samples
    LatencyHistogram extremes;
    extremes.record(0ULL);
    extremes.record(500ULL);
    extremes.record(400'000'000'000ULL);
    EXPECT_EQ(extremes.count, 3U);
    EXPECT_EQ(extremes.min_ns, 0U);
    EXPECT_EQ(extremes.max_ns, 400'000'000'000ULL);

    // Merge and diff
    LatencyHistogram h2 = h1;
    for (uint64_t ms = 1001; ms <= 1200; ++ms) {
        h2.record(ms * 1'000'000ULL);
    }
    LatencyHistogram delta = h2.diff(h1);
    EXPECT_EQ(delta.count, 200U);
    EXPECT_NEAR(delta.mean_ms(), 1100.5, 0.01);
    EXPECT_GE(delta.p50_ms(), 1000.0);
    EXPECT_LE(delta.p50_ms(), 1200.0);
}

TEST(QueryStatsCore, ThreadIsolation) {
    CollectorSession session;
    EXPECT_TRUE(is_active());

    record_storage_op(
            StorageBackend::S3, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 2'000'000ULL, 1024, 1, "lib_a"
    );

    // Unrelated thread without this RequestContext must not pollute session.
    std::thread unrelated([]() {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                9'000'000ULL,
                4096,
                1,
                "lib_other"
        );
    });
    unrelated.join();

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.storage.contains(StorageBackend::S3));
    const auto& rd = snap.storage.at(StorageBackend::S3).at(StorageOp::Read).at("TABLE_DATA");
    EXPECT_EQ(rd.ok, 1U);
    EXPECT_EQ(rd.bytes, 1024U);
    EXPECT_EQ(snap.by_library.size(), 1U);
    EXPECT_TRUE(snap.by_library.contains("lib_a"));
    EXPECT_FALSE(snap.by_library.contains("lib_other"));
}

TEST(QueryStatsCore, NestingAndIsolate) {
    CollectorSession outer(CollectorOptions{.by_library = true, .by_symbol = false, .isolate = false});
    record_storage_op(StorageBackend::Lmdb, StorageOp::Read, KeyType::VERSION_REF, StorageOutcome::Ok, 1000, 10, 1);

    // Non-isolated inner records into both inner and outer.
    {
        CollectorSession inner(CollectorOptions{.by_library = true, .by_symbol = false, .isolate = false});
        record_storage_op(StorageBackend::Lmdb, StorageOp::Read, KeyType::VERSION_REF, StorageOutcome::Ok, 2000, 20, 1);

        const auto inner_snap = inner.snapshot();
        EXPECT_EQ(inner_snap.storage.at(StorageBackend::Lmdb).at(StorageOp::Read).at("VERSION_REF").ok, 1U);
        EXPECT_EQ(inner_snap.storage.at(StorageBackend::Lmdb).at(StorageOp::Read).at("VERSION_REF").bytes, 20U);
    }

    // Isolated inner records ONLY into inner, NOT outer.
    {
        CollectorSession isolated_inner(CollectorOptions{.by_library = true, .by_symbol = false, .isolate = true});
        record_storage_op(
                StorageBackend::Lmdb, StorageOp::Write, KeyType::TABLE_DATA, StorageOutcome::Ok, 3000, 300, 1
        );

        const auto iso_snap = isolated_inner.snapshot();
        EXPECT_EQ(iso_snap.storage.at(StorageBackend::Lmdb).at(StorageOp::Write).at("TABLE_DATA").ok, 1U);
    }

    const auto outer_snap = outer.snapshot();
    EXPECT_EQ(outer_snap.storage.at(StorageBackend::Lmdb).at(StorageOp::Read).at("VERSION_REF").ok, 2U);
    EXPECT_EQ(outer_snap.storage.at(StorageBackend::Lmdb).at(StorageOp::Read).at("VERSION_REF").bytes, 30U);
    EXPECT_FALSE(outer_snap.storage.at(StorageBackend::Lmdb).contains(StorageOp::Write));
}

TEST(QueryStatsCore, RetiredThreadsFlushIntoSnapshot) {
    CollectorSession session;
    auto captured = capture_context();

    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 25;
    std::vector<std::thread> workers;
    workers.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; ++t) {
        workers.emplace_back([captured]() {
            RestoredContextGuard guard(captured);
            for (int i = 0; i < kOpsPerThread; ++i) {
                record_storage_op(
                        StorageBackend::Memory,
                        StorageOp::Write,
                        KeyType::TABLE_DATA,
                        StorageOutcome::Ok,
                        5000,
                        64,
                        1,
                        "lib_mt"
                );
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    // All worker threads have exited; retired stats must still be preserved.
    const auto snap = session.snapshot();
    const auto& wr = snap.storage.at(StorageBackend::Memory).at(StorageOp::Write).at("TABLE_DATA");
    EXPECT_EQ(wr.ok, static_cast<uint64_t>(kNumThreads * kOpsPerThread));
    EXPECT_EQ(wr.bytes, static_cast<uint64_t>(kNumThreads * kOpsPerThread * 64));
}

TEST(QueryStatsCore, ConcurrentWritersAndSnapshotsAreSafe) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true, .max_symbols = 50});
    auto captured = capture_context();
    std::atomic<bool> done{false};

    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([captured, t]() {
            RestoredContextGuard guard(captured);
            for (int i = 0; i < 500; ++i) {
                const std::string sym = "sym_" + std::to_string((t * 30 + i) % 80);
                record_storage_op(
                        StorageBackend::S3,
                        StorageOp::Read,
                        KeyType::TABLE_DATA,
                        StorageOutcome::Ok,
                        1000,
                        128,
                        1,
                        "lib_concurrent",
                        sym
                );
            }
        });
    }

    std::thread reader([&]() {
        while (!done.load(std::memory_order_acquire)) {
            auto snap = session.snapshot();
            (void)snap.wall_time_ms;
        }
    });

    for (auto& w : writers) {
        w.join();
    }
    done.store(true, std::memory_order_release);
    reader.join();

    const auto final_snap = session.snapshot();
    EXPECT_EQ(final_snap.storage.at(StorageBackend::S3).at(StorageOp::Read).at("TABLE_DATA").ok, 2000U);
    EXPECT_LE(final_snap.by_symbol.size(), 51U); // <= max_symbols + "__other__"
    EXPECT_TRUE(final_snap.by_symbol.contains("__other__"));
    EXPECT_GT(final_snap.dropped_symbols, 0U);
}

TEST(QueryStatsCore, SymbolAttributionAndMaxSymbolsCap) {
    // Non-symbol key types must not be treated as symbols.
    EXPECT_TRUE(is_symbol_scoped_key_type(KeyType::TABLE_DATA));
    EXPECT_TRUE(is_symbol_scoped_key_type(KeyType::VERSION_REF));
    EXPECT_TRUE(is_symbol_scoped_key_type(KeyType::COLUMN_STATS));
    EXPECT_FALSE(is_symbol_scoped_key_type(KeyType::SYMBOL_LIST));
    EXPECT_FALSE(is_symbol_scoped_key_type(KeyType::LOCK));
    EXPECT_FALSE(is_symbol_scoped_key_type(KeyType::SNAPSHOT_REF));
    EXPECT_FALSE(is_symbol_scoped_key_type(KeyType::LIBRARY_CONFIG));

    // Default by_symbol=false collects zero symbol entries.
    {
        CollectorSession s(CollectorOptions{.by_library = true, .by_symbol = false});
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                1000,
                100,
                1,
                "lib1",
                "AAPL"
        );
        const auto snap = s.snapshot();
        EXPECT_TRUE(snap.by_symbol.empty());
        EXPECT_TRUE(snap.by_library.at("lib1").by_symbol.empty());
    }

    // Opt-in by_symbol=true with max_symbols=3 spills extras into "__other__".
    {
        CollectorSession s(CollectorOptions{.by_library = true, .by_symbol = true, .max_symbols = 3});
        for (int i = 0; i < 8; ++i) {
            const std::string sym = "SYM_" + std::to_string(i);
            // Repeat 3 ops per symbol to ensure dropped_symbols counts distinct symbols, not ops.
            for (int op = 0; op < 3; ++op) {
                record_storage_op(
                        StorageBackend::S3,
                        StorageOp::Read,
                        KeyType::TABLE_DATA,
                        StorageOutcome::Ok,
                        1000,
                        50,
                        1,
                        "lib1",
                        sym
                );
            }
        }
        const auto snap = s.snapshot();
        EXPECT_EQ(snap.by_symbol.size(), 4U); // 3 named + "__other__"
        ASSERT_TRUE(snap.by_symbol.contains("__other__"));
        EXPECT_EQ(snap.by_symbol.at("__other__").reads, 15U); // 5 spilled symbols * 3 ops
        EXPECT_EQ(snap.dropped_symbols, 5U);
        EXPECT_EQ(snap.by_library.at("lib1").dropped_symbols, 5U);
    }
}

TEST(QueryStatsCore, TaskSchedulerCpuAndIoContextPropagation) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
    ContextTagScope tag("lib_sched", "MSFT");

    auto cpu_fut = async::submit_cpu_task(SimpleTask([]() {
        record_storage_op(StorageBackend::S3, StorageOp::Read, KeyType::TABLE_INDEX, StorageOutcome::Ok, 2000, 256, 1);
    }));
    auto io_fut = async::submit_io_task(SimpleTask([]() {
        record_storage_op(StorageBackend::S3, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 4000, 512, 1);
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 3000, 0);
        record_http_request_finish("S3", "GetObject", 3500);
    }));

    cpu_fut.wait();
    io_fut.wait();

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.by_library.contains("lib_sched"));
    ASSERT_TRUE(snap.by_symbol.contains("MSFT"));
    EXPECT_EQ(snap.by_symbol.at("MSFT").reads, 2U);
    EXPECT_EQ(snap.by_symbol.at("MSFT").bytes_read, 768U);
    EXPECT_EQ(snap.by_symbol.at("MSFT").http_requests, 1U);
    EXPECT_EQ(snap.http.at("s3").at("GetObject").requests, 1U);
}

TEST(QueryStatsCore, LegacyGlobalApiCompatibility) {
    auto qs = QueryStats::instance();
    struct LegacyCleanup {
        std::shared_ptr<QueryStats> qs;
        ~LegacyCleanup() {
            qs->disable();
            qs->reset_stats();
        }
    } cleanup{qs};
    qs->reset_stats();
    qs->enable();

    {
        auto timer = add_task_count_and_time(TaskType::S3_GetObject, KeyType::TABLE_DATA);
        add(TaskType::S3_GetObject, KeyType::TABLE_DATA, StatType::SIZE_BYTES, 1234);
    }
    record_storage_op(
            StorageBackend::S3, StorageOp::Write, KeyType::TABLE_INDEX, StorageOutcome::Ok, 2'000'000, 500, 1
    );

    auto legacy = qs->get_stats();
    qs->disable();
    qs->reset_stats();

    ASSERT_TRUE(legacy.contains("storage_operations"));
    EXPECT_EQ(legacy["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"], 1U);
    EXPECT_EQ(legacy["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["size_bytes"], 1234U);
    EXPECT_EQ(legacy["storage_operations"]["S3_PutObject"]["TABLE_INDEX"]["count"], 1U);
    EXPECT_EQ(legacy["storage_operations"]["S3_PutObject"]["TABLE_INDEX"]["size_bytes"], 500U);
    EXPECT_EQ(legacy["storage_operations"]["S3_PutObject"]["TABLE_INDEX"]["total_time_ms"], 2U);
}

TEST(QueryStatsCore, TaskSchedulerPoolMetricsExactCountAndIsolation) {
    CollectorSession session;

    auto cpu_fut =
            async::submit_cpu_task(SimpleTask([]() { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }));
    auto io_fut =
            async::submit_io_task(SimpleTask([]() { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }));
    cpu_fut.wait();
    io_fut.wait();

    // Unrelated thread must not contribute to this session's pool counts.
    std::thread unrelated([]() {
        auto f = async::submit_cpu_task(SimpleTask([]() {}));
        f.wait();
    });
    unrelated.join();

    // Direct continuation via cpu_executor should also be recorded once.
    auto via_fut = folly::makeFuture().via(&async::cpu_executor()).thenValue([](folly::Unit) { return 42; });
    EXPECT_EQ(std::move(via_fut).get(), 42);

    record_semaphore_wait(250'000ULL);

    const auto snap = session.snapshot();
    // 1 submit_cpu_task + 1 via(&cpu_executor) = 2 (verifies no double-counting in SchedulerWrapper::add)
    EXPECT_EQ(snap.thread_pools.cpu.tasks, 2U);
    EXPECT_EQ(snap.thread_pools.cpu.wait_latency.count, 2U);
    EXPECT_EQ(snap.thread_pools.cpu.run_latency.count, 2U);
    EXPECT_GT(snap.thread_pools.cpu.run_latency.sum_ns, 0U);

    EXPECT_EQ(snap.thread_pools.io.tasks, 1U);
    EXPECT_EQ(snap.thread_pools.io.wait_latency.count, 1U);
    EXPECT_EQ(snap.thread_pools.io.run_latency.count, 1U);
    EXPECT_GT(snap.thread_pools.io.run_latency.sum_ns, 0U);

    EXPECT_EQ(snap.thread_pools.semaphore_waits, 1U);
    EXPECT_EQ(snap.thread_pools.semaphore_wait_latency.count, 1U);
    EXPECT_EQ(snap.thread_pools.semaphore_wait_latency.sum_ns, 250'000ULL);
}

TEST(QueryStatsCore, Phase3SubsystemMetricsMergeAndReset) {
    CollectorSession session;
    auto captured = capture_context();

    record_version_map_cache(/*hit=*/true, /*is_reload=*/false);
    record_version_map_cache(/*hit=*/false, /*is_reload=*/true);
    record_version_map_chain_hops(3);
    record_version_map_ref_retry();
    record_dedup_lookup(/*hit=*/true, 4096);
    record_dedup_lookup(/*hit=*/false, 0);
    record_config_cache(/*hit=*/true);
    record_config_cache(/*hit=*/false);
    record_codec_encode(500'000ULL, 10'000, 2'500);
    record_codec_decode(200'000ULL, 2'500, 10'000);
    record_query_pruning(10, 4, 3, 3, 20, 5, 1000, 300);
    record_lock_event(LockKind::StorageLock, /*acquired=*/true, 2, 2, /*timed_out=*/false, 1'000'000ULL);

    std::thread worker([captured]() {
        RestoredContextGuard guard(captured);
        record_version_map_cache(/*hit=*/true, /*is_reload=*/false);
        record_dedup_lookup(/*hit=*/true, 1024);
        record_codec_encode(100'000ULL, 2'000, 500);
        record_lock_event(LockKind::ReliableStorageLock, /*acquired=*/false, 1, 0, /*timed_out=*/true, 5'000'000ULL);
    });
    worker.join();

    const auto snap = session.snapshot();
    EXPECT_EQ(snap.caches.version_map.hits, 2U);
    EXPECT_EQ(snap.caches.version_map.misses, 1U);
    EXPECT_EQ(snap.caches.version_map.reloads, 1U);
    EXPECT_EQ(snap.caches.version_map.chain_hops, 3U);
    EXPECT_EQ(snap.caches.version_map.ref_retries, 1U);

    EXPECT_EQ(snap.caches.dedup_map.hits, 2U);
    EXPECT_EQ(snap.caches.dedup_map.misses, 1U);
    EXPECT_EQ(snap.caches.dedup_map.bytes_saved, 5120U);

    EXPECT_EQ(snap.caches.config_cache.hits, 1U);
    EXPECT_EQ(snap.caches.config_cache.misses, 1U);

    EXPECT_EQ(snap.codec.encodes, 2U);
    EXPECT_EQ(snap.codec.decodes, 1U);
    EXPECT_EQ(snap.codec.encode_uncompressed_bytes, 12'000U);
    EXPECT_EQ(snap.codec.encode_compressed_bytes, 3'000U);
    EXPECT_NEAR(snap.codec.encode_compression_ratio(), 4.0, 1e-6);
    EXPECT_NEAR(snap.codec.decode_compression_ratio(), 4.0, 1e-6);
    EXPECT_EQ(snap.codec.encode_latency.count, 2U);
    EXPECT_EQ(snap.codec.decode_latency.count, 1U);

    EXPECT_EQ(snap.query.queries, 1U);
    EXPECT_EQ(snap.query.segments_total, 10U);
    EXPECT_EQ(snap.query.segments_pruned_index, 4U);
    EXPECT_EQ(snap.query.segments_pruned_column_stats, 3U);
    EXPECT_EQ(snap.query.segments_read, 3U);
    EXPECT_EQ(snap.query.columns_total, 20U);
    EXPECT_EQ(snap.query.columns_projected, 5U);
    EXPECT_EQ(snap.query.rows_total, 1000U);
    EXPECT_EQ(snap.query.rows_selected, 300U);

    ASSERT_TRUE(snap.locks.contains(LockKind::StorageLock));
    EXPECT_EQ(snap.locks.at(LockKind::StorageLock).acquires, 1U);
    EXPECT_EQ(snap.locks.at(LockKind::StorageLock).conflicts, 2U);
    EXPECT_EQ(snap.locks.at(LockKind::StorageLock).retries, 2U);
    EXPECT_EQ(snap.locks.at(LockKind::StorageLock).timeouts, 0U);

    ASSERT_TRUE(snap.locks.contains(LockKind::ReliableStorageLock));
    EXPECT_EQ(snap.locks.at(LockKind::ReliableStorageLock).acquires, 0U);
    EXPECT_EQ(snap.locks.at(LockKind::ReliableStorageLock).conflicts, 1U);
    EXPECT_EQ(snap.locks.at(LockKind::ReliableStorageLock).timeouts, 1U);

    session.collector()->reset();
    const auto reset_snap = session.snapshot();
    EXPECT_TRUE(reset_snap.caches.empty());
    EXPECT_TRUE(reset_snap.codec.empty());
    EXPECT_TRUE(reset_snap.query.empty());
    EXPECT_TRUE(reset_snap.locks.empty());
}

TEST(QueryStatsCore, ConfigCacheInstrumentation) {
    auto resolver = std::make_shared<storage::details::InMemoryConfigResolver>();
    proto::storage::LibraryDescriptor desc;
    desc.set_name("present_lib");
    resolver->add_library(storage::EnvironmentName{"test_env"}, desc);
    storage::ConfigCache cache(storage::EnvironmentName{"test_env"}, resolver);

    CollectorSession session;
    EXPECT_TRUE(cache.get_descriptor(storage::LibraryPath::from_delim_path("present_lib")).has_value());
    EXPECT_FALSE(cache.get_descriptor(storage::LibraryPath::from_delim_path("missing_lib")).has_value());

    const auto snap = session.snapshot();
    EXPECT_EQ(snap.caches.config_cache.hits, 1U);
    EXPECT_EQ(snap.caches.config_cache.misses, 1U);
}

TEST(QueryStatsCore, PrometheusStatsSinkForwardsMetricsWithoutSymbolLabel) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_test_job", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    ASSERT_FALSE(is_active());
    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    EXPECT_TRUE(is_active());

    {
        ContextTagScope tag("prom_lib", "SENSITIVE_HIGH_CARD_SYMBOL");
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                2'500'000ULL,
                2048,
                1,
                "prom_lib",
                "SENSITIVE_HIGH_CARD_SYMBOL"
        );
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::VERSION_REF,
                StorageOutcome::NotFound,
                500'000ULL,
                0,
                1,
                "prom_lib",
                "SENSITIVE_HIGH_CARD_SYMBOL"
        );
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status5xx, 1'000'000ULL, 1);
        record_http_retry("S3", "GetObject", 500'000ULL);
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 2'000'000ULL, 0);
        record_http_request_finish("S3", "GetObject", 3'500'000ULL);
    }

    const auto families = prom->get_metrics();
    bool saw_storage_ops = false;
    bool saw_storage_bytes = false;
    bool saw_storage_dur = false;
    bool saw_http_attempts = false;
    bool saw_http_retries = false;
    bool saw_http_dur = false;

    for (const auto& family : families) {
        for (const auto& metric : family.metric) {
            for (const auto& lbl : metric.label) {
                EXPECT_NE(lbl.name, "symbol") << "Prometheus metric must never emit a symbol label";
                EXPECT_NE(lbl.value, "SENSITIVE_HIGH_CARD_SYMBOL");
            }
        }
        if (family.name == "arcticdb_storage_ops_total") {
            saw_storage_ops = true;
            double total = 0.0;
            for (const auto& m : family.metric) {
                total += m.counter.value;
            }
            EXPECT_DOUBLE_EQ(total, 2.0);
        } else if (family.name == "arcticdb_storage_bytes_total") {
            saw_storage_bytes = true;
            ASSERT_EQ(family.metric.size(), 1U);
            EXPECT_DOUBLE_EQ(family.metric[0].counter.value, 2048.0);
        } else if (family.name == "arcticdb_storage_op_duration_seconds") {
            saw_storage_dur = true;
            uint64_t count = 0;
            for (const auto& m : family.metric) {
                count += m.histogram.sample_count;
            }
            EXPECT_EQ(count, 2U);
        } else if (family.name == "arcticdb_http_attempts_total") {
            saw_http_attempts = true;
            double total = 0.0;
            for (const auto& m : family.metric) {
                total += m.counter.value;
            }
            EXPECT_DOUBLE_EQ(total, 2.0);
        } else if (family.name == "arcticdb_http_retries_total") {
            saw_http_retries = true;
            ASSERT_EQ(family.metric.size(), 1U);
            EXPECT_DOUBLE_EQ(family.metric[0].counter.value, 1.0);
        } else if (family.name == "arcticdb_http_request_duration_seconds") {
            saw_http_dur = true;
            ASSERT_EQ(family.metric.size(), 1U);
            EXPECT_EQ(family.metric[0].histogram.sample_count, 1U);
        }
    }

    EXPECT_TRUE(saw_storage_ops);
    EXPECT_TRUE(saw_storage_bytes);
    EXPECT_TRUE(saw_storage_dur);
    EXPECT_TRUE(saw_http_attempts);
    EXPECT_TRUE(saw_http_retries);
    EXPECT_TRUE(saw_http_dur);

    set_stats_sink(nullptr);
    EXPECT_FALSE(is_active());
}

TEST(QueryStatsCore, PrometheusStatsSinkForwardsAllSubsystemMetrics) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_sub_job", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));

    {
        ContextTagScope tag("sub_lib", "SECRET_SYM");
        record_storage_page(StorageBackend::S3, StorageOp::List, KeyType::VERSION, 100'000ULL, 10, "sub_lib");
        record_partial_delete_retry(StorageBackend::S3, "TABLE_DATA", "sub_lib");
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 1'000'000ULL, 2, 250'000ULL);
        record_http_retry("S3", "GetObject", 500'000ULL);
        record_checksum_mismatch_retry("S3", "GetObject");

        record_pool_task(PoolKind::Cpu, 100'000ULL, 500'000ULL);
        record_pool_task(PoolKind::Io, 200'000ULL, 1'000'000ULL);
        record_pool_task_expired(PoolKind::Io);
        record_semaphore_wait(300'000ULL);

        record_version_map_cache(true, false);
        record_version_map_cache(false, true);
        record_version_map_chain_hops(3);
        record_version_map_ref_retry();
        record_dedup_lookup(true, 4096);
        record_dedup_lookup(false, 0);
        record_config_cache(true);
        record_config_cache(false);

        record_codec_encode(400'000ULL, 10'000, 2'500);
        record_codec_decode(200'000ULL, 2'500, 10'000);

        record_query_pruning(10, 4, 3, 3, 8, 2, 1000, 150);

        record_lock_event(LockKind::StorageLock, true, 1, 2, false, 1'500'000ULL);
        record_lock_event(LockKind::ReliableStorageLock, false, 1, 1, true, 5'000'000ULL);
    }

    std::unordered_map<std::string, prometheus::MetricFamily> by_name;
    for (const auto& family : prom->get_metrics()) {
        for (const auto& metric : family.metric) {
            for (const auto& lbl : metric.label) {
                EXPECT_NE(lbl.name, "symbol");
                EXPECT_NE(lbl.value, "SECRET_SYM");
            }
        }
        by_name.emplace(family.name, family);
    }

    auto sum_counter = [&](const std::string& name) -> double {
        auto it = by_name.find(name);
        if (it == by_name.end()) {
            return -1.0;
        }
        double total = 0.0;
        for (const auto& m : it->second.metric) {
            total += m.counter.value;
        }
        return total;
    };

    auto sum_hist_count = [&](const std::string& name) -> uint64_t {
        auto it = by_name.find(name);
        if (it == by_name.end()) {
            return 0U;
        }
        uint64_t total = 0;
        for (const auto& m : it->second.metric) {
            total += m.histogram.sample_count;
        }
        return total;
    };

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_storage_pages_total"), 1.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_storage_partial_delete_retries_total"), 1.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_http_new_connections_total"), 2.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_http_checksum_mismatch_retries_total"), 1.0);
    EXPECT_NEAR(sum_counter("arcticdb_http_backoff_seconds_total"), 0.00075, 1e-9);

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_pool_tasks_total"), 2.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_pool_tasks_expired_total"), 1.0);
    EXPECT_EQ(sum_hist_count("arcticdb_pool_task_wait_duration_seconds"), 2U);
    EXPECT_EQ(sum_hist_count("arcticdb_pool_task_run_duration_seconds"), 2U);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_async_store_semaphore_waits_total"), 1.0);
    EXPECT_EQ(sum_hist_count("arcticdb_async_store_semaphore_wait_duration_seconds"), 1U);

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_cache_lookups_total"), 6.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_version_map_reloads_total"), 1.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_version_map_chain_hops_total"), 3.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_version_map_ref_retries_total"), 1.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_dedup_bytes_saved_total"), 4096.0);

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_codec_ops_total"), 2.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_codec_bytes_total"), 25'000.0);
    EXPECT_EQ(sum_hist_count("arcticdb_codec_duration_seconds"), 2U);

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_query_reads_total"), 1.0);
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_query_segments_total"), 20.0); // 10 + 4 + 3 + 3
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_query_columns_total"), 10.0);  // 8 + 2
    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_query_rows_total"), 1150.0);   // 1000 + 150

    EXPECT_DOUBLE_EQ(sum_counter("arcticdb_lock_events_total"), 7.0); // 1+1+2 + 1+1+1
    EXPECT_EQ(sum_hist_count("arcticdb_lock_wait_duration_seconds"), 2U);

    // Verify Prometheus text exposition format (monotonic buckets + single +Inf per histogram series)
    prometheus::TextSerializer serializer;
    const std::string exposition = serializer.Serialize(prom->get_metrics());
    EXPECT_NE(exposition.find("arcticdb_pool_tasks_total{pool=\"cpu\"} 1"), std::string::npos);
    EXPECT_NE(
            exposition.find("arcticdb_pool_task_run_duration_seconds_bucket{pool=\"cpu\",le=\"+Inf\"} 1"),
            std::string::npos
    );
    EXPECT_EQ(exposition.find("SECRET_SYM"), std::string::npos);
    EXPECT_EQ(exposition.find("symbol="), std::string::npos);
}

TEST(QueryStatsCore, PrometheusStatsSinkConcurrentWritersRetiredThreadsAndScrapes) {
    auto prom = std::make_shared<PrometheusInstance>();
    // Also verify enable-before-configure works cleanly
    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    prom->configure(MetricsConfig{"localhost", "9091", "qs_mt_job", "inst", "test", MetricsConfig::Model::PUSH});

    constexpr int NUM_WRITERS = 4;
    constexpr int ITERS_PER_WRITER = 2000;
    std::atomic<bool> done{false};

    std::thread scraper([&]() {
        while (!done.load(std::memory_order_acquire)) {
            auto families = prom->get_metrics();
            (void)families;
        }
    });

    std::vector<std::thread> writers;
    writers.reserve(NUM_WRITERS);
    for (int t = 0; t < NUM_WRITERS; ++t) {
        writers.emplace_back([t]() {
            const std::string lib = "mt_lib_" + std::to_string(t);
            for (int i = 0; i < ITERS_PER_WRITER; ++i) {
                record_storage_op(
                        StorageBackend::Memory,
                        StorageOp::Read,
                        KeyType::TABLE_DATA,
                        StorageOutcome::Ok,
                        10'000ULL,
                        128,
                        1,
                        lib,
                        "SYM_" + std::to_string(i)
                );
                record_pool_task(PoolKind::Cpu, 1'000ULL, 5'000ULL);
                record_codec_decode(2'000ULL, 64, 256);
                record_version_map_cache(true, false);
            }
        });
    }
    // Join all writers BEFORE final scrape to verify retired-thread flush preserves 100% of counts
    for (auto& w : writers) {
        w.join();
    }
    done.store(true, std::memory_order_release);
    scraper.join();

    double storage_ops = 0.0;
    double storage_bytes = 0.0;
    double pool_tasks = 0.0;
    double codec_ops = 0.0;
    double cache_hits = 0.0;
    uint64_t pool_run_hist_samples = 0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_storage_ops_total") {
            for (const auto& m : family.metric) {
                storage_ops += m.counter.value;
            }
        } else if (family.name == "arcticdb_storage_bytes_total") {
            for (const auto& m : family.metric) {
                storage_bytes += m.counter.value;
            }
        } else if (family.name == "arcticdb_pool_tasks_total") {
            for (const auto& m : family.metric) {
                pool_tasks += m.counter.value;
            }
        } else if (family.name == "arcticdb_pool_task_run_duration_seconds") {
            for (const auto& m : family.metric) {
                pool_run_hist_samples += m.histogram.sample_count;
            }
        } else if (family.name == "arcticdb_codec_ops_total") {
            for (const auto& m : family.metric) {
                codec_ops += m.counter.value;
            }
        } else if (family.name == "arcticdb_cache_lookups_total") {
            for (const auto& m : family.metric) {
                cache_hits += m.counter.value;
            }
        }
    }

    constexpr double EXPECTED = static_cast<double>(NUM_WRITERS * ITERS_PER_WRITER);
    EXPECT_DOUBLE_EQ(storage_ops, EXPECTED);
    EXPECT_DOUBLE_EQ(storage_bytes, EXPECTED * 128.0);
    EXPECT_DOUBLE_EQ(pool_tasks, EXPECTED);
    EXPECT_EQ(pool_run_hist_samples, static_cast<uint64_t>(NUM_WRITERS * ITERS_PER_WRITER));
    EXPECT_DOUBLE_EQ(codec_ops, EXPECTED);
    EXPECT_DOUBLE_EQ(cache_hits, EXPECTED);
}

TEST(QueryStatsCore, HttpSubPhaseTimingsAndThrottlingCollectorAndPrometheus) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_http_phase", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});

    {
        ContextTagScope tag("phase_lib", "AAPL");
        // Attempt 1: cold connection with DNS/TCP/TLS/TTFB + 503 throttled
        HttpAttemptTimings t1{
                .dns_ns = 1'000'000ULL,
                .tcp_connect_ns = 2'000'000ULL,
                .tls_handshake_ns = 3'000'000ULL,
                .ttfb_ns = 10'000'000ULL,
                .transfer_ns = 500'000ULL,
                .has_timings = true
        };
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status5xx, 10'500'000ULL, 1, 0, /*throttled=*/true, t1);

        // Attempt 2: reused keep-alive connection (dns/tcp/tls == 0), 200 OK
        HttpAttemptTimings t2{
                .dns_ns = 0,
                .tcp_connect_ns = 0,
                .tls_handshake_ns = 0,
                .ttfb_ns = 4'000'000ULL,
                .transfer_ns = 2'000'000ULL,
                .has_timings = true
        };
        record_http_attempt(
                "S3", "GetObject", HttpStatusClass::Status2xx, 6'000'000ULL, 0, 5'000'000ULL, /*throttled=*/false, t2
        );
    }

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.http.contains("s3"));
    const auto& st = snap.http.at("s3").at("GetObject");
    EXPECT_EQ(st.throttled, 1U);
    EXPECT_EQ(st.dns_latency.count, 1U);
    EXPECT_EQ(st.tcp_connect_latency.count, 1U);
    EXPECT_EQ(st.tls_handshake_latency.count, 1U);
    EXPECT_EQ(st.ttfb_latency.count, 2U);
    EXPECT_EQ(st.transfer_latency.count, 2U);
    EXPECT_NEAR(st.ttfb_latency.total_ms(), 14.0, 0.01);
    EXPECT_NEAR(st.transfer_latency.total_ms(), 2.5, 0.01);
    ASSERT_TRUE(snap.by_library.contains("phase_lib"));
    EXPECT_EQ(snap.by_library.at("phase_lib").http.at("s3").at("GetObject").throttled, 1U);

    bool saw_throttled = false;
    std::unordered_map<std::string, uint64_t> phase_counts;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_http_throttled_total") {
            saw_throttled = true;
            ASSERT_EQ(family.metric.size(), 1U);
            EXPECT_DOUBLE_EQ(family.metric[0].counter.value, 1.0);
        } else if (family.name == "arcticdb_http_phase_duration_seconds") {
            for (const auto& m : family.metric) {
                for (const auto& lbl : m.label) {
                    if (lbl.name == "phase") {
                        phase_counts[lbl.value] += m.histogram.sample_count;
                    }
                }
            }
        }
    }
    EXPECT_TRUE(saw_throttled);
    EXPECT_EQ(phase_counts["dns"], 1U);
    EXPECT_EQ(phase_counts["tcp_connect"], 1U);
    EXPECT_EQ(phase_counts["tls_handshake"], 1U);
    EXPECT_EQ(phase_counts["ttfb"], 2U);
    EXPECT_EQ(phase_counts["transfer"], 2U);
}

TEST(QueryStatsCore, ApiOpScopeRedMetricsNestingAndPrometheus) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_api_red", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});

    // 1. Successful Read with nested inner Delete scope (inner must NOT double-count)
    {
        ApiOpScope outer(ApiOp::Read, "api_lib");
        {
            ApiOpScope nested(ApiOp::Delete, "api_lib");
        }
    }
    // 2. Error Write via thrown exception
    try {
        ApiOpScope err_scope(ApiOp::Write, "api_lib");
        throw std::runtime_error("simulated api failure");
    } catch (const std::runtime_error&) {
    }
    // 3. Explicit mark_error on Delete
    {
        ApiOpScope marked(ApiOp::Delete, "api_lib");
        marked.mark_error();
    }

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.api.contains(ApiOp::Read));
    EXPECT_EQ(snap.api.at(ApiOp::Read).ok, 1U);
    EXPECT_EQ(snap.api.at(ApiOp::Read).error, 0U);
    EXPECT_EQ(snap.api.at(ApiOp::Read).latency.count, 1U);

    ASSERT_TRUE(snap.api.contains(ApiOp::Write));
    EXPECT_EQ(snap.api.at(ApiOp::Write).ok, 0U);
    EXPECT_EQ(snap.api.at(ApiOp::Write).error, 1U);
    EXPECT_EQ(snap.api.at(ApiOp::Write).error_latency.count, 1U);

    ASSERT_TRUE(snap.api.contains(ApiOp::Delete));
    EXPECT_EQ(snap.api.at(ApiOp::Delete).ok, 0U);
    EXPECT_EQ(snap.api.at(ApiOp::Delete).error, 1U);

    ASSERT_TRUE(snap.by_library.contains("api_lib"));
    EXPECT_EQ(snap.by_library.at("api_lib").api.at(ApiOp::Read).ok, 1U);
    EXPECT_EQ(snap.by_library.at("api_lib").api.at(ApiOp::Write).error, 1U);

    double api_ops_total = 0.0;
    uint64_t api_dur_samples = 0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_api_ops_total") {
            for (const auto& m : family.metric) {
                api_ops_total += m.counter.value;
            }
        } else if (family.name == "arcticdb_api_op_duration_seconds") {
            for (const auto& m : family.metric) {
                api_dur_samples += m.histogram.sample_count;
            }
        }
    }
    EXPECT_DOUBLE_EQ(api_ops_total, 3.0);
    EXPECT_EQ(api_dur_samples, 3U);
}

TEST(QueryStatsCore, BoundaryOpScopeAndPrometheus) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_boundary", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});

    record_boundary_op(BoundaryPhase::Normalize, 150'000ULL);
    record_boundary_op(BoundaryPhase::Denormalize, 250'000ULL);
    record_boundary_op(BoundaryPhase::GilWait, 50'000ULL);
    {
        BoundaryOpScope in_scope(BoundaryPhase::CppInput);
        BoundaryOpScope out_scope(BoundaryPhase::CppOutput);
    }

    const auto snap = session.snapshot();
    ASSERT_EQ(snap.boundary.size(), 5U);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::Normalize).count, 1U);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::Normalize).latency.sum_ns, 150'000ULL);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::Denormalize).count, 1U);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::GilWait).count, 1U);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::CppInput).count, 1U);
    EXPECT_EQ(snap.boundary.at(BoundaryPhase::CppOutput).count, 1U);

    double boundary_ops_total = 0.0;
    uint64_t boundary_dur_samples = 0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_boundary_ops_total") {
            for (const auto& m : family.metric) {
                boundary_ops_total += m.counter.value;
            }
        } else if (family.name == "arcticdb_boundary_duration_seconds") {
            for (const auto& m : family.metric) {
                boundary_dur_samples += m.histogram.sample_count;
            }
        }
    }
    EXPECT_DOUBLE_EQ(boundary_ops_total, 5.0);
    EXPECT_EQ(boundary_dur_samples, 5U);
}

TEST(QueryStatsCore, ProcessingClausesAdmissionAndPrometheus) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_processing", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});

    record_clause_exec(ClauseKind::Filter, 120'000ULL, 1000, 250);
    record_clause_exec(ClauseKind::Filter, 80'000ULL, 500, 100);
    record_clause_exec(ClauseKind::Aggregation, 300'000ULL, 350, 10);
    record_admission_unit(/*waited=*/false, 0);
    record_admission_unit(/*waited=*/true, 500'000ULL);
    record_admission_segment(/*waited=*/false, 0);
    record_admission_segment(/*waited=*/true, 200'000ULL);

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.processing.clauses.contains(ClauseKind::Filter));
    EXPECT_EQ(snap.processing.clauses.at(ClauseKind::Filter).executions, 2U);
    EXPECT_EQ(snap.processing.clauses.at(ClauseKind::Filter).rows_in, 1500U);
    EXPECT_EQ(snap.processing.clauses.at(ClauseKind::Filter).rows_out, 350U);
    EXPECT_EQ(snap.processing.clauses.at(ClauseKind::Filter).latency.count, 2U);

    ASSERT_TRUE(snap.processing.clauses.contains(ClauseKind::Aggregation));
    EXPECT_EQ(snap.processing.clauses.at(ClauseKind::Aggregation).rows_out, 10U);

    EXPECT_EQ(snap.processing.admission.units_admitted, 2U);
    EXPECT_EQ(snap.processing.admission.residency_waits, 1U);
    EXPECT_EQ(snap.processing.admission.segments_dispatched, 2U);
    EXPECT_EQ(snap.processing.admission.read_window_waits, 1U);
    EXPECT_EQ(snap.processing.admission.residency_wait_latency.count, 1U);
    EXPECT_EQ(snap.processing.admission.read_window_wait_latency.count, 1U);

    double clause_execs = 0.0;
    double clause_rows = 0.0;
    uint64_t clause_dur_samples = 0;
    double admission_events = 0.0;
    uint64_t admission_wait_samples = 0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_processing_clause_executions_total") {
            for (const auto& m : family.metric) {
                clause_execs += m.counter.value;
            }
        } else if (family.name == "arcticdb_processing_clause_rows_total") {
            for (const auto& m : family.metric) {
                clause_rows += m.counter.value;
            }
        } else if (family.name == "arcticdb_processing_clause_duration_seconds") {
            for (const auto& m : family.metric) {
                clause_dur_samples += m.histogram.sample_count;
            }
        } else if (family.name == "arcticdb_admission_events_total") {
            for (const auto& m : family.metric) {
                admission_events += m.counter.value;
            }
        } else if (family.name == "arcticdb_admission_wait_duration_seconds") {
            for (const auto& m : family.metric) {
                admission_wait_samples += m.histogram.sample_count;
            }
        }
    }
    EXPECT_DOUBLE_EQ(clause_execs, 3.0);
    EXPECT_DOUBLE_EQ(clause_rows, 2210.0); // (1500+350) + (350+10)
    EXPECT_EQ(clause_dur_samples, 3U);
    EXPECT_DOUBLE_EQ(admission_events, 6.0); // 2 units + 1 residency_wait + 2 segments + 1 read_window_wait
    EXPECT_EQ(admission_wait_samples, 2U);
}

TEST(QueryStatsCore, ThreadExitRacingWithCollectorAndSinkDestruction) {
    // Regression test for UAF when exiting threads run folly::ThreadLocal
    // onThreadExit concurrently with QueryStatsCollector::Impl / PromCollectableState destruction.
    for (int iter = 0; iter < 50; ++iter) {
        auto prom = std::make_shared<PrometheusInstance>();
        prom->configure(MetricsConfig{"localhost", "9091", "qs_race", "inst", "test", MetricsConfig::Model::PUSH});
        set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));

        auto recorded = std::make_shared<std::atomic<int>>(0);
        constexpr int kThreads = 4;
        std::vector<std::thread> workers;
        workers.reserve(kThreads);
        {
            CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
            ContextTagScope tag("race_lib", "SYM_RACE");
            auto ctx = capture_context();

            for (int t = 0; t < kThreads; ++t) {
                workers.emplace_back([ctx, recorded, t]() {
                    {
                        RestoredContextGuard guard(ctx);
                        record_storage_op(
                                StorageBackend::S3,
                                StorageOp::Read,
                                entity::KeyType::TABLE_DATA,
                                StorageOutcome::Ok,
                                1000ULL,
                                64ULL,
                                1ULL,
                                "race_lib",
                                "SYM_" + std::to_string(t)
                        );
                    }
                    // Signal after releasing RequestContext so main destroys session while
                    // this thread is tearing down its folly::ThreadLocal entries.
                    recorded->fetch_add(1, std::memory_order_release);
                });
            }

            while (recorded->load(std::memory_order_acquire) < kThreads) {
                std::this_thread::yield();
            }
        }
        set_stats_sink(nullptr);
        for (auto& w : workers) {
            w.join();
        }
    }
}

TEST(QueryStatsCore, StreamWritePathologiesAndPrometheus) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_stream", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});

    record_segment_written(/*rows=*/50, /*staged=*/false);
    record_segment_written(/*rows=*/200, /*staged=*/true);
    record_segment_written(/*rows=*/50'000, /*staged=*/false);
    record_incomplete_chain_load(/*segments_or_hops=*/4, /*via_iteration=*/false);
    record_incomplete_chain_load(/*segments_or_hops=*/3, /*via_iteration=*/true);
    record_compaction(CompactionKind::Incomplete, 4, 1, 2'000'000ULL);
    record_compaction(CompactionKind::SymbolList, 12, 1, 500'000ULL);
    record_write_conflict(WriteConflictKind::NonIncreasingVersion);
    record_write_conflict(WriteConflictKind::ConcurrentSymbolListCompaction);

    const auto snap = session.snapshot();
    EXPECT_EQ(snap.stream.data_segments_written, 2U);
    EXPECT_EQ(snap.stream.staged_segments_written, 1U);
    EXPECT_EQ(snap.stream.small_segments_written, 2U);
    EXPECT_EQ(snap.stream.rows_per_segment.count, 3U);
    EXPECT_EQ(snap.stream.rows_per_segment.sum, 50'250U);
    EXPECT_EQ(snap.stream.rows_per_segment.min_value(), 50U);
    EXPECT_EQ(snap.stream.rows_per_segment.max, 50'000U);
    EXPECT_GE(snap.stream.rows_per_segment.p50(), 50U);
    EXPECT_EQ(snap.stream.incomplete_chain_loads, 1U);
    EXPECT_EQ(snap.stream.incomplete_chain_hops, 4U);
    EXPECT_EQ(snap.stream.incomplete_iteration_loads, 1U);
    EXPECT_EQ(snap.stream.incomplete_iteration_segments, 3U);
    ASSERT_TRUE(snap.stream.compactions.contains(CompactionKind::Incomplete));
    EXPECT_EQ(snap.stream.compactions.at(CompactionKind::Incomplete).count, 1U);
    EXPECT_EQ(snap.stream.compactions.at(CompactionKind::Incomplete).input_segments, 4U);
    EXPECT_EQ(snap.stream.compactions.at(CompactionKind::Incomplete).output_segments, 1U);
    ASSERT_TRUE(snap.stream.compactions.contains(CompactionKind::SymbolList));
    EXPECT_EQ(snap.stream.compactions.at(CompactionKind::SymbolList).input_segments, 12U);
    EXPECT_EQ(snap.stream.conflicts.at(WriteConflictKind::NonIncreasingVersion), 1U);
    EXPECT_EQ(snap.stream.conflicts.at(WriteConflictKind::ConcurrentSymbolListCompaction), 1U);

    double segs_written = 0.0;
    double small_segs = 0.0;
    uint64_t row_hist_samples = 0;
    double inc_loads = 0.0;
    double inc_hops = 0.0;
    double compactions = 0.0;
    double comp_segs = 0.0;
    uint64_t comp_dur_samples = 0;
    double conflicts = 0.0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_stream_segments_written_total") {
            for (const auto& m : family.metric) {
                segs_written += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_small_segments_written_total") {
            for (const auto& m : family.metric) {
                small_segs += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_segment_rows") {
            for (const auto& m : family.metric) {
                row_hist_samples += m.histogram.sample_count;
            }
        } else if (family.name == "arcticdb_stream_incomplete_chain_loads_total") {
            for (const auto& m : family.metric) {
                inc_loads += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_incomplete_chain_hops_total") {
            for (const auto& m : family.metric) {
                inc_hops += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_compactions_total") {
            for (const auto& m : family.metric) {
                compactions += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_compaction_segments_total") {
            for (const auto& m : family.metric) {
                comp_segs += m.counter.value;
            }
        } else if (family.name == "arcticdb_stream_compaction_duration_seconds") {
            for (const auto& m : family.metric) {
                comp_dur_samples += m.histogram.sample_count;
            }
        } else if (family.name == "arcticdb_stream_write_conflicts_total") {
            for (const auto& m : family.metric) {
                conflicts += m.counter.value;
            }
        }
    }
    EXPECT_DOUBLE_EQ(segs_written, 3.0);
    EXPECT_DOUBLE_EQ(small_segs, 2.0);
    EXPECT_EQ(row_hist_samples, 3U);
    EXPECT_DOUBLE_EQ(inc_loads, 2.0);
    EXPECT_DOUBLE_EQ(inc_hops, 7.0);
    EXPECT_DOUBLE_EQ(compactions, 2.0);
    EXPECT_DOUBLE_EQ(comp_segs, 18.0);
    EXPECT_EQ(comp_dur_samples, 2U);
    EXPECT_DOUBLE_EQ(conflicts, 2.0);
}

TEST(QueryStatsCore, HttpSubPhaseTimingsSkipsZeroTtfbAndTransfer) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_http_zero", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});

    // All-zero timings with has_timings=true must emit zero phase samples (no bogus 0s TTFB/transfer).
    HttpAttemptTimings all_zero{.has_timings = true};
    record_http_attempt("S3", "PutObject", HttpStatusClass::Status2xx, 800'000ULL, 0, 0, false, all_zero);

    // Non-zero DNS with zero TTFB/transfer must only record DNS.
    HttpAttemptTimings dns_only{.dns_ns = 1'000'000ULL, .has_timings = true};
    record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 2'000'000ULL, 1, 0, false, dns_only);

    const auto snap = session.snapshot();
    const auto& put_st = snap.http.at("s3").at("PutObject");
    EXPECT_EQ(put_st.ttfb_latency.count, 0U);
    EXPECT_EQ(put_st.transfer_latency.count, 0U);

    const auto& get_st = snap.http.at("s3").at("GetObject");
    EXPECT_EQ(get_st.dns_latency.count, 1U);
    EXPECT_EQ(get_st.ttfb_latency.count, 0U);
    EXPECT_EQ(get_st.transfer_latency.count, 0U);

    std::unordered_map<std::string, uint64_t> phase_counts;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_http_phase_duration_seconds") {
            for (const auto& m : family.metric) {
                for (const auto& lbl : m.label) {
                    if (lbl.name == "phase") {
                        phase_counts[lbl.value] += m.histogram.sample_count;
                    }
                }
            }
        }
    }
    EXPECT_EQ(phase_counts["dns"], 1U);
    EXPECT_EQ(phase_counts["ttfb"], 0U);
    EXPECT_EQ(phase_counts["transfer"], 0U);
}

TEST(QueryStatsCore, ApiOpStageHasSymbolHelpAndDefaultBuckets) {
    EXPECT_STREQ(to_string(ApiOp::Stage), "stage");
    EXPECT_STREQ(to_string(ApiOp::HasSymbol), "has_symbol");

    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_api_ext", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});

    {
        ApiOpScope s1(ApiOp::Stage, "api_ext_lib");
    }
    {
        ApiOpScope s2(ApiOp::HasSymbol, "api_ext_lib");
    }

    const auto snap = session.snapshot();
    ASSERT_TRUE(snap.api.contains(ApiOp::Stage));
    ASSERT_TRUE(snap.api.contains(ApiOp::HasSymbol));
    EXPECT_EQ(snap.api.at(ApiOp::Stage).ok, 1U);
    EXPECT_EQ(snap.api.at(ApiOp::HasSymbol).ok, 1U);
    EXPECT_EQ(snap.by_library.at("api_ext_lib").api.at(ApiOp::Stage).ok, 1U);
    EXPECT_EQ(snap.by_library.at("api_ext_lib").api.at(ApiOp::HasSymbol).ok, 1U);

    prometheus::TextSerializer serializer;
    const std::string exposition = serializer.Serialize(prom->get_metrics());
    EXPECT_NE(
            exposition.find("arcticdb_api_ops_total{library=\"api_ext_lib\",op=\"stage\",outcome=\"ok\"} 1"),
            std::string::npos
    );
    EXPECT_NE(
            exposition.find("arcticdb_api_ops_total{library=\"api_ext_lib\",op=\"has_symbol\",outcome=\"ok\"} 1"),
            std::string::npos
    );
    EXPECT_NE(exposition.find("le=\"1.5\""), std::string::npos);
    EXPECT_NE(exposition.find("# HELP arcticdb_api_op_duration_seconds C++ engine"), std::string::npos);
    EXPECT_NE(exposition.find("arcticdb_boundary_duration_seconds"), std::string::npos);
}

TEST(QueryStatsCore, IoPoolActiveThreadsZeroAtIdleAndTracksBusyTask) {
    // Ensure TaskScheduler is initialized and settled.
    async::submit_io_task(SimpleTask([]() {})).wait();

    const auto idle = sample_live_resources();
    ASSERT_TRUE(idle.has_pool_gauges);
    EXPECT_GE(idle.io_pool.max_threads, 1U);
    EXPECT_EQ(idle.io_pool.active_threads, 0U);

    auto check_busy_io_task = []() {
        auto entered = std::make_shared<std::promise<void>>();
        auto release = std::make_shared<std::promise<void>>();
        auto release_fut = release->get_future().share();
        auto task_fut = async::submit_io_task(SimpleTask([entered, release_fut]() mutable {
            entered->set_value();
            release_fut.wait();
        }));
        entered->get_future().wait();

        const auto busy = sample_live_resources();
        EXPECT_GE(busy.io_pool.active_threads, 1U);
        EXPECT_LE(busy.io_pool.active_threads, busy.io_pool.max_threads);

        release->set_value();
        task_fut.wait();
        EXPECT_EQ(sample_live_resources().io_pool.active_threads, 0U);
    };

    // Verify both disabled and active query_stats paths track busy IO threads cleanly.
    check_busy_io_task();
    {
        CollectorSession session;
        check_busy_io_task();
    }
}

TEST(QueryStatsCore, AdmissionHandlerReadWindowWaitNoRace) {
    using namespace arcticdb::pipelines;
    constexpr size_t kNumSegs = 8;
    const std::vector<std::vector<size_t>> units{{0, 1}, {2, 3}, {4, 5}, {6, 7}};

    std::vector<RangesAndKey> ranges;
    ranges.reserve(kNumSegs);
    for (size_t i = 0; i < kNumSegs; ++i) {
        ranges.emplace_back(RowRange{i, i + 1}, ColRange{0, 1}, entity::AtomKey{});
    }

    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::optional<folly::Promise<SegmentAndSlice>>> promises(kNumSegs);

    stream::SegmentReader reader = [&mu, &cv, &promises](RangesAndKey&& rk) -> folly::Future<SegmentAndSlice> {
        const size_t idx = rk.row_range().first;
        folly::Promise<SegmentAndSlice> p;
        auto f = p.getFuture();
        {
            std::lock_guard<std::mutex> lock(mu);
            promises.at(idx).emplace(std::move(p));
        }
        cv.notify_all();
        return f;
    };

    auto complete_seg = [&mu, &cv, &promises](size_t idx) {
        folly::Promise<SegmentAndSlice> p;
        {
            std::unique_lock<std::mutex> lock(mu);
            cv.wait(lock, [&]() { return promises.at(idx).has_value(); });
            p = std::move(*promises.at(idx));
            promises.at(idx).reset();
        }
        p.setValue(SegmentAndSlice(
                RangesAndKey(RowRange{idx, idx + 1}, ColRange{0, 1}, entity::AtomKey{}), SegmentInMemory{}
        ));
    };

    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    auto admission = std::make_shared<version_store::ProcessingUnitAdmissionHandler>(
            std::move(reader),
            std::move(ranges),
            std::vector<std::vector<size_t>>(units),
            /*max_processing_units_in_flight=*/2,
            /*read_window=*/2
    );

    auto seg_futs = admission->futures();
    admission->admit_initial_processing_units();

    const auto after_init = session.snapshot();
    EXPECT_EQ(after_init.processing.admission.units_admitted, 2U);
    EXPECT_EQ(after_init.processing.admission.residency_waits, 0U);
    EXPECT_EQ(after_init.processing.admission.segments_dispatched, 2U);
    EXPECT_EQ(after_init.processing.admission.read_window_waits, 0U);

    // Race read completions (segs 0..1) against concurrent on_processing_unit_complete() calls,
    // holding seg 2 until units 2..3 are queued so active_reads_ stays at read_window=2 and
    // segments 2..7 are deterministically queued behind read_window=2.
    auto ctx = capture_context();
    std::promise<void> units_enqueued;
    auto units_enqueued_fut = units_enqueued.get_future();
    std::thread unit_completer([admission, ctx, &units_enqueued]() {
        RestoredContextGuard guard(ctx);
        admission->on_processing_unit_complete();
        admission->on_processing_unit_complete();
        units_enqueued.set_value();
    });
    std::thread read_completer([&complete_seg, ctx, &units_enqueued_fut]() {
        RestoredContextGuard guard(ctx);
        complete_seg(0);
        complete_seg(1);
        units_enqueued_fut.wait();
        for (size_t i = 2; i < kNumSegs; ++i) {
            complete_seg(i);
        }
    });

    unit_completer.join();
    read_completer.join();
    auto all_done = folly::collectAll(seg_futs).via(&folly::QueuedImmediateExecutor::instance()).get();
    ASSERT_EQ(all_done.size(), kNumSegs);

    const auto final_snap = session.snapshot();
    EXPECT_EQ(final_snap.processing.admission.units_admitted, 4U);
    EXPECT_EQ(final_snap.processing.admission.residency_waits, 2U);
    EXPECT_EQ(final_snap.processing.admission.segments_dispatched, kNumSegs);
    EXPECT_EQ(final_snap.processing.admission.read_window_waits, kNumSegs - 2U);
    EXPECT_EQ(final_snap.processing.admission.read_window_wait_latency.count, kNumSegs - 2U);
}

TEST(QueryStatsCore, LiveResourceGaugesAndLmdbMapFull) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_res", "inst", "test", MetricsConfig::Model::PUSH});

    struct Cleanup {
        PoolGaugeProvider prev_provider;
        uint64_t prev_sem_limit{0};
        bool provider_swapped{false};
        bool counters_adjusted{false};

        ~Cleanup() {
            set_stats_sink(nullptr);
            if (provider_swapped) {
                set_pool_gauge_provider(std::move(prev_provider));
            }
            detail::set_write_semaphore_limit(prev_sem_limit);
            if (counters_adjusted) {
                detail::adjust_write_semaphore_in_flight(-3);
                detail::adjust_version_map_entries(-42);
                detail::adjust_config_cache_counts(-2, -3);
            }
        }
    } cleanup;
    cleanup.prev_sem_limit = detail::get_write_semaphore_limit();

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    cleanup.prev_provider = set_pool_gauge_provider([](PoolGaugeSnapshot& cpu, PoolGaugeSnapshot& io) {
        cpu.pending_tasks = 5;
        cpu.active_threads = 3;
        cpu.total_threads = 8;
        cpu.max_threads = 8;
        io.pending_tasks = 7;
        io.active_threads = 4;
        io.total_threads = 12;
        io.max_threads = 12;
    });
    cleanup.provider_swapped = true;
    detail::set_write_semaphore_limit(16);
    detail::adjust_write_semaphore_in_flight(3);
    detail::adjust_version_map_entries(42);
    detail::adjust_config_cache_counts(2, 3);
    cleanup.counters_adjusted = true;

    // Large 400 GiB sparse env alongside nearly-full 512 KiB env: max_map_utilization_ratio must catch the full env.
    constexpr uint64_t kLargeMapSize = 400ULL << 30U;
    constexpr uint64_t kLargeUsed = 65536ULL;
    constexpr uint64_t kSmallMapSize = 512ULL * 1024ULL;
    constexpr uint64_t kSmallUsed = 500ULL * 1024ULL;
    const double kExpectedMaxRatio = static_cast<double>(kSmallUsed) / static_cast<double>(kSmallMapSize);

    auto lmdb_handle_large = register_lmdb_env_gauge([](uint64_t& map_size, uint64_t& used, uint64_t& readers) {
        map_size = kLargeMapSize;
        used = kLargeUsed;
        readers = 0ULL;
    });
    auto lmdb_handle_small = register_lmdb_env_gauge([](uint64_t& map_size, uint64_t& used, uint64_t& readers) {
        map_size = kSmallMapSize;
        used = kSmallUsed;
        readers = 2ULL;
    });

    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    record_lmdb_map_full();
    record_lmdb_map_full();

    const auto snap = session.snapshot();
    EXPECT_TRUE(snap.resources.has_pool_gauges);
    EXPECT_EQ(snap.resources.cpu_pool.pending_tasks, 5U);
    EXPECT_EQ(snap.resources.cpu_pool.active_threads, 3U);
    EXPECT_EQ(snap.resources.io_pool.pending_tasks, 7U);
    EXPECT_EQ(snap.resources.io_pool.max_threads, 12U);
    EXPECT_EQ(snap.resources.write_semaphore_in_flight, 3U);
    EXPECT_EQ(snap.resources.write_semaphore_limit, 16U);
    EXPECT_GE(snap.resources.version_map_entries, 42U);
    EXPECT_GE(snap.resources.config_cache_libraries, 2U);
    EXPECT_GE(snap.resources.config_cache_storages, 3U);
    EXPECT_EQ(snap.resources.lmdb.open_envs, 2U);
    EXPECT_EQ(snap.resources.lmdb.map_size_bytes, kLargeMapSize + kSmallMapSize);
    EXPECT_EQ(snap.resources.lmdb.used_bytes, kLargeUsed + kSmallUsed);
    EXPECT_NEAR(snap.resources.lmdb.max_map_utilization_ratio, kExpectedMaxRatio, 1e-6);
    EXPECT_EQ(snap.resources.lmdb.active_readers, 2ULL);
    EXPECT_EQ(snap.resources.lmdb.map_full_errors, 2U);

    bool saw_pending = false;
    bool saw_threads = false;
    bool saw_sem_in_flight = false;
    bool saw_sem_limit = false;
    bool saw_cache_entries = false;
    bool saw_lmdb_envs = false;
    bool saw_lmdb_bytes = false;
    bool saw_lmdb_max_util = false;
    double lmdb_max_util_val = 0.0;
    bool saw_lmdb_readers = false;
    double map_full_val = 0.0;
    for (const auto& family : prom->get_metrics()) {
        if (family.name == "arcticdb_pool_pending_tasks") {
            saw_pending = true;
        } else if (family.name == "arcticdb_pool_threads") {
            saw_threads = true;
        } else if (family.name == "arcticdb_async_store_write_semaphore_in_flight") {
            saw_sem_in_flight = true;
        } else if (family.name == "arcticdb_async_store_write_semaphore_limit") {
            saw_sem_limit = true;
        } else if (family.name == "arcticdb_cache_entries") {
            saw_cache_entries = true;
        } else if (family.name == "arcticdb_lmdb_open_envs") {
            saw_lmdb_envs = true;
        } else if (family.name == "arcticdb_lmdb_map_bytes") {
            saw_lmdb_bytes = true;
        } else if (family.name == "arcticdb_lmdb_max_map_utilization_ratio") {
            saw_lmdb_max_util = true;
            ASSERT_EQ(family.metric.size(), 1U);
            lmdb_max_util_val = family.metric[0].gauge.value;
        } else if (family.name == "arcticdb_lmdb_active_readers") {
            saw_lmdb_readers = true;
        } else if (family.name == "arcticdb_lmdb_map_full_errors_total") {
            for (const auto& m : family.metric) {
                map_full_val += m.counter.value;
            }
        }
    }
    EXPECT_TRUE(saw_pending);
    EXPECT_TRUE(saw_threads);
    EXPECT_TRUE(saw_sem_in_flight);
    EXPECT_TRUE(saw_sem_limit);
    EXPECT_TRUE(saw_cache_entries);
    EXPECT_TRUE(saw_lmdb_envs);
    EXPECT_TRUE(saw_lmdb_bytes);
    EXPECT_TRUE(saw_lmdb_max_util);
    EXPECT_NEAR(lmdb_max_util_val, kExpectedMaxRatio, 1e-6);
    EXPECT_TRUE(saw_lmdb_readers);
    EXPECT_DOUBLE_EQ(map_full_val, 2.0);

    // Verify LMDB handle RAII unregister (gauge counters are restored by Cleanup)
    lmdb_handle_small.reset();
    lmdb_handle_large.reset();
    EXPECT_EQ(sample_live_resources().lmdb.open_envs, 0U);
}

TEST(QueryStatsCore, LocalQueryStatsDoesNotCorruptSingleton) {
    struct Cleanup {
        ~Cleanup() {
            QueryStats::instance()->disable();
            QueryStats::instance()->reset_stats();
        }
    } cleanup;

    auto singleton = QueryStats::instance();
    singleton->reset_stats();
    singleton->enable();
    ASSERT_TRUE(singleton->is_enabled());

    {
        QueryStats local_qs;
        local_qs.enable();
        local_qs.disable();
    }

    EXPECT_TRUE(singleton->is_enabled());
    EXPECT_TRUE(is_active());
    record_storage_op(
            StorageBackend::S3, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 1'000'000ULL, 128, 1
    );
    const auto snap = singleton->get_structured_snapshot();
    ASSERT_TRUE(snap.storage.contains(StorageBackend::S3));
    EXPECT_EQ(snap.storage.at(StorageBackend::S3).at(StorageOp::Read).at("TABLE_DATA").ok, 1U);
}

TEST(QueryStatsCore, PrometheusSinkReEnableProducesNoDuplicateMetricFamilies) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "qs_reenable", "inst", "test", MetricsConfig::Model::PUSH});

    struct SinkCleanup {
        ~SinkCleanup() { set_stats_sink(nullptr); }
    } cleanup;

    auto assert_no_duplicate_families = [&prom]() {
        std::unordered_set<std::string> seen;
        const auto families = prom->get_metrics();
        for (const auto& family : families) {
            EXPECT_TRUE(seen.insert(family.name).second) << "Duplicate Prometheus metric family: " << family.name;
        }
        return families;
    };

    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    record_storage_op(
            StorageBackend::Memory, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 100'000ULL, 64, 1
    );
    assert_no_duplicate_families();

    // Direct re-enable while another sink is active (without explicit disable first)
    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    record_storage_op(
            StorageBackend::Memory, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 200'000ULL, 128, 1, "libA"
    );
    assert_no_duplicate_families();

    // Disable then re-enable again
    set_stats_sink(nullptr);
    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/false));
    record_storage_op(
            StorageBackend::Memory, StorageOp::Write, KeyType::TABLE_DATA, StorageOutcome::Ok, 300'000ULL, 256, 1
    );
    const auto families = assert_no_duplicate_families();
    bool saw_write = false;
    for (const auto& family : families) {
        if (family.name == "arcticdb_storage_ops_total") {
            saw_write = true;
        }
    }
    EXPECT_TRUE(saw_write);
}

TEST(QueryStatsCore, QueryStatsSnapshotDiffExactMonotonicPercentiles) {
    // Case 1: 100 fast ops (1ms) before checkpoint, 1 slow op (100ms) after checkpoint
    {
        CollectorSession session;
        for (int i = 0; i < 100; ++i) {
            record_storage_op(
                    StorageBackend::Memory,
                    StorageOp::Read,
                    KeyType::TABLE_DATA,
                    StorageOutcome::Ok,
                    1'000'000ULL,
                    64,
                    1
            );
            record_segment_written(10, false);
        }
        const auto before = session.snapshot();
        record_storage_op(
                StorageBackend::Memory, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 100'000'000ULL, 64, 1
        );
        record_segment_written(100'000, false);
        const auto after = session.snapshot();

        const auto delta = after.diff(before);
        const auto& lat = delta.storage.at(StorageBackend::Memory).at(StorageOp::Read).at("TABLE_DATA").latency;
        EXPECT_EQ(lat.count, 1U);
        EXPECT_LE(lat.min_ms(), lat.p50_ms());
        EXPECT_LE(lat.p50_ms(), lat.p95_ms());
        EXPECT_LE(lat.p95_ms(), lat.p99_ms());
        EXPECT_LE(lat.p99_ms(), lat.max_ms());
        EXPECT_GE(lat.p50_ms(), 50.0);
        EXPECT_GE(lat.p95_ms(), 50.0);

        const auto& rps = delta.stream.rows_per_segment;
        EXPECT_EQ(rps.count, 1U);
        EXPECT_LE(rps.min_value(), rps.p50());
        EXPECT_LE(rps.p50(), rps.p95());
        EXPECT_LE(rps.p95(), rps.p99());
        EXPECT_LE(rps.p99(), rps.max);
        EXPECT_GE(rps.p50(), 50'000U);
    }

    // Case 2: 100 slow ops (100ms) before checkpoint, 1 fast op (1ms) after checkpoint must NOT leak 100ms outlier
    {
        CollectorSession session;
        for (int i = 0; i < 100; ++i) {
            record_storage_op(
                    StorageBackend::Memory,
                    StorageOp::Read,
                    KeyType::TABLE_DATA,
                    StorageOutcome::Ok,
                    100'000'000ULL,
                    64,
                    1
            );
        }
        const auto before = session.snapshot();
        record_storage_op(
                StorageBackend::Memory, StorageOp::Read, KeyType::TABLE_DATA, StorageOutcome::Ok, 1'000'000ULL, 64, 1
        );
        const auto after = session.snapshot();

        const auto delta = after.diff(before);
        const auto& lat = delta.storage.at(StorageBackend::Memory).at(StorageOp::Read).at("TABLE_DATA").latency;
        EXPECT_EQ(lat.count, 1U);
        EXPECT_LE(lat.min_ms(), lat.p50_ms());
        EXPECT_LE(lat.p50_ms(), lat.p95_ms());
        EXPECT_LE(lat.p95_ms(), lat.p99_ms());
        EXPECT_LE(lat.p99_ms(), lat.max_ms());
        EXPECT_LT(lat.p99_ms(), 10.0);
    }
}

TEST(QueryStatsCore, HistogramNaNAndLargeSpanAndRAIIAddTimeSafety) {
    constexpr uint64_t kHuge = std::numeric_limits<uint64_t>::max() - 1ULL;
    const double kNaN = std::numeric_limits<double>::quiet_NaN();

    // LatencyHistogram: NaN percentile returns 0, and span > INT64_MAX stays monotonic without wraparound.
    LatencyHistogram lat;
    lat.record(0ULL);
    lat.record(kHuge);
    EXPECT_EQ(lat.percentile_ns(kNaN), 0U);
    const uint64_t lat_p50 = lat.percentile_ns(0.50);
    const uint64_t lat_p95 = lat.percentile_ns(0.95);
    const uint64_t lat_p99 = lat.percentile_ns(0.99);
    EXPECT_LE(lat.min_ns, lat_p50);
    EXPECT_LE(lat_p50, lat_p95);
    EXPECT_LE(lat_p95, lat_p99);
    EXPECT_LE(lat_p99, lat.max_ns);
    EXPECT_GT(lat_p95, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));

    // RowCountHistogram: NaN percentile returns 0, and span > INT64_MAX stays monotonic without wraparound.
    RowCountHistogram rows;
    rows.record(0ULL);
    rows.record(kHuge);
    EXPECT_EQ(rows.percentile(kNaN), 0U);
    EXPECT_LE(rows.min_value(), rows.p50());
    EXPECT_LE(rows.p50(), rows.p95());
    EXPECT_LE(rows.p95(), rows.p99());
    EXPECT_LE(rows.p99(), rows.max);
    EXPECT_GT(rows.p95(), static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));

    // QueryStats::add negative KeyType bounds check + RAIIAddTime future start & move-assignment flush.
    auto qs = QueryStats::instance();
    struct LegacyCleanup {
        std::shared_ptr<QueryStats> qs;
        ~LegacyCleanup() {
            qs->disable();
            qs->reset_stats();
        }
    } cleanup{qs};
    qs->reset_stats();
    qs->enable();

    qs->add(TaskType::S3_GetObject, static_cast<KeyType>(-1), StatType::COUNT, 1);
    EXPECT_TRUE(qs->get_stats().empty());

    {
        auto future_timer = qs->add_task_count_and_time(
                TaskType::S3_GetObject, KeyType::TABLE_DATA, std::chrono::steady_clock::now() + std::chrono::seconds(10)
        );
        ASSERT_TRUE(future_timer.has_value());
    }
    const auto after_future = qs->get_stats();
    EXPECT_EQ(after_future.at("storage_operations").at("S3_GetObject").at("TABLE_DATA").at("count"), 1U);
    EXPECT_EQ(after_future.at("storage_operations").at("S3_GetObject").at("TABLE_DATA").at("total_time_ms"), 0U);

    uint64_t flushed_put_ms = 0;
    {
        const auto now = std::chrono::steady_clock::now();
        qs->add(TaskType::S3_PutObject, KeyType::TABLE_INDEX, StatType::COUNT, 1);
        qs->add(TaskType::S3_HeadObject, KeyType::VERSION_REF, StatType::COUNT, 1);
        RAIIAddTime first(TaskType::S3_PutObject, KeyType::TABLE_INDEX, now - std::chrono::milliseconds(25));
        {
            RAIIAddTime second(TaskType::S3_HeadObject, KeyType::VERSION_REF, now - std::chrono::milliseconds(15));
            first = std::move(second);
        }
        const auto mid = qs->get_stats();
        flushed_put_ms = mid.at("storage_operations").at("S3_PutObject").at("TABLE_INDEX").at("total_time_ms");
        EXPECT_EQ(mid.at("storage_operations").at("S3_PutObject").at("TABLE_INDEX").at("count"), 1U);
        EXPECT_GE(flushed_put_ms, 20U);
        EXPECT_EQ(mid.at("storage_operations").at("S3_HeadObject").at("VERSION_REF").at("count"), 1U);
        EXPECT_EQ(mid.at("storage_operations").at("S3_HeadObject").at("VERSION_REF").at("total_time_ms"), 0U);
    }
    const auto final_stats = qs->get_stats();
    EXPECT_EQ(
            final_stats.at("storage_operations").at("S3_PutObject").at("TABLE_INDEX").at("total_time_ms"),
            flushed_put_ms
    );
    EXPECT_GE(final_stats.at("storage_operations").at("S3_HeadObject").at("VERSION_REF").at("total_time_ms"), 10U);
}

} // namespace
} // namespace arcticdb::query_stats
