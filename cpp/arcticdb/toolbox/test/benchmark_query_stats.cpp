/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/toolbox/query_stats.hpp>

#include <benchmark/benchmark.h>

#include <memory>
#include <string>
#include <vector>

namespace arcticdb::query_stats {
namespace {

using arcticdb::entity::KeyType;

storage::KeySegmentPair make_bench_key_segment(const std::string& symbol, size_t rows = 64) {
    StreamDescriptor desc{
            stream_descriptor(StreamId{symbol}, stream::RowCountIndex(), {scalar_field(DataType::INT64, "v")})
    };
    SegmentInMemory sim{desc};
    for (size_t i = 0; i < rows; ++i) {
        sim.set_scalar(0, static_cast<int64_t>(i));
        sim.end_row();
    }
    proto::encoding::VariantCodec codec;
    Segment seg = encode_dispatch(std::move(sim), codec, EncodingVersion::V2);
    auto key = entity::VariantKey{entity::RefKey{StreamId{symbol}, KeyType::VERSION_REF}};
    return storage::KeySegmentPair{std::move(key), std::move(seg)};
}

// ---------------------------------------------------------------------------
// Disabled fast-path overhead (production default when no collector is active)
// ---------------------------------------------------------------------------

void BM_QueryStats_Disabled_RecordStorageOp(benchmark::State& state) {
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_Disabled_RecordStorageOp);

void BM_QueryStats_Disabled_ContextTagScope(benchmark::State& state) {
    for (auto _ : state) {
        ContextTagScope tag("market_data", "AAPL");
        benchmark::DoNotOptimize(tag);
    }
}
BENCHMARK(BM_QueryStats_Disabled_ContextTagScope);

void BM_QueryStats_Disabled_CaptureContext(benchmark::State& state) {
    for (auto _ : state) {
        auto ctx = capture_context();
        benchmark::DoNotOptimize(ctx);
    }
}
BENCHMARK(BM_QueryStats_Disabled_CaptureContext);

// ---------------------------------------------------------------------------
// LatencyHistogram microbenchmarks
// ---------------------------------------------------------------------------

void BM_QueryStats_Histogram_Record(benchmark::State& state) {
    LatencyHistogram h;
    uint64_t ns = 10'000ULL;
    for (auto _ : state) {
        h.record(ns);
        ns = (ns * 1103515245ULL + 12345ULL) & 0x0FFFFFFFULL;
    }
    benchmark::DoNotOptimize(h.count);
}
BENCHMARK(BM_QueryStats_Histogram_Record);

void BM_QueryStats_Histogram_Percentile(benchmark::State& state) {
    LatencyHistogram h;
    for (uint64_t i = 1; i <= 10'000; ++i) {
        h.record(i * 1000ULL);
    }
    for (auto _ : state) {
        auto p95 = h.percentile_ns(0.95);
        benchmark::DoNotOptimize(p95);
    }
}
BENCHMARK(BM_QueryStats_Histogram_Percentile);

// ---------------------------------------------------------------------------
// Active single-threaded recording paths
// ---------------------------------------------------------------------------

void BM_QueryStats_Active_NoAttribution(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_Active_NoAttribution);

void BM_QueryStats_Active_ByLibrary(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_Active_ByLibrary);

void BM_QueryStats_Active_BySymbol_Hot(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true, .max_symbols = 1000});
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_Active_BySymbol_Hot);

void BM_QueryStats_Active_BySymbol_Spill(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true, .max_symbols = 100});
    std::vector<std::string> syms;
    syms.reserve(5000);
    for (size_t i = 0; i < 5000; ++i) {
        syms.push_back("SYM_" + std::to_string(i));
    }
    size_t idx = 0;
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                syms[idx]
        );
        if (++idx == syms.size()) {
            idx = 0;
        }
    }
}
BENCHMARK(BM_QueryStats_Active_BySymbol_Spill);

void BM_QueryStats_Active_ContextTagScope(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
    for (auto _ : state) {
        ContextTagScope tag("market_data", "AAPL");
        benchmark::DoNotOptimize(tag);
    }
}
BENCHMARK(BM_QueryStats_Active_ContextTagScope);

void BM_QueryStats_Active_Phase3_Subsystems(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    for (auto _ : state) {
        record_pool_task(PoolKind::Cpu, 15'000ULL, 80'000ULL);
        record_codec_decode(45'000ULL, 2048, 8192);
        record_version_map_cache(/*hit=*/true, /*is_reload=*/false);
    }
}
BENCHMARK(BM_QueryStats_Active_Phase3_Subsystems);

// ---------------------------------------------------------------------------
// Multi-threaded scaling (shared collector via folly::ThreadLocal)
// ---------------------------------------------------------------------------

std::shared_ptr<QueryStatsCollector> g_mt_collector;

void setup_mt_collector(const benchmark::State&) {
    g_mt_collector = std::make_shared<QueryStatsCollector>(CollectorOptions{.by_library = true, .by_symbol = false});
}

void teardown_mt_collector(const benchmark::State&) {
    if (g_mt_collector) {
        auto snap = g_mt_collector->snapshot();
        benchmark::DoNotOptimize(snap.wall_time_ms);
        g_mt_collector.reset();
    }
}

void BM_QueryStats_Active_MultiThreaded(benchmark::State& state) {
    CollectorSession session(g_mt_collector);
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_Active_MultiThreaded)
        ->Setup(setup_mt_collector)
        ->Teardown(teardown_mt_collector)
        ->Threads(1)
        ->Threads(2)
        ->Threads(4)
        ->Threads(8)
        ->UseRealTime();

// ---------------------------------------------------------------------------
// Snapshot cost vs tracked symbol cardinality
// ---------------------------------------------------------------------------

void BM_QueryStats_Snapshot(benchmark::State& state) {
    const auto num_symbols = static_cast<size_t>(state.range(0));
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true, .max_symbols = 1000});
    for (size_t i = 0; i < num_symbols; ++i) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                100'000ULL,
                4096,
                1,
                "market_data",
                "SYM_" + std::to_string(i)
        );
    }
    for (auto _ : state) {
        auto snap = session.snapshot();
        benchmark::DoNotOptimize(snap.dropped_symbols);
    }
}
BENCHMARK(BM_QueryStats_Snapshot)->Arg(10)->Arg(100)->Arg(1000);

// ---------------------------------------------------------------------------
// Real MemoryStorage::read NVI end-to-end overhead
// ---------------------------------------------------------------------------

void BM_QueryStats_MemoryStorageRead_Disabled(benchmark::State& state) {
    storage::memory::MemoryStorage store(
            storage::LibraryPath::from_delim_path("bench_lib"),
            storage::OpenMode::DELETE,
            storage::memory::MemoryStorage::Config{}
    );
    auto ks = make_bench_key_segment("AAPL");
    const auto key = ks.variant_key();
    store.write(ks);

    for (auto _ : state) {
        auto res = store.read(entity::VariantKey{key}, storage::ReadKeyOpts{});
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_QueryStats_MemoryStorageRead_Disabled);

void BM_QueryStats_MemoryStorageRead_Active_ByLibrary(benchmark::State& state) {
    storage::memory::MemoryStorage store(
            storage::LibraryPath::from_delim_path("bench_lib"),
            storage::OpenMode::DELETE,
            storage::memory::MemoryStorage::Config{}
    );
    auto ks = make_bench_key_segment("AAPL");
    const auto key = ks.variant_key();
    store.write(ks);

    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    for (auto _ : state) {
        auto res = store.read(entity::VariantKey{key}, storage::ReadKeyOpts{});
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_QueryStats_MemoryStorageRead_Active_ByLibrary);

void BM_QueryStats_MemoryStorageRead_Active_BySymbol(benchmark::State& state) {
    storage::memory::MemoryStorage store(
            storage::LibraryPath::from_delim_path("bench_lib"),
            storage::OpenMode::DELETE,
            storage::memory::MemoryStorage::Config{}
    );
    auto ks = make_bench_key_segment("AAPL");
    const auto key = ks.variant_key();
    store.write(ks);

    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = true});
    for (auto _ : state) {
        auto res = store.read(entity::VariantKey{key}, storage::ReadKeyOpts{});
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_QueryStats_MemoryStorageRead_Active_BySymbol);

// ---------------------------------------------------------------------------
// Prometheus sink multi-threaded scaling & subsystem hot-path overhead
// ---------------------------------------------------------------------------

std::shared_ptr<PrometheusInstance> g_prom_bench_instance;

void setup_prom_sink(const benchmark::State&) {
    g_prom_bench_instance = std::make_shared<PrometheusInstance>();
    g_prom_bench_instance->configure(
            MetricsConfig{"localhost", "9091", "bench_job", "inst", "bench", MetricsConfig::Model::PUSH}
    );
    set_stats_sink(make_prometheus_stats_sink(g_prom_bench_instance, /*include_library=*/true));
}

void teardown_prom_sink(const benchmark::State&) {
    if (g_prom_bench_instance) {
        auto metrics = g_prom_bench_instance->get_metrics();
        benchmark::DoNotOptimize(metrics.size());
    }
    set_stats_sink(nullptr);
    g_prom_bench_instance.reset();
}

void BM_QueryStats_PrometheusSink_StorageOp_MT(benchmark::State& state) {
    for (auto _ : state) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "market_data",
                "AAPL"
        );
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_StorageOp_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(4)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_PrometheusSink_Subsystems_MT(benchmark::State& state) {
    for (auto _ : state) {
        record_pool_task(PoolKind::Cpu, 15'000ULL, 80'000ULL);
        record_codec_decode(45'000ULL, 2048, 8192);
        record_version_map_cache(/*hit=*/true, /*is_reload=*/false);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_Subsystems_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(4)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_MemoryStorageRead_PrometheusSink(benchmark::State& state) {
    storage::memory::MemoryStorage store(
            storage::LibraryPath::from_delim_path("bench_lib"),
            storage::OpenMode::DELETE,
            storage::memory::MemoryStorage::Config{}
    );
    auto ks = make_bench_key_segment("AAPL");
    const auto key = ks.variant_key();
    store.write(ks);

    for (auto _ : state) {
        auto res = store.read(entity::VariantKey{key}, storage::ReadKeyOpts{});
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_QueryStats_MemoryStorageRead_PrometheusSink)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_PrometheusSink_Scrape(benchmark::State& state) {
    auto prom = std::make_shared<PrometheusInstance>();
    prom->configure(MetricsConfig{"localhost", "9091", "bench_scrape", "inst", "bench", MetricsConfig::Model::PUSH});
    set_stats_sink(make_prometheus_stats_sink(prom, /*include_library=*/true));
    for (int i = 0; i < 100; ++i) {
        record_storage_op(
                StorageBackend::S3,
                StorageOp::Read,
                KeyType::TABLE_DATA,
                StorageOutcome::Ok,
                125'000ULL,
                4096,
                1,
                "lib_" + std::to_string(i % 8)
        );
        record_pool_task(PoolKind::Cpu, 15'000ULL, 80'000ULL);
        record_codec_decode(45'000ULL, 2048, 8192);
        record_version_map_cache(true, false);
        record_query_pruning(4, 2, 1, 1, 5, 2, 100, 25);
        record_lock_event(LockKind::StorageLock, true, 0, 0, false, 50'000ULL);
    }
    for (auto _ : state) {
        auto families = prom->get_metrics();
        benchmark::DoNotOptimize(families.size());
    }
    set_stats_sink(nullptr);
}
BENCHMARK(BM_QueryStats_PrometheusSink_Scrape);

void BM_QueryStats_Disabled_RecordHttpAttempt(benchmark::State& state) {
    const HttpAttemptTimings timings{
            .dns_ns = 1'000'000ULL,
            .tcp_connect_ns = 2'000'000ULL,
            .tls_handshake_ns = 3'000'000ULL,
            .ttfb_ns = 10'000'000ULL,
            .transfer_ns = 500'000ULL,
            .has_timings = true
    };
    for (auto _ : state) {
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 10'500'000ULL, 0, 0, false, timings);
    }
}
BENCHMARK(BM_QueryStats_Disabled_RecordHttpAttempt);

void BM_QueryStats_Active_RecordHttpAttempt(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    const HttpAttemptTimings timings{
            .dns_ns = 0,
            .tcp_connect_ns = 0,
            .tls_handshake_ns = 0,
            .ttfb_ns = 10'000'000ULL,
            .transfer_ns = 500'000ULL,
            .has_timings = true
    };
    for (auto _ : state) {
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 10'500'000ULL, 0, 0, false, timings);
    }
}
BENCHMARK(BM_QueryStats_Active_RecordHttpAttempt);

void BM_QueryStats_PrometheusSink_HttpAttempt_MT(benchmark::State& state) {
    const HttpAttemptTimings timings{
            .dns_ns = 0,
            .tcp_connect_ns = 0,
            .tls_handshake_ns = 0,
            .ttfb_ns = 10'000'000ULL,
            .transfer_ns = 500'000ULL,
            .has_timings = true
    };
    for (auto _ : state) {
        record_http_attempt("S3", "GetObject", HttpStatusClass::Status2xx, 10'500'000ULL, 0, 0, false, timings);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_HttpAttempt_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_Disabled_ApiOpScope(benchmark::State& state) {
    for (auto _ : state) {
        ApiOpScope scope(ApiOp::Read, "market_data");
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_Disabled_ApiOpScope);

void BM_QueryStats_Active_ApiOpScope(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = true, .by_symbol = false});
    for (auto _ : state) {
        ApiOpScope scope(ApiOp::Read, "market_data");
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_Active_ApiOpScope);

void BM_QueryStats_PrometheusSink_ApiOp_MT(benchmark::State& state) {
    for (auto _ : state) {
        ApiOpScope scope(ApiOp::Read, "market_data");
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_ApiOp_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_Disabled_BoundaryOpScope(benchmark::State& state) {
    for (auto _ : state) {
        BoundaryOpScope scope(BoundaryPhase::CppInput);
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_Disabled_BoundaryOpScope);

void BM_QueryStats_Active_BoundaryOpScope(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    for (auto _ : state) {
        BoundaryOpScope scope(BoundaryPhase::CppInput);
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_Active_BoundaryOpScope);

void BM_QueryStats_PrometheusSink_Boundary_MT(benchmark::State& state) {
    for (auto _ : state) {
        BoundaryOpScope scope(BoundaryPhase::CppInput);
        benchmark::DoNotOptimize(scope);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_Boundary_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_Disabled_RecordClauseExec(benchmark::State& state) {
    for (auto _ : state) {
        record_clause_exec(ClauseKind::Filter, 50'000ULL, 1000, 250);
    }
}
BENCHMARK(BM_QueryStats_Disabled_RecordClauseExec);

void BM_QueryStats_Active_RecordClauseExec(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    for (auto _ : state) {
        record_clause_exec(ClauseKind::Filter, 50'000ULL, 1000, 250);
    }
}
BENCHMARK(BM_QueryStats_Active_RecordClauseExec);

void BM_QueryStats_PrometheusSink_Processing_MT(benchmark::State& state) {
    for (auto _ : state) {
        record_clause_exec(ClauseKind::Filter, 50'000ULL, 1000, 250);
        record_admission_unit(/*waited=*/true, 200'000ULL);
        record_admission_segment(/*waited=*/true, 100'000ULL);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_Processing_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_Disabled_RecordSegmentWritten(benchmark::State& state) {
    for (auto _ : state) {
        record_segment_written(250, false);
    }
}
BENCHMARK(BM_QueryStats_Disabled_RecordSegmentWritten);

void BM_QueryStats_Active_RecordSegmentWritten(benchmark::State& state) {
    CollectorSession session(CollectorOptions{.by_library = false, .by_symbol = false});
    for (auto _ : state) {
        record_segment_written(250, false);
    }
}
BENCHMARK(BM_QueryStats_Active_RecordSegmentWritten);

void BM_QueryStats_PrometheusSink_Stream_MT(benchmark::State& state) {
    for (auto _ : state) {
        record_segment_written(250, false);
        record_incomplete_chain_load(3, false);
        record_compaction(CompactionKind::Incomplete, 3, 1, 500'000ULL);
    }
}
BENCHMARK(BM_QueryStats_PrometheusSink_Stream_MT)
        ->Setup(setup_prom_sink)
        ->Teardown(teardown_prom_sink)
        ->Threads(1)
        ->Threads(8)
        ->UseRealTime();

void BM_QueryStats_Disabled_RecordLmdbMapFull(benchmark::State& state) {
    for (auto _ : state) {
        record_lmdb_map_full();
    }
}
BENCHMARK(BM_QueryStats_Disabled_RecordLmdbMapFull);

void BM_QueryStats_SampleLiveResources(benchmark::State& state) {
    for (auto _ : state) {
        auto res = sample_live_resources();
        benchmark::DoNotOptimize(res);
    }
}
BENCHMARK(BM_QueryStats_SampleLiveResources);

} // namespace
} // namespace arcticdb::query_stats
