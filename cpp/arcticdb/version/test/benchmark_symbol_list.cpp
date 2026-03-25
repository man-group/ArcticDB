/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>
#include <malloc.h>
#include <atomic>
#include <thread>

#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/util/configs_map.hpp>

using namespace arcticdb;

std::shared_ptr<Store> create_s3_mock_store(const std::string& lib_name = "benchmark_sl") {
    storage::s3::S3ApiInstance::instance();
    arcticdb::proto::storage::VariantStorage vs;
    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name("test-bucket");
    cfg.set_use_mock_storage_for_testing(true);
    util::pack_to_any(cfg, *vs.mutable_config());

    storage::LibraryPath path{lib_name.c_str(), "store"};
    auto storages = storage::create_storages(path, storage::OpenMode::DELETE, {vs});
    auto library = std::make_shared<storage::Library>(path, std::move(storages));
    return std::make_shared<async::AsyncStore<>>(async::AsyncStore(library, codec::default_lz4_codec(), EncodingVersion::V1));
}

// Tracks peak heap usage by polling mallinfo2 from a background thread.
struct PeakHeapTracker {
    std::atomic<bool> running_{false};
    std::atomic<size_t> peak_uordblks_{0};
    size_t baseline_{0};
    std::thread sampler_;

    void start() {
        malloc_trim(0);
        baseline_ = mallinfo2().uordblks;
        peak_uordblks_ = baseline_;
        running_ = true;
        sampler_ = std::thread([this] {
            while (running_.load(std::memory_order_relaxed)) {
                auto current = mallinfo2().uordblks;
                auto prev = peak_uordblks_.load(std::memory_order_relaxed);
                while (current > prev && !peak_uordblks_.compare_exchange_weak(prev, current, std::memory_order_relaxed)
                )
                    ;
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }

    size_t stop() {
        running_ = false;
        if (sampler_.joinable())
            sampler_.join();
        // One final sample
        auto current = mallinfo2().uordblks;
        auto prev = peak_uordblks_.load(std::memory_order_relaxed);
        if (current > prev)
            peak_uordblks_.store(current, std::memory_order_relaxed);
        return peak_uordblks_.load() - baseline_;
    }
};

// Benchmarks peak memory usage of symbol list loading via the compaction path.
//
// Run with: --benchmark_time_unit=ms --benchmark_filter=BM_symbol_list_load

static void BM_symbol_list_load(benchmark::State& state) {
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    auto num_symbols = state.range(0);

    // Write journal entries for num_symbols symbols
    for (int64_t i = 0; i < num_symbols; ++i) {
        SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, 0);
    }

    // Force compaction on first load to create the compacted segment
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 0);
    {
        SymbolList sl{version_map};
        sl.load<std::set<StreamId>>(version_map, store, false);
    }

    // Add a few journal entries to exercise the merge path
    for (int64_t i = 0; i < 100; ++i) {
        SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, 1);
    }

    // Disable compaction during the benchmark iterations
    ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");

    for (auto _ : state) {
        PeakHeapTracker tracker;
        tracker.start();

        SymbolList sl{version_map};
        auto result = sl.load<std::set<StreamId>>(version_map, store, true);

        auto peak_delta = tracker.stop();
        auto retained = mallinfo2().uordblks - tracker.baseline_;

        state.counters["PeakMB"] = benchmark::Counter(static_cast<double>(peak_delta) / (1024.0 * 1024.0));
        state.counters["RetainedMB"] = benchmark::Counter(static_cast<double>(retained) / (1024.0 * 1024.0));
        state.counters["NumSymbols"] = benchmark::Counter(static_cast<double>(result.size()));

        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_symbol_list_load)->Arg(100'000)->Arg(1'000'000)->Unit(benchmark::kMillisecond)->Iterations(3);

// Benchmarks peak memory with many uncompacted journal entries per symbol.
// This is the scenario where the AtomKey vector elimination matters most.
//
// Setup: N_SYMBOLS symbols, each with ENTRIES_PER_SYMBOL journal entries.

static void BM_symbol_list_load_many_entries(benchmark::State& state) {
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    auto num_symbols = state.range(0);
    auto entries_per_symbol = state.range(1);

    // Write journal entries: each symbol gets multiple versions
    for (int64_t i = 0; i < num_symbols; ++i) {
        for (int64_t v = 0; v < entries_per_symbol; ++v) {
            SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, v);
        }
    }

    // Force compaction to create the compacted segment
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 0);
    {
        SymbolList sl{version_map};
        sl.load<std::set<StreamId>>(version_map, store, false);
    }

    // Write more journal entries (uncompacted)
    for (int64_t i = 0; i < num_symbols; ++i) {
        for (int64_t v = 0; v < entries_per_symbol; ++v) {
            SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, entries_per_symbol + v);
        }
    }

    ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");

    for (auto _ : state) {
        PeakHeapTracker tracker;
        tracker.start();

        SymbolList sl{version_map};
        auto result = sl.load<std::set<StreamId>>(version_map, store, true);

        auto peak_delta = tracker.stop();
        auto retained = mallinfo2().uordblks - tracker.baseline_;

        state.counters["PeakMB"] = benchmark::Counter(static_cast<double>(peak_delta) / (1024.0 * 1024.0));
        state.counters["RetainedMB"] = benchmark::Counter(static_cast<double>(retained) / (1024.0 * 1024.0));
        state.counters["NumSymbols"] = benchmark::Counter(static_cast<double>(result.size()));
        state.counters["TotalEntries"] = benchmark::Counter(static_cast<double>(num_symbols * entries_per_symbol));

        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_symbol_list_load_many_entries)->Args({1'000, 1'000})->Unit(benchmark::kMillisecond)->Iterations(1);

// Benchmarks peak memory during a load that triggers compaction.
// This measures the compaction path where keys are collected for deletion.
//
// Setup: N_SYMBOLS symbols with ENTRIES_PER_SYMBOL uncompacted journal entries each.
// MaxDelta=0 forces compaction on every load.

static void BM_symbol_list_compaction(benchmark::State& state) {
    auto version_map = std::make_shared<VersionMap>();
    auto num_symbols = state.range(0);
    auto entries_per_symbol = state.range(1);

    // Setup outside the benchmark loop — write journal entries once
    auto store = std::make_shared<InMemoryStore>();
    for (int64_t i = 0; i < num_symbols; ++i) {
        for (int64_t v = 0; v < entries_per_symbol; ++v) {
            SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, v);
        }
    }

    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 0);

    for (auto _ : state) {
        PeakHeapTracker tracker;
        tracker.start();

        SymbolList sl{version_map};
        auto result = sl.load<std::set<StreamId>>(version_map, store, false);

        auto peak_delta = tracker.stop();

        state.counters["PeakMB"] = benchmark::Counter(static_cast<double>(peak_delta) / (1024.0 * 1024.0));
        state.counters["NumSymbols"] = benchmark::Counter(static_cast<double>(result.size()));
        state.counters["TotalEntries"] =
                benchmark::Counter(static_cast<double>(num_symbols * entries_per_symbol));

        benchmark::DoNotOptimize(result);
    }

    ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");
}

BENCHMARK(BM_symbol_list_compaction)
        ->Args({1'000, 100})
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1);

// ---- S3 mock variants: exercise the full S3 storage layer (serialization, key parsing) ----

static void BM_symbol_list_load_s3(benchmark::State& state) {
    auto store = create_s3_mock_store();
    auto version_map = std::make_shared<VersionMap>();
    auto num_symbols = state.range(0);

    for (int64_t i = 0; i < num_symbols; ++i) {
        SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, 0);
    }

    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 0);
    {
        SymbolList sl{version_map};
        sl.load<std::set<StreamId>>(version_map, store, false);
    }

    for (int64_t i = 0; i < 100; ++i) {
        SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, 1);
    }

    ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");

    for (auto _ : state) {
        PeakHeapTracker tracker;
        tracker.start();

        SymbolList sl{version_map};
        auto result = sl.load<std::set<StreamId>>(version_map, store, true);

        auto peak_delta = tracker.stop();

        state.counters["PeakMB"] = benchmark::Counter(static_cast<double>(peak_delta) / (1024.0 * 1024.0));
        state.counters["NumSymbols"] = benchmark::Counter(static_cast<double>(result.size()));

        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_symbol_list_load_s3)->Arg(10'000)->Unit(benchmark::kMillisecond)->Iterations(1);

static void BM_symbol_list_compaction_s3(benchmark::State& state) {
    auto version_map = std::make_shared<VersionMap>();
    auto num_symbols = state.range(0);
    auto entries_per_symbol = state.range(1);

    auto store = create_s3_mock_store();
    for (int64_t i = 0; i < num_symbols; ++i) {
        for (int64_t v = 0; v < entries_per_symbol; ++v) {
            SymbolList::add_symbol(store, StreamId{fmt::format("symbol_{:06d}", i)}, v);
        }
    }

    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 0);

    for (auto _ : state) {
        PeakHeapTracker tracker;
        tracker.start();

        SymbolList sl{version_map};
        auto result = sl.load<std::set<StreamId>>(version_map, store, false);

        auto peak_delta = tracker.stop();

        state.counters["PeakMB"] = benchmark::Counter(static_cast<double>(peak_delta) / (1024.0 * 1024.0));
        state.counters["NumSymbols"] = benchmark::Counter(static_cast<double>(result.size()));
        state.counters["TotalEntries"] =
                benchmark::Counter(static_cast<double>(num_symbols * entries_per_symbol));

        benchmark::DoNotOptimize(result);
    }

    ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");
}

BENCHMARK(BM_symbol_list_compaction_s3)
        ->Args({1'000, 100})
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1);
