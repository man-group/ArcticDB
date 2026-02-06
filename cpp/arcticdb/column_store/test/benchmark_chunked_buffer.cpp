/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <random>
#include <benchmark/benchmark.h>
#include <arcticdb/column_store/chunked_buffer.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_chunked_buffer_allocate_with_ensure(benchmark::State& state) {
    auto num_chunks = state.range(0);
    auto chunk_size = state.range(1);
    bool is_alligned = state.range(2);
    auto allocation_type = static_cast<entity::AllocationType>(state.range(3));
    for (auto _ : state) {
        ChunkedBuffer buffer(allocation_type);
        for (auto i = 0; i < num_chunks; ++i) {
            buffer.ensure((i + 1) * chunk_size, is_alligned);
        }
    }
}

static void BM_chunked_buffer_random_access(benchmark::State& state) {
    auto num_chunks = state.range(0);
    auto chunk_size = state.range(1);
    auto is_alligned = state.range(2);
    auto allocation_type = static_cast<entity::AllocationType>(state.range(3));
    ChunkedBuffer buffer(allocation_type);
    for (auto i = 0; i < num_chunks; ++i) {
        buffer.ensure((i + 1) * chunk_size, is_alligned);
    }
    static std::mt19937 gen(42);
    std::uniform_int_distribution<size_t> dis(0, num_chunks * chunk_size - 1);
    for (auto _ : state) {
        auto [block, pos, block_idx] = buffer.block_and_offset(dis(gen));
        *(block->ptr(pos)) = 1;
    }
}

BENCHMARK(BM_chunked_buffer_allocate_with_ensure)
        ->Args({100'000, 203, true, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({100'000, 203, false, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({100'000, 203, false, static_cast<int8_t>(entity::AllocationType::DETACHABLE)})
        ->Args({10'000, 2003, true, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({10'000, 2003, false, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({10'000, 2003, false, static_cast<int8_t>(entity::AllocationType::DETACHABLE)});

BENCHMARK(BM_chunked_buffer_random_access)
        ->Args({100'000, 203, true, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({100'000, 203, false, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({100'000, 203, false, static_cast<int8_t>(entity::AllocationType::DETACHABLE)})
        ->Args({10'000, 2003, true, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({10'000, 2003, false, static_cast<int8_t>(entity::AllocationType::DYNAMIC)})
        ->Args({10'000, 2003, false, static_cast<int8_t>(entity::AllocationType::DETACHABLE)});
