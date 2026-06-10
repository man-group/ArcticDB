/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <algorithm>
#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/util/bitset.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static std::random_device rd;
static std::mt19937 gen(rd());

static void BM_packed_bits_to_buffer(benchmark::State& state) {
    auto num_bits = state.range(0);
    auto num_bytes = bitset_packed_size_bytes(num_bits);
    std::vector<uint8_t> data;
    data.reserve(num_bytes);
    // std::uniform_int_distribution<uint8_t> is not part of the standard
    std::uniform_int_distribution<uint16_t> dis(
            std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max()
    );
    for (size_t idx = 0; idx < num_bytes; ++idx) {
        data.emplace_back(dis(gen));
    }
    const uint8_t* packed_bits = data.data();
    uint8_t* dest_ptr = new uint8_t[num_bits];
    for (auto _ : state) {
        packed_bits_to_buffer(packed_bits, num_bits, 0, dest_ptr);
    }
    delete[] dest_ptr;
}

BENCHMARK(BM_packed_bits_to_buffer)
        ->Args({100'000})
        ->Args({1'000'000})
        ->Args({10'000'000})
        ->Args({100'000'000})
        ->Args({1'000'000'000});

static void BM_bools_to_packed_bits(benchmark::State& state) {
    auto num_bools = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> flat_bools(num_bools);
    std::uniform_int_distribution<int> dis(0, 1);
    for (size_t i = 0; i < num_bools; ++i) {
        flat_bools[i] = dis(gen);
    }
    auto* src = reinterpret_cast<const bool*>(flat_bools.data());
    std::vector<uint8_t> dest(bitset_packed_size_bytes(num_bools));
    for (auto _ : state) {
        bools_to_packed_bits(src, num_bools, dest.data());
        benchmark::DoNotOptimize(dest.data());
    }
}

BENCHMARK(BM_bools_to_packed_bits)->Args({100'000})->Args({1'000'000})->Args({10'000'000});
