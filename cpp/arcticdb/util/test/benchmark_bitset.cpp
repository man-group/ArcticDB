/* Copyright 2025 Man Group Operations Limited
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
