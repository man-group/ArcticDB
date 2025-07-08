/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <algorithm>
#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/column.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static std::random_device rd;
static std::mt19937 gen(rd());

static void BM_search_sorted_random(benchmark::State& state) {
    auto num_rows = state.range(0);
    std::vector<timestamp> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<timestamp> dis(std::numeric_limits<timestamp>::min(), std::numeric_limits<timestamp>::max());
    for (auto idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(dis(gen));
    }
    std::ranges::sort(data);
    Column col(make_scalar_type(DataType::NANOSECONDS_UTC64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(timestamp));
    col.set_row_data(num_rows - 1);
    for (auto _ : state) {
        state.PauseTiming();
        auto value = dis(gen);
        state.ResumeTiming();
        col.search_sorted(value);
    }
}

static void BM_search_sorted_single_value(benchmark::State& state) {
    auto num_rows = state.range(0);
    auto from_right = state.range(1);
    std::uniform_int_distribution<timestamp> dis(std::numeric_limits<timestamp>::min(), std::numeric_limits<timestamp>::max());
    auto value = dis(gen);
    std::vector<timestamp> data(num_rows, value);
    Column col(make_scalar_type(DataType::NANOSECONDS_UTC64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(timestamp));
    col.set_row_data(num_rows - 1);
    for (auto _ : state) {
        col.search_sorted(value, from_right);
    }
}

BENCHMARK(BM_search_sorted_random)->Args({100'000});
BENCHMARK(BM_search_sorted_single_value)->Args({100'000, true})->Args({100'000, false});
