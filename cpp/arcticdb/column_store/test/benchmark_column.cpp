/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <algorithm>
#include <numeric>
#include <random>

#include <benchmark/benchmark.h>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/util/test/test_utils.hpp>

using namespace arcticdb;

using BenchTDT = TypeDescriptorTag<DataTypeTag<DataType::NANOSECONDS_UTC64>, DimensionTag<Dimension::Dim0>>;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static std::random_device rd;
static std::mt19937 gen(rd());

// ─── Sorted-search benchmarks across block layouts ────────────────────────────────────────────────
//
// Four column shapes — single-block (PRESIZED), regular blocks (presized_in_blocks),
// irregular blocks of size 1000 (DETACHABLE), irregular blocks of size 1 (DETACHABLE).

namespace {

std::vector<timestamp> make_sorted_data(size_t num_rows, std::mt19937& rng) {
    std::uniform_int_distribution<timestamp> dis(0, std::numeric_limits<timestamp>::max() / 2);
    std::vector<timestamp> data;
    data.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        data.emplace_back(dis(rng));
    }
    std::ranges::sort(data);
    return data;
}

// NANOSECONDS_UTC64 keeps the column type consistent with BenchTDT.
constexpr DataType index_data_type = DataType::NANOSECONDS_UTC64;

auto make_single_block = [](const std::vector<timestamp>& data) {
    return make_single_block_column<timestamp>(data, index_data_type);
};
auto make_regular_blocks = [](const std::vector<timestamp>& data) {
    return make_regular_blocks_column<timestamp>(data, index_data_type);
};
auto make_irregular_blocks_1000 = [](const std::vector<timestamp>& data) {
    return make_irregular_blocks_column<timestamp>(data, uniform_block_sizes(data.size(), 1000), index_data_type);
};
auto make_irregular_blocks_1 = [](const std::vector<timestamp>& data) {
    return make_irregular_blocks_column<timestamp>(data, uniform_block_sizes(data.size(), 1), index_data_type);
};

} // namespace

// Full-column lower_bound — random target, cbegin to cend.
template<typename MakeColumn>
static void BM_lower_bound_shape(benchmark::State& state, MakeColumn make_column) {
    auto num_rows = state.range(0);
    std::mt19937 rng(0xC0FFEE);
    auto data = make_sorted_data(num_rows, rng);
    auto col = make_column(data);
    auto column_data = col.data();
    auto begin = column_data.template cbegin<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto end = column_data.template cend<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    std::uniform_int_distribution<size_t> idx_dis(0, num_rows - 1);
    for (auto _ : state) {
        state.PauseTiming();
        auto value = data[idx_dis(rng)];
        state.ResumeTiming();
        benchmark::DoNotOptimize(
                lower_bound<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, value)
        );
    }
}

// begin_dist controls where the exponential search starts:
//   begin_dist == -1 → begin = cbegin (full-column gallop).
//   begin_dist >=  0 → begin = citerator_at(target_idx - begin_dist) (answer is begin_dist past begin).
// The begin construction sits inside PauseTiming/ResumeTiming so only the search call is measured.
template<typename MakeColumn>
static void BM_exponential_lower_bound_shape(benchmark::State& state, MakeColumn make_column) {
    auto num_rows = state.range(0);
    auto begin_dist = state.range(1);
    std::mt19937 rng(0xC0FFEE);
    auto data = make_sorted_data(num_rows, rng);
    auto col = make_column(data);
    auto column_data = col.data();
    auto cbegin_it = column_data.template cbegin<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto end = column_data.template cend<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    const size_t lower = begin_dist >= 0 ? static_cast<size_t>(begin_dist) : 0;
    std::uniform_int_distribution<size_t> idx_dis(lower, num_rows - 1);
    for (auto _ : state) {
        state.PauseTiming();
        size_t target_idx = idx_dis(rng);
        auto value = data[target_idx];
        auto begin =
                begin_dist >= 0
                        ? column_data.template citerator_at<BenchTDT, IteratorType::ENUMERATED>(target_idx - begin_dist)
                        : cbegin_it;
        state.ResumeTiming();
        benchmark::DoNotOptimize(
                exponential_lower_bound<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, value)
        );
    }
}

// Full-column iteration — walks cbegin to cend
template<typename MakeColumn>
static void BM_iterate_shape(benchmark::State& state, MakeColumn make_column) {
    auto num_rows = state.range(0);
    std::mt19937 rng(0xC0FFEE);
    auto data = make_sorted_data(num_rows, rng);
    auto col = make_column(data);
    auto column_data = col.data();
    for (auto _ : state) {
        for (auto it = column_data.template cbegin<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(),
                  end = column_data.template cend<BenchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
             it != end;
             ++it) {
            auto v = it->value();
            benchmark::DoNotOptimize(v);
        }
    }
}

static void BM_lower_bound_single_block(benchmark::State& state) { BM_lower_bound_shape(state, make_single_block); }
static void BM_lower_bound_regular_blocks(benchmark::State& state) { BM_lower_bound_shape(state, make_regular_blocks); }
static void BM_lower_bound_irregular_blocks_1000(benchmark::State& state) {
    BM_lower_bound_shape(state, make_irregular_blocks_1000);
}
static void BM_lower_bound_irregular_blocks_1(benchmark::State& state) {
    BM_lower_bound_shape(state, make_irregular_blocks_1);
}

static void BM_exponential_lower_bound_single_block(benchmark::State& state) {
    BM_exponential_lower_bound_shape(state, make_single_block);
}
static void BM_exponential_lower_bound_regular_blocks(benchmark::State& state) {
    BM_exponential_lower_bound_shape(state, make_regular_blocks);
}
static void BM_exponential_lower_bound_irregular_blocks_1000(benchmark::State& state) {
    BM_exponential_lower_bound_shape(state, make_irregular_blocks_1000);
}
static void BM_exponential_lower_bound_irregular_blocks_1(benchmark::State& state) {
    BM_exponential_lower_bound_shape(state, make_irregular_blocks_1);
}

static void BM_iterate_single_block(benchmark::State& state) { BM_iterate_shape(state, make_single_block); }
static void BM_iterate_regular_blocks(benchmark::State& state) { BM_iterate_shape(state, make_regular_blocks); }
static void BM_iterate_irregular_blocks_1000(benchmark::State& state) {
    BM_iterate_shape(state, make_irregular_blocks_1000);
}
static void BM_iterate_irregular_blocks_1(benchmark::State& state) { BM_iterate_shape(state, make_irregular_blocks_1); }

BENCHMARK(BM_lower_bound_single_block)->Args({100'000});
BENCHMARK(BM_lower_bound_regular_blocks)->Args({100'000});
BENCHMARK(BM_lower_bound_irregular_blocks_1000)->Args({100'000});
BENCHMARK(BM_lower_bound_irregular_blocks_1)->Args({100'000});

// begin_dist = -1 → cbegin (full-column gallop, comparable to BM_lower_bound_shape).
// begin_dist = 100 → near-begin gallop. 100 is non-power-of-2 so 2**n probes overshoot before
// landing on the answer.
BENCHMARK(BM_exponential_lower_bound_single_block)->Args({100'000, -1})->Args({100'000, 100});
BENCHMARK(BM_exponential_lower_bound_regular_blocks)->Args({100'000, -1})->Args({100'000, 100});
BENCHMARK(BM_exponential_lower_bound_irregular_blocks_1000)->Args({100'000, -1})->Args({100'000, 100});
BENCHMARK(BM_exponential_lower_bound_irregular_blocks_1)->Args({100'000, -1})->Args({100'000, 100});

BENCHMARK(BM_iterate_single_block)->Args({100'000});
BENCHMARK(BM_iterate_regular_blocks)->Args({100'000});
BENCHMARK(BM_iterate_irregular_blocks_1000)->Args({100'000});
BENCHMARK(BM_iterate_irregular_blocks_1)->Args({100'000});
