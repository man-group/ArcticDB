/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/stream/test/stream_test_common.hpp>
#include <folly/container/Enumerate.h>

#include <algorithm>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

std::vector<bool> get_sparse_bits(size_t num_rows, size_t num_set, std::mt19937 g) {
    auto sparse_bits = std::vector<bool>(num_rows, false);
    std::fill(sparse_bits.begin(), sparse_bits.begin() + num_set, true);
    std::shuffle(sparse_bits.begin(), sparse_bits.end(), g);
    return sparse_bits;
}

std::vector<uint64_t> get_random_permutation(size_t num_rows, std::mt19937 g) {
    auto result = std::vector<uint64_t>(num_rows);
    std::iota(result.begin(), result.end(), 1);
    std::shuffle(result.begin(), result.end(), g);
    return result;
}

SegmentInMemory get_shuffled_segment(
        const StreamId& id, size_t num_rows, size_t num_columns, std::optional<float> sparsity_percentage = std::nullopt
) {
    // We use a seed to get the same shuffled segment for given arguments.
    std::mt19937 g(0);
    std::vector<FieldWrapper> fields;
    for (auto i = 0u; i < num_columns; ++i) {
        fields.emplace_back(make_scalar_type(DataType::UINT64), "column_" + std::to_string(i));
    }

    std::vector<FieldRef> field_refs;
    field_refs.reserve(fields.size());
    for (const auto& wrapper : fields) {
        field_refs.emplace_back(FieldRef{wrapper.type(), wrapper.name()});
    }

    auto segment = SegmentInMemory{
            get_test_descriptor<stream::TimeseriesIndex>(id, field_refs),
            num_rows,
            AllocationType::DYNAMIC,
            sparsity_percentage.has_value() ? Sparsity::PERMITTED : Sparsity::NOT_PERMITTED
    };

    for (auto i = 0u; i <= num_columns; ++i) {
        auto& column = segment.column(i);
        auto values = get_random_permutation(num_rows, g);
        // We ensure the column we're sorting by is NOT sparse. As of 2023/12 sorting by sparse columns is not
        // supported.
        auto num_set = num_rows;
        if (i != 0 && sparsity_percentage.has_value()) {
            num_set = size_t(num_rows * (1 - *sparsity_percentage));
        }
        auto has_value = get_sparse_bits(num_rows, num_set, g);
        for (auto j = 0u; j < num_rows; ++j) {
            if (has_value[j]) {
                column.set_scalar(j, values[j]);
            }
        }
    }
    segment.set_row_data(num_rows - 1);

    return segment;
}

static void BM_sort_shuffled(benchmark::State& state) {
    auto segment = get_shuffled_segment("test", state.range(0), state.range(1));
    for (auto _ : state) {
        state.PauseTiming();
        auto temp = segment.clone();
        state.ResumeTiming();
        temp.sort("time");
    }
}

static void BM_sort_ordered(benchmark::State& state) {
    auto segment = get_shuffled_segment("test", state.range(0), state.range(1));
    segment.sort("time");
    for (auto _ : state) {
        state.PauseTiming();
        auto temp = segment.clone();
        state.ResumeTiming();
        temp.sort("time");
    }
}

static void BM_sort_sparse(benchmark::State& state) {
    auto segment = get_shuffled_segment("test", state.range(0), state.range(1), 0.5);
    for (auto _ : state) {
        state.PauseTiming();
        auto temp = segment.clone();
        state.ResumeTiming();
        temp.sort("time");
    }
}

// The {100k, 100} puts more weight on the sort_external part of the sort
// where the {1M, 1} puts more weight on the create_jive_table part.
BENCHMARK(BM_sort_shuffled)->Args({100'000, 100})->Args({1'000'000, 1});
BENCHMARK(BM_sort_ordered)->Args({100'000, 100});
BENCHMARK(BM_sort_sparse)->Args({100'000, 100});
