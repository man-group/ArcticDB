/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <arcticdb/arrow/test/arrow_test_utils.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_arrow_convert_single_record_batch_to_segment(benchmark::State& state) {
    const auto num_rows = state.range(0);
    const auto num_columns = state.range(1);
    const auto index_column = state.range(2);
    std::vector<std::pair<std::string, sparrow::array>> columns;
    columns.reserve(num_columns);
    for (auto idx = 0; idx < num_columns; ++idx) {
        columns.emplace_back(fmt::format("col_{}", idx), create_array(std::vector<int32_t>(num_rows, idx)));
    }
    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(create_record_batch(columns));
    if (index_column > 0) {
        std::string index_name = fmt::format("col_{}", index_column);
        for (auto _ : state) {
            arrow_data_to_segment(record_batches, index_name);
        }
    } else {
        for (auto _ : state) {
            arrow_data_to_segment(record_batches);
        }
    }
}

static void BM_arrow_convert_multiple_record_batches_to_segment(benchmark::State& state) {
    const auto num_rows = state.range(0);
    const auto num_columns = state.range(1);
    const auto num_record_batches = state.range(2);
    const auto rows_per_record_batch = num_rows / num_record_batches;
    std::vector<sparrow::record_batch> record_batches;
    for (auto idx = 0; idx < num_record_batches; ++idx) {
        std::vector<std::pair<std::string, sparrow::array>> columns;
        columns.reserve(num_columns);
        for (auto col_idx = 0; col_idx < num_columns; ++col_idx) {
            columns.emplace_back(
                    fmt::format("col_{}", idx), create_array(std::vector<int32_t>(rows_per_record_batch, col_idx))
            );
        }
        record_batches.emplace_back(create_record_batch(columns));
    }
    for (auto _ : state) {
        arrow_data_to_segment(record_batches);
    }
}

BENCHMARK(BM_arrow_convert_single_record_batch_to_segment)
        // Short and wide data
        ->Args({10, 100'000, -1})
        ->Args({10, 100'000, 0})
        ->Args({10, 100'000, 50'000})
        // Long and thin data
        ->Args({10'000'000, 10, -1})
        ->Args({10'000'000, 10, 0})
        ->Args({10'000'000, 10, 5});

BENCHMARK(BM_arrow_convert_multiple_record_batches_to_segment)
        // Short and wide data
        ->Args({10, 100'000, 10})
        // Long and thin data
        ->Args({10'000'000, 10, 100})
        ->Args({10'000'000, 10, 10'000})
        // Highly fragmented data
        ->Args({1'000, 10, 1'000});
