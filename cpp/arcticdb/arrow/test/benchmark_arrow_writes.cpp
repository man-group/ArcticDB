/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/arrow/test/arrow_test_utils.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_arrow_convert_single_record_batch_to_segment(benchmark::State& state) {
    const auto num_rows = state.range(0);
    const auto num_columns = state.range(1);
    const auto index_column = state.range(2);
    const bool numeric_data = state.range(3);
    std::vector<std::pair<std::string, sparrow::array>> columns;
    columns.reserve(num_columns);
    for (auto idx = 0; idx < num_columns; ++idx) {
        if (numeric_data) {
            columns.emplace_back(fmt::format("col_{}", idx), create_array(std::vector<int32_t>(num_rows, idx)));
        } else {
            columns.emplace_back(
                    fmt::format("col_{}", idx), create_array(std::vector<std::string>(num_rows, "hi there"))
            );
        }
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
    const bool numeric_data = state.range(3);
    const auto rows_per_record_batch = num_rows / num_record_batches;
    std::vector<sparrow::record_batch> record_batches;
    for (auto idx = 0; idx < num_record_batches; ++idx) {
        std::vector<std::pair<std::string, sparrow::array>> columns;
        columns.reserve(num_columns);
        for (auto col_idx = 0; col_idx < num_columns; ++col_idx) {
            if (numeric_data) {
                columns.emplace_back(
                        fmt::format("col_{}", idx), create_array(std::vector<int32_t>(rows_per_record_batch, col_idx))
                );
            } else {
                columns.emplace_back(
                        fmt::format("col_{}", idx),
                        create_array(std::vector<std::string>(rows_per_record_batch, "hi there"))
                );
            }
        }
        record_batches.emplace_back(create_record_batch(columns));
    }
    for (auto _ : state) {
        arrow_data_to_segment(record_batches);
    }
}

BENCHMARK(BM_arrow_convert_single_record_batch_to_segment)
        // Numeric data
        // Short and wide
        ->Args({10, 100'000, -1, true})
        ->Args({10, 100'000, 0, true})
        ->Args({10, 100'000, 50'000, true})
        // Long and thin
        ->Args({10'000'000, 10, -1, true})
        ->Args({10'000'000, 10, 0, true})
        ->Args({10'000'000, 10, 5, true})
        // String data
        // Short and wide
        ->Args({10, 100'000, -1, false})
        ->Args({10, 100'000, 0, false})
        ->Args({10, 100'000, 50'000, false})
// Long and thin - Windows CI can't handle this
#ifndef WIN32
        ->Args({10'000'000, 10, -1, false})
        ->Args({10'000'000, 10, 0, false})
        ->Args({10'000'000, 10, 5, false})
#endif
        ;

BENCHMARK(BM_arrow_convert_multiple_record_batches_to_segment)
        // Numeric data
        // Short and wide
        ->Args({10, 100'000, 10, true})
        // Long and thin
        ->Args({10'000'000, 10, 100, true})
        ->Args({10'000'000, 10, 10'000, true})
        // Highly fragmented
        ->Args({1'000, 10, 1'000, true})
        // String data
        // Short and wide
        ->Args({10, 100'000, 10, false})
// Long and thin - Windows CI can't handle this
#ifndef WIN32
        ->Args({10'000'000, 10, 100, false})
        ->Args({10'000'000, 10, 10'000, false})
#endif
        // Highly fragmented
        ->Args({1'000, 10, 1'000, false});
