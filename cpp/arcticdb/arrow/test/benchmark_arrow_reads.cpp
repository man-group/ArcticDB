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
#include <arcticdb/arrow/arrow_handlers.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_arrow_string_handler(benchmark::State& state) {
    const auto num_rows = state.range(0);
    const auto num_different_strings = state.range(1);
    const auto num_sparse = state.range(2);
    const auto string_output_format = ArrowOutputStringFormat(state.range(3));
    auto read_options = ReadOptions{};
    read_options.set_arrow_output_default_string_format(string_output_format);

    auto handler = ArrowStringHandler();

    const double sparsity_ratio = num_sparse / num_rows;
    auto source_type_desc = TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::Dim0};
    auto [dest_type_desc, extra_bytes_per_block] =
            handler.output_type_and_extra_bytes({DataType::UTF_DYNAMIC64, Dimension::Dim0}, "col", read_options);
    auto dest_size = data_type_size(dest_type_desc);
    auto sparsity = num_sparse == 0 ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;

    // Fill up source column
    auto source_column = Column(source_type_desc, num_rows, AllocationType::DYNAMIC, sparsity);
    auto string_pool = std::make_shared<StringPool>();
    for (auto i = 0u, num_set = 0u; i < num_rows; i++) {
        auto offset = string_pool->get(fmt::format("str_{}", i % num_different_strings)).offset();
        auto expected_set = std::round((i + 1) * (1 - sparsity_ratio));
        if (num_set < expected_set) {
            source_column.set_scalar(i, offset);
            ++num_set;
        }
    }
    auto field_wrapper = FieldWrapper(dest_type_desc, "col");
    auto mapping = ColumnMapping(
            source_type_desc,
            dest_type_desc,
            field_wrapper.field(),
            dest_size,
            num_rows,
            0,
            0,
            dest_size * num_different_strings,
            0
    );
    auto handler_data = std::any{};

    for (auto _ : state) {
        state.PauseTiming();
        auto dest_column = Column(dest_type_desc, 0, AllocationType::DETACHABLE, sparsity, extra_bytes_per_block);
        allocate_chunked_column(dest_column, num_rows, num_rows);
        state.ResumeTiming();
        handler.convert_type(
                source_column, dest_column, mapping, DecodePathData{}, handler_data, string_pool, read_options
        );
    }
}

BENCHMARK(BM_arrow_string_handler)
        // ArrowOutputStringFormat::CATEGORICAL
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 0})
        ->Args({100'000, 1, 0, 0})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 0})
        ->Args({100'000, 100'000, 0, 0})
        // Half sparse
        ->Args({10'000, 1, 5'000, 0})
        ->Args({100'000, 1, 50'000, 0})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 0})
        ->Args({100'000, 1, 100'000, 0})
        // ArrowOutputStringFormat::LARGE_STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 1})
        ->Args({100'000, 1, 0, 1})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 1})
        ->Args({100'000, 100'000, 0, 1})
        // Half sparse
        ->Args({10'000, 1, 5'000, 1})
        ->Args({100'000, 1, 50'000, 1})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 1})
        ->Args({100'000, 1, 100'000, 1})
        // ArrowOutputStringFormat::SMALL_STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 2})
        ->Args({100'000, 1, 0, 2})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 2})
        ->Args({100'000, 100'000, 0, 2})
        // Half sparse
        ->Args({10'000, 1, 5'000, 2})
        ->Args({100'000, 1, 50'000, 2})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 2})
        ->Args({100'000, 1, 100'000, 2});
