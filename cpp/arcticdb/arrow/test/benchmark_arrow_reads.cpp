/* Copyright 2026 Man Group Operations Limited
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
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_arrow_string_handler(benchmark::State& state) {
    const auto num_rows = state.range(0);
    const auto num_different_strings = state.range(1);
    const auto num_sparse = state.range(2);
    const auto string_output_format = ArrowOutputStringFormat(state.range(3));
    const bool fixed_width_strings = state.range(4);
    auto read_options = ReadOptions{};
    read_options.set_output_format(OutputFormat::ARROW);
    read_options.set_arrow_output_default_string_format(string_output_format);

    auto handler = ArrowStringHandler();

    const double sparsity_ratio = num_sparse / num_rows;
    const auto data_type = fixed_width_strings ? DataType::UTF_FIXED64 : DataType::UTF_DYNAMIC64;
    auto source_type_desc = TypeDescriptor{data_type, Dimension::Dim0};
    auto [dest_type_desc, extra_bytes_per_block] =
            handler.output_type_and_extra_bytes(source_type_desc, "col", read_options);
    auto dest_size = data_type_size(dest_type_desc);
    auto sparsity = num_sparse == 0 ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;

    // Fill up source column
    auto source_column = Column(source_type_desc, num_rows, AllocationType::DYNAMIC, sparsity);
    auto string_pool = std::make_shared<StringPool>();
    size_t max_string_length = 4 * fmt::format("str_{}", num_different_strings - 1).size();
    for (auto i = 0u, num_set = 0u; i < num_rows; i++) {
        auto str = fmt::format("str_{}", i % num_different_strings);
        if (fixed_width_strings) {
            // Cheap and cheerful ASCII -> UTF-32 conversion
            std::string tmp;
            for (auto c : str) {
                tmp.append(1, c);
                tmp.append(3, '\0');
            }
            // Numpy fixed-width strings pad with nulls at the end for strings shorter than the longest string in the
            // column
            while (tmp.size() < max_string_length) {
                tmp.append(4, '\0');
            }
            str = tmp;
        }
        auto offset = string_pool->get(str).offset();
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
        // Dynamic strings
        // ArrowOutputStringFormat::CATEGORICAL
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 0, 0})
        ->Args({100'000, 1, 0, 0, 0})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 0, 0})
        ->Args({100'000, 100'000, 0, 0, 0})
        // Half sparse
        ->Args({10'000, 1, 5'000, 0, 0})
        ->Args({100'000, 1, 50'000, 0, 0})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 0, 0})
        ->Args({100'000, 1, 100'000, 0, 0})
        // ArrowOutputStringFormat::LARGE_STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 1, 0})
        ->Args({100'000, 1, 0, 1, 0})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 1, 0})
        ->Args({100'000, 100'000, 0, 1, 0})
        // Half sparse
        ->Args({10'000, 1, 5'000, 1, 0})
        ->Args({100'000, 1, 50'000, 1, 0})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 1, 0})
        ->Args({100'000, 1, 100'000, 1, 0})
        // ArrowOutputStringFormat::SMALL_STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 2, 0})
        ->Args({100'000, 1, 0, 2, 0})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 2, 0})
        ->Args({100'000, 100'000, 0, 2, 0})
        // Half sparse
        ->Args({10'000, 1, 5'000, 2, 0})
        ->Args({100'000, 1, 50'000, 2, 0})
        // Fully sparse
        ->Args({10'000, 1, 10'000, 2, 0})
        ->Args({100'000, 1, 100'000, 2, 0})

        // Fixed-width strings
        // ArrowOutputStringFormat::CATEGORICAL
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 0, 1})
        ->Args({100'000, 1, 0, 0, 1})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 0, 1})
        ->Args({100'000, 100'000, 0, 0, 1})
        // ArrowOutputStringFormat::LARGE_STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 1, 1})
        ->Args({100'000, 1, 0, 1, 1})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 1, 1})
        ->Args({100'000, 100'000, 0, 1, 1})
        // ArrowOutputStringFormat::STRING
        // Not sparse, small string buffers
        ->Args({10'000, 1, 0, 2, 1})
        ->Args({100'000, 1, 0, 2, 1})
        // Not sparse, large string buffers
        ->Args({10'000, 10'000, 0, 2, 1})
        ->Args({100'000, 100'000, 0, 2, 1});

namespace {

// Create a numeric segment with DETACHABLE allocation (matching the real read pipeline).
// num_blocks controls how many blocks per column (simulates multiple segments merged into one frame).
// Args: total_rows, num_data_cols, num_blocks
SegmentInMemory make_detachable_numeric_segment(size_t total_rows, size_t num_data_cols, size_t num_blocks) {
    std::vector<FieldRef> fields;
    fields.reserve(num_data_cols);
    for (size_t c = 0; c < num_data_cols; ++c) {
        fields.push_back(scalar_field(DataType::FLOAT64, fmt::format("col{}", c)));
    }
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("bench", std::span(fields.data(), fields.size()));

    // Allocate with DETACHABLE (like allocate_chunked_frame does for Arrow output)
    SegmentInMemory seg(std::move(desc), 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);

    const size_t rows_per_block = total_rows / num_blocks;
    const size_t total_cols = num_data_cols + 1; // +1 for index

    for (size_t col_idx = 0; col_idx < total_cols; ++col_idx) {
        auto& column = seg.column(static_cast<position_t>(col_idx));
        for (size_t b = 0; b < num_blocks; ++b) {
            size_t block_rows =
                    (b == num_blocks - 1) ? (total_rows - rows_per_block * (num_blocks - 1)) : rows_per_block;
            size_t bytes = block_rows * sizeof(double);
            column.allocate_data(bytes);
            // Fill with data
            auto data = column.data().buffer().last_block()->data();
            auto typed = reinterpret_cast<double*>(data);
            for (size_t i = 0; i < block_rows; ++i) {
                typed[i] = static_cast<double>(b * rows_per_block + i) + 0.5;
            }
            column.advance_data(bytes);
        }
        column.set_inflated(total_rows);
    }
    seg.set_row_data(total_rows - 1);
    return seg;
}

} // anonymous namespace

// Benchmark: segment_to_arrow_data — measures pure Arrow conversion cost (no I/O, no decode).
// This isolates the sparrow type construction and zero-copy buffer transfer overhead.
// Args: total_rows, num_data_cols, num_blocks
static void BM_segment_to_arrow_data(benchmark::State& state) {
    const auto total_rows = static_cast<size_t>(state.range(0));
    const auto num_data_cols = static_cast<size_t>(state.range(1));
    const auto num_blocks = static_cast<size_t>(state.range(2));

    for (auto _ : state) {
        state.PauseTiming();
        auto seg = make_detachable_numeric_segment(total_rows, num_data_cols, num_blocks);
        state.ResumeTiming();
        auto result = segment_to_arrow_data(seg);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(total_rows * num_data_cols));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(total_rows * num_data_cols * sizeof(double)));
}

BENCHMARK(BM_segment_to_arrow_data)
        // Small frame: 100K rows, 10 cols, single block
        ->Args({100'000, 10, 1})
        // 1M rows, 10 cols, single block (contiguous allocation)
        ->Args({1'000'000, 10, 1})
        // 1M rows, 10 cols, 10 blocks (simulates 10 merged segments — matches real eager path)
        ->Args({1'000'000, 10, 10})
        // Wide frame: 100K rows, 100 cols, 1 block
        ->Args({100'000, 100, 1})
        // Small segments: 10K rows, 10 cols, 1 block (typical lazy path per-segment)
        ->Args({10'000, 10, 1});