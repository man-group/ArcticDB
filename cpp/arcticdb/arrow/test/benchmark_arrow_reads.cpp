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
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/constants.hpp>

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
    auto [dest_type_desc, block_config] = handler.output_type_and_block_config(source_type_desc, "col", read_options);
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
        auto dest_column = Column(dest_type_desc, 0, AllocationType::DETACHABLE, sparsity, block_config);
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

// ── Timestamp NaT handler benchmark ─────────────────────────────────────────

// Args: {num_rows, nat_percentage, num_sparse, truncate_25pct, num_source_chunks}
// num_source_chunks: 0 or 1 = single block, >1 = multi-block source
static void BM_arrow_timestamp_handler(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto nat_percentage = static_cast<size_t>(state.range(1));
    const auto num_sparse = static_cast<size_t>(state.range(2));
    const bool truncate = state.range(3) != 0;
    const auto num_source_chunks = static_cast<size_t>(state.range(4));

    auto read_options = ReadOptions{};
    read_options.set_output_format(OutputFormat::ARROW);

    auto handler = ArrowTimestampHandler{};
    auto source_type_desc = make_scalar_type(DataType::NANOSECONDS_UTC64);
    auto [dest_type_desc, block_config] = handler.output_type_and_block_config(source_type_desc, "ts", read_options);
    auto dest_size = data_type_size(dest_type_desc);
    auto sparsity = num_sparse == 0 ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED;

    std::vector<timestamp> values(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        values[i] = (i % 100 < nat_percentage) ? NaT : static_cast<timestamp>(i * 1000);
    }

    bool multi_block = num_source_chunks > 1;
    auto source_column = [&]() {
        if (multi_block) {
            auto col = Column(source_type_desc, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
            allocate_and_fill_chunked_column<timestamp>(
                    col, num_rows, num_rows / num_source_chunks, std::span<timestamp>(values)
            );
            return col;
        }
        auto col = Column(source_type_desc, num_rows, AllocationType::DYNAMIC, sparsity);
        const double sparsity_ratio = static_cast<double>(num_sparse) / static_cast<double>(num_rows);
        for (size_t i = 0, num_set = 0; i < num_rows; ++i) {
            auto expected_set = static_cast<size_t>(std::round((i + 1) * (1 - sparsity_ratio)));
            if (num_set < expected_set) {
                col.set_scalar(i, values[i]);
                ++num_set;
            }
        }
        return col;
    }();

    auto field_wrapper = FieldWrapper(dest_type_desc, "ts");
    auto mapping = ColumnMapping(
            source_type_desc, dest_type_desc, field_wrapper.field(), dest_size, num_rows, 0, 0, dest_size * num_rows, 0
    );
    if (truncate) {
        mapping.set_truncate(ColumnTruncation(num_rows / 4, num_rows * 3 / 4));
    }
    auto handler_data = std::any{};
    auto string_pool = std::make_shared<StringPool>();

    for (auto _ : state) {
        state.PauseTiming();
        auto dest_column = Column(dest_type_desc, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, block_config);
        allocate_chunked_column(dest_column, num_rows, num_rows);
        state.ResumeTiming();
        handler.convert_type(
                source_column, dest_column, mapping, DecodePathData{}, handler_data, string_pool, read_options
        );
        benchmark::DoNotOptimize(dest_column);
    }
}

BENCHMARK(BM_arrow_timestamp_handler)
        // Dense, no NaTs
        ->Args({100'000, 0, 0, 0, 0})
        ->Args({1'000'000, 0, 0, 0, 0})
        ->Args({10'000'000, 0, 0, 0, 0})
        // Dense, 25% NaTs
        ->Args({100'000, 25, 0, 0, 0})
        ->Args({1'000'000, 25, 0, 0, 0})
        ->Args({10'000'000, 25, 0, 0, 0})
        // Dense, 50% NaTs
        ->Args({100'000, 50, 0, 0, 0})
        ->Args({1'000'000, 50, 0, 0, 0})
        ->Args({10'000'000, 50, 0, 0, 0})
        // Dense, all NaTs
        ->Args({100'000, 100, 0, 0, 0})
        ->Args({1'000'000, 100, 0, 0, 0})
        ->Args({10'000'000, 100, 0, 0, 0})
        // Half sparse, no NaTs
        ->Args({100'000, 0, 50'000, 0, 0})
        ->Args({1'000'000, 0, 500'000, 0, 0})
        // Half sparse, 25% NaTs
        ->Args({100'000, 25, 50'000, 0, 0})
        ->Args({1'000'000, 25, 500'000, 0, 0})
        // Fully sparse
        ->Args({100'000, 0, 100'000, 0, 0})
        ->Args({1'000'000, 0, 1'000'000, 0, 0})
        // Truncated (25%-75% window), dense, 25% NaTs
        ->Args({1'000'000, 25, 0, 1, 0})
        ->Args({10'000'000, 25, 0, 1, 0})
        // Truncated, half sparse, 25% NaTs
        ->Args({1'000'000, 25, 500'000, 1, 0})
        // Multi-block source (copy_frame_data_to_buffer path)
        ->Args({1'000'000, 0, 0, 0, 10})
        ->Args({1'000'000, 25, 0, 0, 10});