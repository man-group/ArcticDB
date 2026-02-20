/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

namespace {

// Create a numeric segment with N float64 data columns + 1 timestamp index column.
SegmentInMemory make_multi_col_segment(size_t num_rows, size_t num_data_cols, timestamp start_ts = 0) {
    std::vector<FieldRef> fields;
    fields.reserve(num_data_cols);
    for (size_t c = 0; c < num_data_cols; ++c) {
        fields.push_back(scalar_field(DataType::FLOAT64, fmt::format("col{}", c)));
    }
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("bench", std::span(fields.data(), fields.size()));
    SegmentInMemory seg(std::move(desc), num_rows);

    // Fill index column
    auto& idx_col = seg.column(0);
    for (size_t i = 0; i < num_rows; ++i) {
        idx_col.set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(start_ts + static_cast<timestamp>(i)));
    }

    // Fill data columns with sequential doubles
    for (size_t c = 0; c < num_data_cols; ++c) {
        auto& col = seg.column(c + 1); // +1 for index column
        for (size_t i = 0; i < num_rows; ++i) {
            col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i * num_data_cols + c) + 0.5);
        }
    }
    seg.set_row_data(num_rows - 1);
    return seg;
}

// Write a segment to the in-memory store and return a SliceAndKey.
pipelines::SliceAndKey write_to_store(
        const std::shared_ptr<InMemoryStore>& store, const StreamId& stream_id, SegmentInMemory&& segment,
        size_t row_start, size_t row_end, size_t col_start, size_t col_end
) {
    auto key = store->write(KeyType::TABLE_DATA,
                            0,
                            stream_id,
                            static_cast<timestamp>(row_start),
                            static_cast<timestamp>(row_end),
                            std::move(segment))
                       .get();
    pipelines::FrameSlice slice{pipelines::ColRange{col_start, col_end}, pipelines::RowRange{row_start, row_end}};
    return pipelines::SliceAndKey{std::move(slice), to_atom(key)};
}

} // namespace

// Benchmark: LazyRecordBatchIterator end-to-end — measures full lazy read pipeline
// Includes: read from InMemoryStore → decompress → prepare_segment_for_arrow → segment_to_arrow_data
// Args: num_segments, rows_per_segment, prefetch_size
static void BM_lazy_iterator_throughput(benchmark::State& state) {
    const auto num_segments = static_cast<size_t>(state.range(0));
    const auto rows_per_segment = static_cast<size_t>(state.range(1));
    const auto prefetch_size = static_cast<size_t>(state.range(2));
    constexpr size_t num_data_cols = 10;

    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"bench_symbol"};

    // num_data_cols + 1 (index) = total columns per segment
    const size_t total_cols = num_data_cols + 1;

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    slice_and_keys.reserve(num_segments);
    StreamDescriptor desc;

    for (size_t s = 0; s < num_segments; ++s) {
        auto start_ts = static_cast<timestamp>(s * rows_per_segment);
        auto segment = make_multi_col_segment(rows_per_segment, num_data_cols, start_ts);
        if (s == 0) {
            desc = segment.descriptor().clone();
        }
        auto row_start = s * rows_per_segment;
        auto row_end = row_start + rows_per_segment;
        slice_and_keys.push_back(write_to_store(store, stream_id, std::move(segment), row_start, row_end, 0, total_cols)
        );
    }

    for (auto _ : state) {
        LazyRecordBatchIterator iter(
                slice_and_keys, desc.clone(), store, nullptr, FilterRange{}, nullptr, "", prefetch_size
        );

        while (auto batch = iter.next()) {
            benchmark::DoNotOptimize(batch);
        }
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(num_segments * rows_per_segment * num_data_cols));
}

BENCHMARK(BM_lazy_iterator_throughput)
        ->Args({10, 10'000, 2})
        ->Args({10, 10'000, 5})
        ->Args({10, 10'000, 10})
        ->Args({50, 10'000, 5})
        ->Args({100, 10'000, 5})
        ->Args({10, 100'000, 5});

// Benchmark: LazyRecordBatchIterator with column-slice merging
// Writes 2 column slices per row group to exercise horizontal merge.
// Args: num_row_groups, rows_per_segment, prefetch_size
static void BM_lazy_iterator_with_merge(benchmark::State& state) {
    const auto num_row_groups = static_cast<size_t>(state.range(0));
    const auto rows_per_segment = static_cast<size_t>(state.range(1));
    const auto prefetch_size = static_cast<size_t>(state.range(2));
    constexpr size_t cols_per_slice = 5;

    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"bench_merge_symbol"};

    // Build slice_and_keys: 2 column slices per row group
    std::vector<pipelines::SliceAndKey> slice_and_keys;
    slice_and_keys.reserve(num_row_groups * 2);
    StreamDescriptor desc;

    for (size_t rg = 0; rg < num_row_groups; ++rg) {
        auto row_start = rg * rows_per_segment;
        auto row_end = row_start + rows_per_segment;
        auto start_ts = static_cast<timestamp>(row_start);

        // First column slice: index + col0..col4
        {
            std::vector<FieldRef> fields;
            for (size_t c = 0; c < cols_per_slice; ++c) {
                fields.push_back(scalar_field(DataType::FLOAT64, fmt::format("col{}", c)));
            }
            auto d = get_test_descriptor<stream::TimeseriesIndex>(
                    "bench_merge", std::span(fields.data(), fields.size())
            );
            SegmentInMemory seg(d.clone(), rows_per_segment);
            auto& idx_col = seg.column(0);
            for (size_t i = 0; i < rows_per_segment; ++i) {
                idx_col.set_scalar(
                        static_cast<ssize_t>(i), static_cast<timestamp>(start_ts + static_cast<timestamp>(i))
                );
            }
            for (size_t c = 0; c < cols_per_slice; ++c) {
                auto& col = seg.column(c + 1);
                for (size_t i = 0; i < rows_per_segment; ++i) {
                    col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
                }
            }
            seg.set_row_data(rows_per_segment - 1);
            // col_start=0, col_end=cols_per_slice+1 (index + data cols)
            slice_and_keys.push_back(
                    write_to_store(store, stream_id, std::move(seg), row_start, row_end, 0, cols_per_slice + 1)
            );
        }

        // Second column slice: col5..col9 (same row range, different col range)
        {
            std::vector<FieldRef> fields;
            for (size_t c = cols_per_slice; c < 2 * cols_per_slice; ++c) {
                fields.push_back(scalar_field(DataType::FLOAT64, fmt::format("col{}", c)));
            }
            auto d = get_test_descriptor<stream::TimeseriesIndex>(
                    "bench_merge", std::span(fields.data(), fields.size())
            );
            SegmentInMemory seg(d.clone(), rows_per_segment);
            auto& idx_col = seg.column(0);
            for (size_t i = 0; i < rows_per_segment; ++i) {
                idx_col.set_scalar(
                        static_cast<ssize_t>(i), static_cast<timestamp>(start_ts + static_cast<timestamp>(i))
                );
            }
            for (size_t c = 0; c < cols_per_slice; ++c) {
                auto& col = seg.column(c + 1);
                for (size_t i = 0; i < rows_per_segment; ++i) {
                    col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 1.5);
                }
            }
            seg.set_row_data(rows_per_segment - 1);
            slice_and_keys.push_back(write_to_store(
                    store, stream_id, std::move(seg), row_start, row_end, cols_per_slice + 1, 2 * cols_per_slice + 1
            ));
        }

        // Capture descriptor from first row group
        if (rg == 0) {
            // Build combined descriptor with all columns
            std::vector<FieldRef> all_fields;
            for (size_t c = 0; c < 2 * cols_per_slice; ++c) {
                all_fields.push_back(scalar_field(DataType::FLOAT64, fmt::format("col{}", c)));
            }
            desc = get_test_descriptor<stream::TimeseriesIndex>(
                    "bench_merge", std::span(all_fields.data(), all_fields.size())
            );
        }
    }

    for (auto _ : state) {
        LazyRecordBatchIterator iter(
                slice_and_keys, desc.clone(), store, nullptr, FilterRange{}, nullptr, "", prefetch_size
        );

        while (auto batch = iter.next()) {
            benchmark::DoNotOptimize(batch);
        }
    }

    state.SetItemsProcessed(
            state.iterations() * static_cast<int64_t>(num_row_groups * rows_per_segment * 2 * cols_per_slice)
    );
}

BENCHMARK(BM_lazy_iterator_with_merge)->Args({10, 10'000, 5})->Args({50, 10'000, 5})->Args({10, 100'000, 5});
