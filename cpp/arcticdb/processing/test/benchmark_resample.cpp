/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/processing/component_manager.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/test/segment_generation_utils.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace arcticdb::stream;

// run like: --benchmark_time_unit=ms --benchmark_filter=BM_resample.* --benchmark_min_time=5x
//
// Isolates ResampleClause::process from the rest of the read pipeline. Segments are constructed in memory and
// fed directly into the clause, so timings reflect the cost of bucket-boundary advancement, output index
// construction, and per-column aggregation only.
//
// Args (per benchmark): {rows_per_segment, num_segments, num_buckets, num_value_cols}
//
// num_buckets together with the total row count selects the layout:
//   * num_buckets <= num_segments              -> each bucket spans several row-slices
//   * num_segments < num_buckets <= total_rows -> many rows per bucket, all inside a single row-slice
//   * num_buckets > total_rows                 -> bucket size smaller than the row spacing, most empty
//
// Row spacing is derived: if num_buckets > total_rows the rows are spread out so that 1ns buckets still
// cover the full data span; otherwise rows are 1ns apart.

namespace {

constexpr DataType kValueDataType = DataType::INT64;
const std::string kValuePrefix = "col_";

ResampleClause<ResampleBoundary::LEFT>::BucketGeneratorT make_bucket_generator(std::vector<timestamp> boundaries) {
    return [boundaries = std::move(boundaries)](
                   timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, ResampleOrigin
           ) { return boundaries; };
}

StreamDescriptor make_descriptor(size_t num_value_cols) {
    std::vector<FieldRef> fields;
    fields.reserve(num_value_cols);
    for (size_t c = 0; c < num_value_cols; ++c) {
        fields.emplace_back(TypeDescriptor(kValueDataType, Dimension::Dim0), kValuePrefix + std::to_string(c));
    }
    return TimeseriesIndex::default_index().create_stream_descriptor("Resample", std::move(fields));
}

std::shared_ptr<SegmentInMemory> make_segment(
        const StreamDescriptor& desc, size_t num_rows, timestamp start_ts, timestamp step, size_t num_value_cols
) {
    auto seg = std::make_shared<SegmentInMemory>(
            desc, num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED, std::nullopt
    );
    std::vector<timestamp> index_data(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        index_data[row] = start_ts + step * static_cast<timestamp>(row);
    }
    fill_dense_column_data(*seg, 0, index_data);
    std::vector<int64_t> col_data(num_rows);
    for (size_t c = 0; c < num_value_cols; ++c) {
        for (size_t row = 0; row < num_rows; ++row) {
            col_data[row] = static_cast<int64_t>((row + c) % 1024);
        }
        fill_dense_column_data(*seg, c + 1, col_data);
    }
    return seg;
}

ResampleClause<ResampleBoundary::LEFT> make_resample_clause(
        std::vector<timestamp> bucket_boundaries, timestamp date_range_start, timestamp date_range_end,
        const std::shared_ptr<ComponentManager>& component_manager, size_t num_value_cols
) {
    ResampleClause<ResampleBoundary::LEFT> clause{
            "dummy", ResampleBoundary::LEFT, make_bucket_generator(bucket_boundaries), 0, 0
    };
    clause.bucket_boundaries_ = std::move(bucket_boundaries);
    clause.date_range_ = TimestampRange{date_range_start, date_range_end};
    clause.set_component_manager(component_manager);
    clause.set_processing_config(ProcessingConfig{false, 0, IndexDescriptor::Type::TIMESTAMP});
    std::vector<NamedAggregator> aggs;
    aggs.reserve(num_value_cols);
    for (size_t c = 0; c < num_value_cols; ++c) {
        const auto col = kValuePrefix + std::to_string(c);
        aggs.emplace_back("sum", col, col);
    }
    clause.set_aggregations(aggs);
    return clause;
}

struct Layout {
    std::vector<std::shared_ptr<SegmentInMemory>> segments;
    std::vector<timestamp> bucket_boundaries;
    timestamp date_range_start;
    timestamp date_range_end;
};

Layout build_layout(size_t rows_per_segment, size_t num_segments, size_t num_buckets, size_t num_value_cols) {
    const size_t total_rows = rows_per_segment * num_segments;
    // Pick row_step so the data spans at least num_buckets ns. If num_buckets <= total_rows the rows are 1ns
    // apart and buckets are bigger than 1ns. If num_buckets > total_rows the rows are spread out so 1ns
    // buckets still cover everything, leaving most buckets empty.
    const auto target_span = std::max<size_t>(total_rows, num_buckets);
    const auto row_step = static_cast<timestamp>(target_span / total_rows);
    const auto total_span = static_cast<timestamp>(total_rows) * row_step;
    const auto bucket_size = std::max<timestamp>(1, total_span / static_cast<timestamp>(num_buckets));

    auto desc = make_descriptor(num_value_cols);
    std::vector<std::shared_ptr<SegmentInMemory>> segments;
    segments.reserve(num_segments);
    for (size_t s = 0; s < num_segments; ++s) {
        const auto start_ts = static_cast<timestamp>(s * rows_per_segment) * row_step;
        segments.push_back(make_segment(desc, rows_per_segment, start_ts, row_step, num_value_cols));
    }

    std::vector<timestamp> boundaries;
    boundaries.reserve(num_buckets + 1);
    for (timestamp b = 0; b <= total_span; b += bucket_size) {
        boundaries.push_back(b);
    }
    if (boundaries.back() <= total_span - row_step) {
        boundaries.push_back(boundaries.back() + bucket_size);
    }

    return Layout{std::move(segments), std::move(boundaries), 0, total_span - row_step};
}

std::vector<EntityId> register_segments(
        ComponentManager& component_manager, const std::vector<std::shared_ptr<SegmentInMemory>>& segments,
        size_t num_value_cols
) {
    auto ids = component_manager.get_new_entity_ids(segments.size());
    size_t row_offset = 0;
    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& seg = segments[i];
        const auto rows = seg->row_count();
        auto rr = std::make_shared<RowRange>(row_offset, row_offset + rows);
        auto cr = std::make_shared<ColRange>(1, num_value_cols + 1);
        component_manager.add_entity(ids[i], seg, rr, cr, EntityFetchCount{1});
        row_offset += rows;
    }
    return ids;
}

} // namespace

static void BM_resample(benchmark::State& state) {
    const auto rows_per_segment = static_cast<size_t>(state.range(0));
    const auto num_segments = static_cast<size_t>(state.range(1));
    const auto num_buckets = static_cast<size_t>(state.range(2));
    const auto num_value_cols = static_cast<size_t>(state.range(3));

    auto layout = build_layout(rows_per_segment, num_segments, num_buckets, num_value_cols);

    for (auto _ : state) {
        state.PauseTiming();
        auto component_manager = std::make_shared<ComponentManager>();
        auto clause = make_resample_clause(
                layout.bucket_boundaries,
                layout.date_range_start,
                layout.date_range_end,
                component_manager,
                num_value_cols
        );
        auto ids = register_segments(*component_manager, layout.segments, num_value_cols);
        state.ResumeTiming();
        auto out = clause.process(std::move(ids));
        benchmark::DoNotOptimize(out);
    }
}

// 1m rows total across all regimes; num_segments and num_buckets carry the variation.
// 1000 rows per bucket, all rows in a single row-slice.
BENCHMARK(BM_resample)->Args({100'000, 10, 1'000, 1})->Args({100'000, 10, 1'000, 100});
// 100 rows per bucket, all rows in a single row-slice.
BENCHMARK(BM_resample)->Args({100'000, 10, 10'000, 1})->Args({100'000, 10, 10'000, 100});
// 10 rows per bucket, all rows in a single row-slice.
BENCHMARK(BM_resample)->Args({100'000, 10, 100'000, 1})->Args({100'000, 10, 100'000, 100});
// Each bucket spans several row-slices.
BENCHMARK(BM_resample)->Args({2'000, 500, 100, 1})->Args({2'000, 500, 100, 100});
// Bucket size smaller than row spacing, most buckets empty.
BENCHMARK(BM_resample)->Args({100'000, 10, 10'000'000, 1})->Args({100'000, 10, 10'000'000, 100});
