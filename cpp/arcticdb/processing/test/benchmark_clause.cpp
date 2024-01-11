/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

SegmentInMemory get_segment_for_merge(const StreamId &id, size_t num_rows, size_t start, size_t step){
    auto segment = SegmentInMemory{
        get_test_descriptor<stream::TimeseriesIndex>(id, {
            scalar_field(DataType::UINT8, "column")
        }),
        num_rows
    };
    auto& index_col = segment.column(0);
    auto& value_col = segment.column(1);
    for (auto i=0u; i<num_rows; ++i){
        index_col.push_back(static_cast<int64_t>(start + i*step));
        value_col.push_back(static_cast<uint8_t>(i));
    }
    segment.set_row_data(num_rows-1);
    return segment;
}

void time_merge_on_segments(const std::vector<SegmentInMemory> &segments, benchmark::State& state){
    // Pauses the timing while setting up the merge clause to only time the merging itself
    state.PauseTiming();
    auto component_manager = std::make_shared<ComponentManager>();
    Composite<EntityIds> entity_ids;
    for (auto& segment : segments){
        auto proc_unit = ProcessingUnit{segment.clone()};
        entity_ids.push_back(push_entities(component_manager, std::move(proc_unit)));
    }

    auto stream_id = StreamId("Merge");
    StreamDescriptor descriptor{};
    descriptor.add_field(FieldRef{make_scalar_type(DataType::NANOSECONDS_UTC64),"time"});
    MergeClause merge_clause;
    merge_clause.set_component_manager(component_manager);
    state.ResumeTiming();

    auto _ = merge_clause.process(std::move(entity_ids));
}

static void BM_merge_interleaved(benchmark::State& state){
    const auto num_segs = state.range(0);
    const auto num_rows = state.range(1);
    std::vector<SegmentInMemory> segments;
    for (auto i = 0u; i<num_segs; ++i){
        auto id = "merge_" + std::to_string(i);
        // step size of [num_segs] guarantees the segments will merge completely interleaved
        auto seg = get_segment_for_merge(id, num_rows, i, num_segs);
        segments.emplace_back(std::move(seg));
    }

    for (auto _ : state){
        time_merge_on_segments(segments, state);
    }
}

static void BM_merge_ordered(benchmark::State& state){
    const auto num_segs = state.range(0);
    const auto num_rows = state.range(1);
    std::vector<SegmentInMemory> segments;
    for (auto i = 0u; i<num_segs; ++i){
        auto id = "merge_" + std::to_string(i);
        // start of [i*num_segs] guarantees the segments will merge completely in order
        auto seg = get_segment_for_merge(id, num_rows, i*num_segs, 1);
        segments.emplace_back(std::move(seg));
    }

    for (auto _ : state){
        time_merge_on_segments(segments, state);
    }
}

BENCHMARK(BM_merge_interleaved)->Args({10'000, 100});
BENCHMARK(BM_merge_ordered)->Args({10'000, 100});