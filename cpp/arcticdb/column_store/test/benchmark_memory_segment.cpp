/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <folly/container/Enumerate.h>

#include <algorithm>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

SegmentInMemory get_segment_for_bm(const StreamId &id, size_t num_rows, size_t num_columns){
    auto fields = std::vector<FieldRef>(num_columns);
    auto data_types = std::vector<DataType>{DataType::UINT8, DataType::UINT64, DataType::FLOAT64, DataType::ASCII_FIXED64};
    for (size_t i=0; i<num_columns; ++i){
        fields[i] = scalar_field(data_types[i%data_types.size()], "column_"+std::to_string(i));
    }
    auto test_frame = get_test_frame<stream::TimeseriesIndex>(id, fields, num_rows, 0, 0);
    return test_frame.segment_;
}

static void BM_sort(benchmark::State& state) {
    auto segment = get_segment_for_bm("test", state.range(0), state.range(1));
    std::random_device rng;
    std::mt19937 urng(rng());
    std::shuffle(segment.begin(), segment.end(), urng);
    for (auto _ : state) {
        state.PauseTiming();
        auto temp = segment.clone();
        state.ResumeTiming();
        temp.sort("time");
    }
}

BENCHMARK(BM_sort)->Args({100'000, 100})->Args({1'000'000, 1});