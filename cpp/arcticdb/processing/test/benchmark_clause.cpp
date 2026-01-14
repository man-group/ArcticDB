/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/processing/grouper.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

SegmentInMemory get_segment_for_merge(const StreamId& id, size_t num_rows, size_t start, size_t step) {
    auto segment = SegmentInMemory{
            get_test_descriptor<stream::TimeseriesIndex>(id, std::array{scalar_field(DataType::UINT8, "column")}),
            num_rows
    };
    auto& index_col = segment.column(0);
    auto& value_col = segment.column(1);
    for (auto i = 0u; i < num_rows; ++i) {
        index_col.push_back(static_cast<int64_t>(start + i * step));
        value_col.push_back(static_cast<uint8_t>(i));
    }
    segment.set_row_data(num_rows - 1);
    return segment;
}

void time_merge_on_segments(const std::vector<SegmentInMemory>& segments, benchmark::State& state) {
    // Pauses the timing while setting up the merge clause to only time the merging itself
    state.PauseTiming();
    auto component_manager = std::make_shared<ComponentManager>();
    std::vector<EntityId> entity_ids;
    for (auto& segment : segments) {
        auto proc_unit = ProcessingUnit{segment.clone()};
        entity_ids.push_back(push_entities(*component_manager, std::move(proc_unit))[0]);
    }

    auto stream_id = StreamId("Merge");
    StreamDescriptor descriptor{};
    descriptor.add_field(FieldRef{make_scalar_type(DataType::NANOSECONDS_UTC64), "time"});
    MergeClause merge_clause{TimeseriesIndex{"time"}, DenseColumnPolicy{}, stream_id, descriptor, false};
    merge_clause.set_component_manager(component_manager);
    state.ResumeTiming();

    auto _ = merge_clause.process(std::move(entity_ids));
}

static void BM_merge_interleaved(benchmark::State& state) {
    const auto num_segs = state.range(0);
    const auto num_rows = state.range(1);
    std::vector<SegmentInMemory> segments;
    for (auto i = 0u; i < num_segs; ++i) {
        auto id = "merge_" + std::to_string(i);
        // step size of [num_segs] guarantees the segments will merge completely interleaved
        auto seg = get_segment_for_merge(id, num_rows, i, num_segs);
        segments.emplace_back(std::move(seg));
    }

    for (auto _ : state) {
        time_merge_on_segments(segments, state);
    }
}

static void BM_merge_ordered(benchmark::State& state) {
    const auto num_segs = state.range(0);
    const auto num_rows = state.range(1);
    std::vector<SegmentInMemory> segments;
    for (auto i = 0u; i < num_segs; ++i) {
        auto id = "merge_" + std::to_string(i);
        // start of [i*num_segs] guarantees the segments will merge completely in order
        auto seg = get_segment_for_merge(id, num_rows, i * num_segs, 1);
        segments.emplace_back(std::move(seg));
    }

    for (auto _ : state) {
        time_merge_on_segments(segments, state);
    }
}

template<typename integer>
requires std::integral<integer>
void BM_hash_grouping_int(benchmark::State& state) {
    auto num_rows = state.range(0);
    auto num_unique_values = state.range(1);
    auto num_buckets = state.range(2);

    std::random_device rd;
    std::mt19937 gen(rd());
    // uniform_int_distribution (validly) undefined for int8_t in MSVC, hence the casting backwards and forwards
    std::uniform_int_distribution<int64_t> dis(
            static_cast<int64_t>(std::numeric_limits<integer>::lowest()),
            static_cast<int64_t>(std::numeric_limits<integer>::max())
    );
    std::vector<integer> unique_values;
    unique_values.reserve(num_unique_values);
    for (auto idx = 0; idx < num_unique_values; ++idx) {
        unique_values.emplace_back(static_cast<integer>(dis(gen)));
    }

    std::uniform_int_distribution<size_t> unique_values_dis(0, num_unique_values - 1);

    std::vector<integer> data;
    data.reserve(num_rows);
    for (int idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(unique_values[unique_values_dis(gen)]);
    }

    constexpr auto data_type = data_type_from_raw_type<integer>();
    Column column(make_scalar_type(data_type), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(column.ptr(), data.data(), num_rows * sizeof(integer));
    column.set_row_data(num_rows - 1);
    ColumnWithStrings col_with_strings(std::move(column), {}, "random_ints");

    grouping::HashingGroupers::Grouper<ScalarTagType<DataTypeTag<data_type>>> grouper;
    grouping::ModuloBucketizer bucketizer(num_buckets);

    for (auto _ : state) {
        auto buckets ARCTICDB_UNUSED = get_buckets(col_with_strings, grouper, bucketizer);
    }
}

void BM_hash_grouping_string(benchmark::State& state) {
    auto num_rows = state.range(0);
    auto num_unique_values = state.range(1);
    auto num_buckets = state.range(2);
    auto string_length = state.range(3);

    std::random_device rd;
    std::mt19937 gen(rd());
    const std::string character_set{
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`¬!\"£$%^&*()_+-=[]{};:'@#~,<.>/? "
    };
    std::uniform_int_distribution<> dis(0, character_set.size() - 1);
    std::vector<std::string> unique_values;
    unique_values.reserve(num_unique_values);
    for (auto idx = 0; idx < num_unique_values; ++idx) {
        std::string builder;
        builder.reserve(string_length);
        for (auto idx_2 = 0; idx_2 < string_length; ++idx_2) {
            builder += character_set[dis(gen)];
        }
        unique_values.emplace_back(builder);
    }

    std::uniform_int_distribution<size_t> unique_values_dis(0, num_unique_values - 1);

    Column column(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::NOT_PERMITTED);
    auto string_pool = std::make_shared<StringPool>();
    for (auto idx = 0; idx < num_rows; ++idx) {
        std::string_view str{unique_values[unique_values_dis(gen)]};
        OffsetString ofstr = string_pool->get(str);
        column.set_scalar(idx, ofstr.offset());
    }

    ColumnWithStrings col_with_strings(std::move(column), string_pool, "random_strings");

    grouping::HashingGroupers::Grouper<ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>> grouper;
    grouping::ModuloBucketizer bucketizer(num_buckets);

    for (auto _ : state) {
        auto buckets ARCTICDB_UNUSED = get_buckets(col_with_strings, grouper, bucketizer);
    }
}

BENCHMARK(BM_merge_interleaved)->Args({10'000, 100});
BENCHMARK(BM_merge_ordered)->Args({10'000, 100});

BENCHMARK(BM_hash_grouping_int<int8_t>)->Args({100'000, 10, 2});
BENCHMARK(BM_hash_grouping_int<int16_t>)->Args({100'000, 10, 2})->Args({100'000, 10'000, 2});
BENCHMARK(BM_hash_grouping_int<int32_t>)->Args({100'000, 10, 2})->Args({100'000, 100'000, 2});
BENCHMARK(BM_hash_grouping_int<int64_t>)->Args({100'000, 10, 2})->Args({100'000, 100'000, 2});

BENCHMARK(BM_hash_grouping_string)
        ->Args({100'000, 10, 2, 10})
        ->Args({100'000, 100'000, 2, 10})
        ->Args({100'000, 10, 2, 100})
        ->Args({100'000, 100'000, 2, 100});
