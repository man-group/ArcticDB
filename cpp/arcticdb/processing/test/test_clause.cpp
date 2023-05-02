/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>

template<typename T>
void segment_scalar_assert_all_values_equal(const arcticdb::ProcessingSegment& segment, const arcticdb::ColumnName& name, const std::unordered_set<T>& expected, size_t expected_row_count) {
    const arcticdb::pipelines::SliceAndKey& slice_and_key = segment.data().front();
    std::shared_ptr<arcticdb::Store> empty;
    slice_and_key.ensure_segment(empty);
    const arcticdb::SegmentInMemory& segment_memory = slice_and_key.segment(empty);
    segment_memory.init_column_map();
    auto column_index = segment_memory.column_index(name.value).value();
    size_t row_counter = 0;
    for (auto row : segment_memory) {
        if (auto maybe_val = row.scalar_at<T>(column_index); maybe_val){
            ASSERT_THAT(expected, testing::Contains(maybe_val.value()));
            row_counter++;
        }
    }

    ASSERT_EQ(expected_row_count, row_counter);
}

void segment_string_assert_all_values_equal(const arcticdb::ProcessingSegment& segment, const arcticdb::ColumnName& name, std::string_view expected, size_t expected_row_count) {
    const arcticdb::pipelines::SliceAndKey& slice_and_key = segment.data().front();
    std::shared_ptr<arcticdb::Store> empty;
    slice_and_key.ensure_segment(empty);
    const arcticdb::SegmentInMemory& segment_memory = slice_and_key.segment(empty);
    segment_memory.init_column_map();
    auto column_index = segment_memory.column_index(name.value).value();
    size_t row_counter = 0;
    for (auto row : segment_memory) {
        if (auto maybe_val = row.string_at(column_index); maybe_val){
            ASSERT_EQ(maybe_val.value(), expected);
            row_counter++;
        }
    }

    ASSERT_EQ(expected_row_count, row_counter);

}

TEST(Clause, Partition) {
    using namespace arcticdb;
    auto seg = get_groupable_timeseries_segment("groupable", 30, {1,2,3,1,2,3,1,2,3});

    ScopedConfig num_buckets("Partition.NumBuckets", 16);
    std::shared_ptr<Store> empty;
    auto proc_seg = ProcessingSegment{std::move(seg), pipelines::FrameSlice{}};
    auto col = std::get<ColumnWithStrings>(proc_seg.get(ColumnName("int8"), empty));
    proc_seg.computed_data_.try_emplace("int8", col);
    Composite<ProcessingSegment> comp;
    comp.push_back(std::move(proc_seg));

    ExecutionContext context{};
    context.root_node_name_ = ExpressionName("int8");
    PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer> partition{std::make_shared<ExecutionContext>(std::move(context))};

    auto partitioned = partition.process(empty, std::move(comp));

    ASSERT_EQ(partitioned.level_1_size(), 1);

    std::vector<std::unordered_set<int8_t>> tags = {{1, 3}, {2}};
    std::vector<size_t> sizes = {180, 90};
    for (auto inner_seg : folly::enumerate(partitioned.as_range())){
        segment_scalar_assert_all_values_equal<int8_t>(*inner_seg, ColumnName("int8"), tags[inner_seg.index], sizes[inner_seg.index]);
    }
}

TEST(Clause, PartitionString) {
    using namespace arcticdb;
    auto seg = get_groupable_timeseries_segment("groupable", 30, {1,1,3,3,1,1});
    ScopedConfig num_buckets("Partition.NumBuckets", 16);
    std::shared_ptr<Store> empty;
    auto proc_seg = ProcessingSegment{std::move(seg), pipelines::FrameSlice{}};
    auto col = std::get<ColumnWithStrings>(proc_seg.get(ColumnName("strings"), empty));
    proc_seg.computed_data_.try_emplace("strings", col);
    Composite<ProcessingSegment> comp;
    comp.push_back(std::move(proc_seg));

    ExecutionContext context{};
    context.root_node_name_ = ExpressionName("strings");
    PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer> partition{std::make_shared<ExecutionContext>(std::move(context))};

    auto partitioned = partition.process(empty, std::move(comp));

    ASSERT_EQ(partitioned.level_1_size(), 1);

    std::vector<size_t> tags = {1, 3};
    std::vector<size_t> sizes = {120, 60};
    for (auto inner_seg : folly::enumerate(partitioned.as_range())){
        segment_string_assert_all_values_equal(*inner_seg, ColumnName("strings"), fmt::format("string_{}", tags[inner_seg.index]), sizes[inner_seg.index]);
    }
}

TEST(Clause, Passthrough) {
    using namespace arcticdb;
    auto seg = get_standard_timeseries_segment("passthrough");

    PassthroughClause passthrough;
    auto proc_seg = ProcessingSegment{std::move(seg), pipelines::FrameSlice{}};
    Composite<ProcessingSegment> comp;
    comp.push_back(std::move(proc_seg));
    std::shared_ptr<Store> empty;
    auto ret = passthrough.process(empty, std::move(comp));
}

TEST(Clause, Sort) {
    using namespace arcticdb;
    auto seg = get_standard_timeseries_segment("sort");
    std::random_device rng;
    std::mt19937 urng(rng());
    auto copied = seg.clone();
    std::shuffle(seg.begin(), seg.end(), urng);
    std::shared_ptr<Store> empty;
    SortClause sort_clause("time");
    auto proc_seg = ProcessingSegment{std::move(seg), pipelines::FrameSlice{}};
    Composite<ProcessingSegment> comp;
    comp.push_back(std::move(proc_seg));
    auto res = sort_clause.process(empty, std::move(comp));
    ASSERT_EQ(res.size(), 1);
    bool equal = std::get<ProcessingSegment>(res[0]).data_[0].segment(empty) == copied;
    ASSERT_EQ(equal, true);
}

TEST(Clause, Split) {
    using namespace arcticdb;
    std::string symbol{"split"};
    auto seg = get_standard_timeseries_segment(symbol, 100);
    auto copied = seg.clone();
    SplitClause split_clause(10);
    auto proc_seg = ProcessingSegment{std::move(seg)};
    Composite<ProcessingSegment> comp;
    comp.push_back(std::move(proc_seg));
    std::shared_ptr<Store> empty;
    auto res = split_clause.process(empty, std::move(comp));
    ASSERT_EQ(res.size(), 10);

    std::vector<FieldDescriptor> desc;
    const auto& fields = copied.descriptor().fields();
    auto beg = std::begin(fields);
    std::advance(beg, 1);
    for(auto field = beg; field != std::end(fields); ++field) {
        FieldDescriptor::Proto proto;
        proto.CopyFrom(*field);
        desc.emplace_back(std::move(proto));
    }

    SegmentSinkWrapper seg_wrapper(symbol, TimeseriesIndex::default_index(), std::move(desc));

    for(auto i = 0u; i < res.level_1_size(); ++i ) {
        auto item =  std::move(std::get<ProcessingSegment>(res[i]));
        pipelines::FrameSlice slice(item.data_[0].segment(empty));
        seg_wrapper.aggregator_.add_segment(std::move(item.data_[0].segment(empty)), slice, false);
    }

    seg_wrapper.aggregator_.commit();
    auto accum = seg_wrapper.segment();

    bool equal = accum == copied;
    ASSERT_EQ(equal, true);
}

TEST(Clause, Merge) {
    using namespace arcticdb;
    const auto seg_size = 5;
    ScopedConfig max_blocks("Merge.SegmentSize", seg_size);
    Composite<ProcessingSegment> comp;
    std::vector<SegmentInMemory> copies;
    const auto num_segs = 2;
    const auto num_rows = 5;
    for(auto x = 0u; x < num_segs; ++x) {
        auto symbol = fmt::format("merge_{}", x);
        auto seg = get_standard_timeseries_segment(symbol, 10);
        copies.emplace_back(seg.clone());
        auto proc_seg = ProcessingSegment{std::move(seg)};
        comp.push_back(std::move(proc_seg));
    }

    StreamDescriptor descriptor{};
    descriptor.add_field(scalar_field_proto(DataType::MICROS_UTC64, "time"));
    ExecutionContext merge_clause_context{};
    merge_clause_context.set_descriptor(descriptor);
    auto stream_id = StreamId("Merge");
    MergeClause merge_clause{TimeseriesIndex{"time"}, DenseColumnPolicy{}, stream_id, std::make_shared<ExecutionContext>(std::move(merge_clause_context))};
    std::shared_ptr<Store> empty;
    auto res = merge_clause.process(empty, std::move(comp));
    ASSERT_EQ(res.size(), 4u);
    for(auto i = 0; i < num_rows * num_segs; ++i) {
        auto& output_seg = std::get<ProcessingSegment>(res[i / seg_size]).data_[0].segment(empty);
        auto output_row = i % seg_size;
        const auto& expected_seg = copies[i % num_segs];
        auto expected_row = i / num_segs;
        for(auto field : folly::enumerate(output_seg.descriptor().fields())) {
            if(field.index == 1)
                continue;

            visit_field(*field, [&output_seg, &expected_seg, output_row, expected_row, &field] (auto tdt) {
                using DataTypeTag = typename decltype(tdt)::DataTypeTag;
                if constexpr(is_sequence_type(DataTypeTag::data_type)) {
                    const auto val1 = output_seg.string_at(output_row, position_t(field.index));
                    const auto val2 = expected_seg.string_at(expected_row, position_t(field.index));
                    ASSERT_EQ(val1, val2);
                } else {
                    using RawType = typename decltype(tdt)::DataTypeTag::raw_type;
                    const auto val1 = output_seg.scalar_at<RawType>(output_row, field.index);
                    const auto val2 = expected_seg.scalar_at<RawType>(expected_row, field.index);
                    ASSERT_EQ(val1, val2);
                }
            });
        }
    }
}
