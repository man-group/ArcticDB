/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>

template<typename T>
void segment_scalar_assert_all_values_equal(const arcticdb::ProcessingUnit& proc_unit, const arcticdb::ColumnName& name, const std::unordered_set<T>& expected, size_t expected_row_count) {
    auto segment = *proc_unit.segments_->front();
    segment.init_column_map();
    auto column_index = segment.column_index(name.value).value();
    size_t row_counter = 0;
    for (const auto& row: segment) {
        if (auto maybe_val = row.scalar_at<T>(column_index); maybe_val){
            ASSERT_THAT(expected, testing::Contains(maybe_val.value()));
            row_counter++;
        }
    }

    ASSERT_EQ(expected_row_count, row_counter);
}

void segment_string_assert_all_values_equal(const arcticdb::ProcessingUnit& proc_unit, const arcticdb::ColumnName& name, std::string_view expected, size_t expected_row_count) {
    auto segment = *proc_unit.segments_->front();
    segment.init_column_map();
    auto column_index = segment.column_index(name.value).value();
    size_t row_counter = 0;
    for (auto row: segment) {
        if (auto maybe_val = row.string_at(column_index); maybe_val){
            ASSERT_EQ(maybe_val.value(), expected);
            row_counter++;
        }
    }

    ASSERT_EQ(expected_row_count, row_counter);

}

TEST(Clause, PartitionEmptyColumn) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer> partition{"empty_sum"};
    partition.set_component_manager(component_manager);

    auto proc_unit = ProcessingUnit{generate_groupby_testing_empty_segment(100, 10)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));
    auto partitioned = gather_entities(component_manager, partition.process(std::move(entity_ids)));

    ASSERT_TRUE(partitioned.empty());
}

TEST(Clause, AggregationEmptyColumn) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation("int_repeated_values", {{"empty_sum", "sum"}, {"empty_min", "min"}, {"empty_max", "max"}, {"empty_mean", "mean"}, {"empty_count", "count"}, {"empty_first", "first"}, {"empty_last", "last"}});
    aggregation.set_component_manager(component_manager);

    size_t num_rows{100};
    size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_segment(num_rows, unique_grouping_values)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto aggregated = gather_entities(component_manager, aggregation.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(1, aggregated.size());
    ASSERT_TRUE(aggregated[0].segments_.has_value());
    auto segments = aggregated[0].segments_.value();
    ASSERT_EQ(1, segments.size());
    auto segment = segments[0];

    // Sum aggregations should produce a float64 column full of zeros
    auto sum_column_index = segment->column_index("empty_sum");
    ASSERT_TRUE(sum_column_index.has_value());
    auto& sum_column = segment->column(*sum_column_index);
    ASSERT_EQ(DataType::FLOAT64, sum_column.type().data_type());
    for (size_t idx = 0; idx < unique_grouping_values; idx++) {
        ASSERT_EQ(double(0), sum_column.scalar_at<double>(idx));
    }

    // Min, max, mean, count, first and last aggregations should not be present in the output segment
    ASSERT_FALSE(segment->column_index("empty_min").has_value());
    ASSERT_FALSE(segment->column_index("empty_max").has_value());
    ASSERT_FALSE(segment->column_index("empty_mean").has_value());
    ASSERT_FALSE(segment->column_index("empty_count").has_value());
    ASSERT_FALSE(segment->column_index("empty_first").has_value());
    ASSERT_FALSE(segment->column_index("empty_last").has_value());
}

namespace aggregation_test
{
    template <class T, class F>
    void check_column(arcticdb::SegmentInMemory segment, std::string_view column_name, std::size_t ugv, F&& f)
    {
        auto column_index = segment.column_index(column_name);
        ASSERT_TRUE(column_index.has_value());
        auto& column = segment.column(*column_index);
        auto dt = arcticdb::data_type_from_raw_type<T>();
        ASSERT_EQ(dt, column.type().data_type());
        for(std::size_t idx = 0u; idx < ugv; ++idx)
        {
            if constexpr (std::is_floating_point_v<T>)
            {
                T val = column.scalar_at<T>(idx).value();
                if (std::isnan(val))
                {
                    ASSERT_TRUE(std::isnan(f(idx)));
                }
                else
                {
                    ASSERT_EQ(f(idx), val);
                }
            }
            else
            {
                ASSERT_EQ(f(idx), column.scalar_at<T>(idx));
            }
        }
    }
}

TEST(Clause, AggregationColumn)
{
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation("int_repeated_values", {{"sum_int", "sum"}, {"min_int", "min"}, {"max_int", "max"}, {"mean_int", "mean"}, {"count_int", "count"}});
    aggregation.set_component_manager(component_manager);

    size_t num_rows{100};
    size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_segment(num_rows, unique_grouping_values)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto aggregated = gather_entities(component_manager, aggregation.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(1, aggregated.size());
    ASSERT_TRUE(aggregated[0].segments_.has_value());
    auto segments = aggregated[0].segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_grouping_values, [](size_t idx) { return 450 + 10*idx; });
    check_column<int64_t>(*segments[0], "min_int", unique_grouping_values, [](size_t idx) { return idx; });
    check_column<int64_t>(*segments[0], "max_int", unique_grouping_values, [](size_t idx) { return 90+idx; });
    check_column<double>(*segments[0], "mean_int", unique_grouping_values, [](size_t idx) { return double(45+idx); });
    check_column<uint64_t>(*segments[0], "count_int", unique_grouping_values, [](size_t) { return 10; });
}

TEST(Clause, AggregationSparseColumn)
{
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation("int_repeated_values", {{"sum_int", "sum"}, {"min_int", "min"}, {"max_int", "max"}, {"mean_int", "mean"}, {"count_int", "count"}});
    aggregation.set_component_manager(component_manager);

    size_t num_rows{100};
    size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_sparse_segment(num_rows, unique_grouping_values)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto aggregated = gather_entities(component_manager, aggregation.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(1, aggregated.size());
    ASSERT_TRUE(aggregated[0].segments_.has_value());
    auto segments = aggregated[0].segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_grouping_values, [](size_t idx) {
        if (idx%2 == 0) {
            return 450 + 10*static_cast<int64_t>(idx);
        } else {
            return int64_t(0);
        }
    });
    check_column<int64_t>(*segments[0], "min_int", unique_grouping_values, [](size_t idx) {
        if (idx%2 == 0) {
            return static_cast<int64_t>(idx);
        } else {
            return std::numeric_limits<int64_t>::max();
        }
    });
    check_column<int64_t>(*segments[0], "max_int", unique_grouping_values, [](size_t idx) {
        if (idx%2 == 0) {
            return 90 + static_cast<int64_t>(idx);
        } else {
            return std::numeric_limits<int64_t>::lowest();
        }
    });
    check_column<double>(*segments[0], "mean_int", unique_grouping_values, [](size_t idx) {
        if (idx%2 == 0) {
            return double(45 + idx);
        }
        else {
            return std::numeric_limits<double>::quiet_NaN();
        }
    });
    check_column<uint64_t>(*segments[0], "count_int", unique_grouping_values, [](size_t idx) {
        if (idx%2 == 0) {
            return uint64_t(10);
        }
        else {
            return uint64_t(0);
        }
    });
}

TEST(Clause, AggregationSparseGroupby) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation("int_sparse_repeated_values", {{"sum_int", "sum"}, {"min_int", "min"}, {"max_int", "max"}, {"mean_int", "mean"}, {"count_int", "count"}});
    aggregation.set_component_manager(component_manager);

    size_t num_rows{100};
    size_t unique_grouping_values{10};
    // 1 more group because of missing values
    size_t unique_groups{unique_grouping_values + 1};
    auto proc_unit = ProcessingUnit{generate_sparse_groupby_testing_segment(num_rows, unique_grouping_values)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto aggregated = gather_entities(component_manager, aggregation.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(1, aggregated.size());
    ASSERT_TRUE(aggregated[0].segments_.has_value());
    auto segments = aggregated[0].segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_groups, [unique_grouping_values](size_t idx) -> int64_t {
        if (idx == 0) {
            return 495;
        } else {
            return 450 - static_cast<int64_t>(idx % unique_grouping_values);
        }
    });
    check_column<int64_t>(*segments[0], "min_int", unique_groups, [](size_t idx) -> int64_t {
        if (idx == 0) {
            return 0;
        } else {
            return idx;
        }
    });
    check_column<int64_t>(*segments[0], "max_int", unique_groups, [unique_grouping_values](size_t idx) -> int64_t {
        if (idx == 0) {
            return 99;
        }
        else if (idx == 9) {
            return 89;
        } else {
            return 90 + idx % unique_grouping_values;
        }
    });
    check_column<double>(*segments[0], "mean_int", unique_groups, [unique_grouping_values](size_t idx) -> double {
        if (idx == 0) {
            return 49.5;
        } else {
            return (450 - static_cast<int64_t>(idx % unique_grouping_values)) / 9.;
        }
    });
    check_column<uint64_t>(*segments[0], "count_int", unique_groups, [](size_t idx) -> uint64_t {
        if (idx == 0) {
            return 10;
        } else {
            return 9;
        }
    });
}

TEST(Clause, Passthrough) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    PassthroughClause passthrough;
    passthrough.set_component_manager(component_manager);

    auto seg = get_standard_timeseries_segment("passthrough");
    auto copied = seg.clone();
    auto proc_unit = ProcessingUnit{std::move(seg)};;
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto ret = gather_entities(component_manager, passthrough.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(ret.size(), 1);
    ASSERT_TRUE(ret[0].segments_.has_value());
    ASSERT_EQ(ret[0].segments_->size(), 1);
    ASSERT_EQ(*ret[0].segments_->at(0), copied);
}

TEST(Clause, Sort) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    SortClause sort_clause("time");
    sort_clause.set_component_manager(component_manager);

    auto seg = get_standard_timeseries_segment("sort");
    auto copied = seg.clone();
    std::random_device rng;
    std::mt19937 urng(rng());
    std::shuffle(seg.begin(), seg.end(), urng);
    auto proc_unit = ProcessingUnit{std::move(seg)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto res = gather_entities(component_manager, sort_clause.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(res.size(), 1);
    ASSERT_TRUE(res[0].segments_.has_value());
    ASSERT_EQ(*res[0].segments_->at(0), copied);
}

TEST(Clause, Split) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    SplitClause split_clause(10);
    split_clause.set_component_manager(component_manager);

    std::string symbol{"split"};
    auto seg = get_standard_timeseries_segment(symbol, 100);
    auto copied = seg.clone();
    auto proc_unit = ProcessingUnit{std::move(seg)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto res = gather_entities(component_manager, split_clause.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(res.size(), 10);

    FieldCollection desc;
    const auto& fields = copied.descriptor().fields();
    auto beg = std::begin(fields);
    std::advance(beg, 1);
    for(auto field = beg; field != std::end(fields); ++field) {
        desc.add_field(field->ref());
    }
    SegmentSinkWrapper seg_wrapper(symbol, TimeseriesIndex::default_index(), std::move(desc));
    for (auto& item: res) {
        ASSERT_TRUE(item.segments_.has_value());
        ASSERT_EQ(item.segments_->size(), 1);
        auto segment = *item.segments_->at(0);
        pipelines::FrameSlice slice(segment);
        seg_wrapper.aggregator_.add_segment(std::move(segment), slice, false);
    }
    seg_wrapper.aggregator_.commit();

    ASSERT_EQ(seg_wrapper.segment(), copied);
}

TEST(Clause, Merge) {
    using namespace arcticdb;
    const auto seg_size = 5;
    ScopedConfig max_blocks("Merge.SegmentSize", seg_size);
    auto component_manager = std::make_shared<ComponentManager>();

    auto stream_id = StreamId("Merge");
    StreamDescriptor descriptor{};
    descriptor.add_field(FieldRef{make_scalar_type(DataType::NANOSECONDS_UTC64),"time"});
    MergeClause merge_clause{TimeseriesIndex{"time"}, DenseColumnPolicy{}, stream_id, descriptor};
    merge_clause.set_component_manager(component_manager);

    Composite<EntityIds> entity_ids;
    std::vector<SegmentInMemory> copies;
    const auto num_segs = 2;
    const auto num_rows = 5;
    for(auto x = 0u; x < num_segs; ++x) {
        auto symbol = fmt::format("merge_{}", x);
        auto seg = get_standard_timeseries_segment(symbol, 10);
        copies.emplace_back(seg.clone());
        auto proc_unit = ProcessingUnit{std::move(seg)};
        entity_ids.push_back(push_entities(component_manager, std::move(proc_unit)));
    }

    auto res = gather_entities(component_manager, merge_clause.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(res.size(), 4u);
    for(auto i = 0; i < num_rows * num_segs; ++i) {
        auto& output_seg = *res[i / seg_size].segments_->at(0);
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
