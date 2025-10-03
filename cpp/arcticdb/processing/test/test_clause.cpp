/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/grouper.hpp>

template<typename T>
void segment_scalar_assert_all_values_equal(
        const arcticdb::ProcessingUnit& proc_unit, const arcticdb::ColumnName& name,
        const std::unordered_set<T>& expected, size_t expected_row_count
) {
    auto segment = *proc_unit.segments_->front();
    segment.init_column_map();
    auto column_index = segment.column_index(name.value).value();
    size_t row_counter = 0;
    for (const auto& row : segment) {
        if (auto maybe_val = row.scalar_at<T>(column_index); maybe_val) {
            ASSERT_THAT(expected, testing::Contains(maybe_val.value()));
            row_counter++;
        }
    }

    ASSERT_EQ(expected_row_count, row_counter);
}

void segment_string_assert_all_values_equal(
        const arcticdb::ProcessingUnit& proc_unit, const arcticdb::ColumnName& name, std::string_view expected,
        size_t expected_row_count
) {
    auto segment = *proc_unit.segments_->front();
    segment.init_column_map();
    auto column_index = segment.column_index(name.value).value();
    size_t row_counter = 0;
    for (auto row : segment) {
        if (auto maybe_val = row.string_at(column_index); maybe_val) {
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
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));
    auto processed = partition.process(std::move(entity_ids));

    ASSERT_TRUE(processed.empty());
}

TEST(Clause, AggregationEmptyColumn) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation(
            "int_repeated_values",
            {{"sum", "empty_sum", "empty_sum"},
             {"min", "empty_min", "empty_min"},
             {"max", "empty_max", "empty_max"},
             {"mean", "empty_mean", "empty_mean"},
             {"count", "empty_count", "empty_count"}}
    );
    aggregation.set_component_manager(component_manager);

    constexpr size_t num_rows{100};
    constexpr size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_empty_segment(num_rows, unique_grouping_values)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    const auto aggregated =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, aggregation.process(std::move(entity_ids))
            );
    ASSERT_TRUE(aggregated.segments_.has_value());
    const auto segments = aggregated.segments_.value();
    ASSERT_EQ(1, segments.size());
    const auto segment = segments[0];

    // Empty columns are not added to the segment.
    // Calls to modify_segment are adding the field and then the NullValueReducer will fill in with the correct values
    ASSERT_FALSE(segment->column_index("empty_min").has_value());
    ASSERT_FALSE(segment->column_index("empty_max").has_value());
    ASSERT_FALSE(segment->column_index("empty_mean").has_value());
    ASSERT_FALSE(segment->column_index("empty_count").has_value());
    ASSERT_FALSE(segment->column_index("empty_sum").has_value());
}

namespace aggregation_test {
template<class T, class F>
void check_column(arcticdb::SegmentInMemory segment, std::string_view column_name, std::size_t ugv, F&& f) {
    const auto column_index = segment.column_index(column_name);
    ASSERT_TRUE(column_index.has_value());
    const auto& column = segment.column(*column_index);
    auto dt = arcticdb::data_type_from_raw_type<T>();
    ASSERT_EQ(dt, column.type().data_type());
    for (std::size_t idx = 0u; idx < ugv; ++idx) {
        if constexpr (std::is_floating_point_v<T>) {
            ASSERT_EQ(f(idx), column.scalar_at<T>(idx));
        } else {
            ASSERT_EQ(f(idx), column.scalar_at<T>(idx));
        }
    }
}
} // namespace aggregation_test

TEST(Clause, AggregationColumn) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation(
            "int_repeated_values",
            {{"sum", "sum_int", "sum_int"},
             {"min", "min_int", "min_int"},
             {"max", "max_int", "max_int"},
             {"mean", "mean_int", "mean_int"},
             {"count", "count_int", "count_int"}}
    );
    aggregation.set_component_manager(component_manager);

    constexpr size_t num_rows{100};
    constexpr size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_segment(num_rows, unique_grouping_values)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto aggregated =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, aggregation.process(std::move(entity_ids))
            );
    ASSERT_TRUE(aggregated.segments_.has_value());
    auto segments = aggregated.segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_grouping_values, [](size_t idx) { return 450 + 10 * idx; });
    check_column<int64_t>(*segments[0], "min_int", unique_grouping_values, [](size_t idx) { return idx; });
    check_column<int64_t>(*segments[0], "max_int", unique_grouping_values, [](size_t idx) { return 90 + idx; });
    check_column<double>(*segments[0], "mean_int", unique_grouping_values, [](size_t idx) { return double(45 + idx); });
    check_column<uint64_t>(*segments[0], "count_int", unique_grouping_values, [](size_t) { return 10; });
}

TEST(Clause, AggregationSparseColumn) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation(
            "int_repeated_values",
            {{"sum", "sum_int", "sum_int"},
             {"min", "min_int", "min_int"},
             {"max", "max_int", "max_int"},
             {"mean", "mean_int", "mean_int"},
             {"count", "count_int", "count_int"}}
    );
    aggregation.set_component_manager(component_manager);

    constexpr size_t num_rows{100};
    constexpr size_t unique_grouping_values{10};
    auto proc_unit = ProcessingUnit{generate_groupby_testing_sparse_segment(num_rows, unique_grouping_values)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    const auto aggregated =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, aggregation.process(std::move(entity_ids))
            );
    ASSERT_TRUE(aggregated.segments_.has_value());
    const auto segments = aggregated.segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_grouping_values, [](size_t idx) -> int64_t {
        return idx % 2 == 0 ? 450 + 10 * idx : 0;
    });
    check_column<int64_t>(*segments[0], "min_int", unique_grouping_values, [](size_t idx) -> std::optional<int64_t> {
        return idx % 2 == 0 ? std::make_optional<int64_t>(idx) : std::nullopt;
    });
    check_column<int64_t>(*segments[0], "max_int", unique_grouping_values, [](size_t idx) -> std::optional<int64_t> {
        return idx % 2 == 0 ? std::make_optional<int64_t>(90 + idx) : std::nullopt;
    });
    check_column<double>(*segments[0], "mean_int", unique_grouping_values, [](size_t idx) -> std::optional<double> {
        return idx % 2 == 0 ? std::make_optional<double>(45 + idx) : std::nullopt;
    });
    check_column<uint64_t>(
            *segments[0],
            "count_int",
            unique_grouping_values,
            [](size_t idx) -> std::optional<uint64_t> {
                return idx % 2 == 0 ? std::make_optional<uint64_t>(10) : std::nullopt;
            }
    );
}

TEST(Clause, AggregationSparseGroupby) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    AggregationClause aggregation(
            "int_sparse_repeated_values",
            {{"sum", "sum_int", "sum_int"},
             {"min", "min_int", "min_int"},
             {"max", "max_int", "max_int"},
             {"mean", "mean_int", "mean_int"},
             {"count", "count_int", "count_int"}}
    );
    aggregation.set_component_manager(component_manager);

    const size_t num_rows{100};
    const size_t unique_grouping_values{10};
    // 1 more group because of missing values
    size_t unique_groups{unique_grouping_values + 1};
    auto proc_unit = ProcessingUnit{generate_sparse_groupby_testing_segment(num_rows, unique_grouping_values)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto aggregated =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, aggregation.process(std::move(entity_ids))
            );
    ASSERT_TRUE(aggregated.segments_.has_value());
    auto segments = aggregated.segments_.value();
    ASSERT_EQ(1, segments.size());

    using aggregation_test::check_column;
    check_column<int64_t>(*segments[0], "sum_int", unique_groups, [](size_t idx) -> int64_t {
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
    check_column<int64_t>(*segments[0], "max_int", unique_groups, [](size_t idx) -> int64_t {
        if (idx == 0) {
            return 99;
        } else if (idx == 9) {
            return 89;
        } else {
            return 90 + idx % unique_grouping_values;
        }
    });
    check_column<double>(*segments[0], "mean_int", unique_groups, [](size_t idx) -> double {
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
    auto proc_unit = ProcessingUnit{std::move(seg)};
    ;
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto ret = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, passthrough.process(std::move(entity_ids))
    );
    ASSERT_TRUE(ret.segments_.has_value());
    ASSERT_EQ(ret.segments_->size(), 1);
    ASSERT_EQ(*ret.segments_->at(0), copied);
}

TEST(Clause, Sort) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    SortClause sort_clause("time", size_t(0));
    sort_clause.set_component_manager(component_manager);

    auto seg = get_standard_timeseries_segment("sort");
    auto copied = seg.clone();
    std::random_device rng;
    std::mt19937 urng(rng());
    std::shuffle(seg.begin(), seg.end(), urng);
    auto proc_unit = ProcessingUnit{std::move(seg)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto res = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, sort_clause.process(std::move(entity_ids))
    );
    ASSERT_TRUE(res.segments_.has_value());
    ASSERT_EQ(*res.segments_->at(0), copied);
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
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto res = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, split_clause.process(std::move(entity_ids))
    );
    ASSERT_TRUE(res.segments_.has_value());
    ASSERT_EQ(res.segments_->size(), 10);

    FieldCollection desc;
    const auto& fields = copied.descriptor().fields();
    auto beg = std::begin(fields);
    std::advance(beg, 1);
    for (auto field = beg; field != std::end(fields); ++field) {
        desc.add_field(field->ref());
    }
    SegmentSinkWrapper seg_wrapper(symbol, TimeseriesIndex::default_index(), std::move(desc));
    for (auto segment : res.segments_.value()) {
        pipelines::FrameSlice slice(*segment);
        seg_wrapper.aggregator_.add_segment(std::move(*segment), slice, false);
    }
    seg_wrapper.aggregator_.commit();

    ASSERT_EQ(seg_wrapper.segment(), copied);
}

TEST(Clause, Merge) {
    using namespace arcticdb;
    const auto seg_size = 30;
    ScopedConfig max_blocks("Merge.SegmentSize", seg_size);
    auto component_manager = std::make_shared<ComponentManager>();

    const auto num_segs = 4;
    const auto num_rows = 20;

    const auto stream_id = StreamId("Merge");
    const auto seg = get_standard_timeseries_segment(std::get<StringId>(stream_id), num_rows);
    MergeClause merge_clause{TimeseriesIndex{"time"}, SparseColumnPolicy{}, stream_id, seg.descriptor(), false};
    merge_clause.set_component_manager(component_manager);

    std::vector<SegmentInMemory> segs;
    for (auto x = 0u; x < num_segs; ++x) {
        segs.emplace_back(SegmentInMemory{seg.descriptor().clone(), num_rows / num_segs, AllocationType::DYNAMIC});
    }

    for (auto i = 0u; i < num_rows; ++i) {
        auto& current = segs[i % num_segs];
        for (auto j = 0U; j < seg.descriptor().field_count(); ++j) {
            details::visit_scalar(current.column(j).type(), [&current, &seg, i, j](auto&& tag) {
                using DT = std::decay_t<decltype(tag)>;
                const auto data_type = DT::DataTypeTag::data_type;
                using RawType = typename DT::DataTypeTag::raw_type;
                if constexpr (is_sequence_type(data_type)) {
                    current.set_string(j, seg.string_at(i, j).value());
                } else {
                    current.set_scalar<RawType>(j, seg.scalar_at<RawType>(i, j).value());
                }
            });
        }
        current.end_row();
    }

    std::vector<EntityId> entity_ids;
    for (auto x = 0u; x < num_segs; ++x) {
        auto proc_unit = ProcessingUnit{std::move(segs[x])};
        entity_ids.push_back(push_entities(*component_manager, std::move(proc_unit))[0]);
    }

    std::vector<EntityId> processed_ids = merge_clause.process(std::move(entity_ids));
    std::vector<std::vector<EntityId>> vec;
    vec.emplace_back(std::move(processed_ids));
    auto repartitioned = merge_clause.structure_for_processing(std::move(vec));
    auto res = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, std::move(repartitioned.at(0))
    );
    ASSERT_TRUE(res.segments_.has_value());
    ASSERT_EQ(res.segments_->size(), 1u);
    ASSERT_EQ(*res.segments_->at(0), seg);
}
