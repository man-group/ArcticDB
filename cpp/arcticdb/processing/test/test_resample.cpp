/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>

//TEST(Resample, StructureForProcessingBasic) {
//    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
//    // No column slicing
//    using namespace arcticdb;
//    StreamId sym{"sym"};
//    ColRange col_range{1, 2};
//    RowRange row_range_1{0, 100};
//    RowRange row_range_2{100, 200};
//    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    RangesAndKey top(row_range_1, col_range, key_1);
//    RangesAndKey bottom(row_range_2, col_range, key_2);
//    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
//    std::vector<RangesAndKey> ranges_and_keys{bottom, top};
//
//    std::vector<timestamp> bucket_boundaries{1, 500, 1500, 2500, 2999};
//
//    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
//    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
//    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 2);
//    ASSERT_EQ(ranges_and_keys[0], top);
//    ASSERT_EQ(ranges_and_keys[1], bottom);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0}, {1}};
//    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
//}
//
//TEST(Resample, StructureForProcessingColumnSlicing) {
//    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
//    // Two column slices
//    using namespace arcticdb;
//    StreamId sym{"sym"};
//    ColRange col_range_1{1, 2};
//    ColRange col_range_2{2, 3};
//    RowRange row_range_1{0, 100};
//    RowRange row_range_2{100, 200};
//    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_2 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_3 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    auto key_4 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    RangesAndKey top_left(row_range_1, col_range_1, key_1);
//    RangesAndKey top_right(row_range_1, col_range_2, key_2);
//    RangesAndKey bottom_left(row_range_2, col_range_1, key_3);
//    RangesAndKey bottom_right(row_range_2, col_range_2, key_4);
//    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
//    std::vector<RangesAndKey> ranges_and_keys{top_right, bottom_left, bottom_right, top_left};
//
//    std::vector<timestamp> bucket_boundaries{1, 500, 1500, 2500, 2999};
//
//    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
//    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
//    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 4);
//    ASSERT_EQ(ranges_and_keys[0], top_left);
//    ASSERT_EQ(ranges_and_keys[1], top_right);
//    ASSERT_EQ(ranges_and_keys[2], bottom_left);
//    ASSERT_EQ(ranges_and_keys[3], bottom_right);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {2, 3}};
//    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
//}
//
//TEST(Resample, StructureForProcessingOverlap) {
//    // Bucket boundaries such that the first processing unit needs rows from the second row slice
//    // No column slicing
//    using namespace arcticdb;
//    StreamId sym{"sym"};
//    ColRange col_range{1, 2};
//    RowRange row_range_1{0, 100};
//    RowRange row_range_2{100, 200};
//    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    RangesAndKey top(row_range_1, col_range, key_1);
//    RangesAndKey bottom(row_range_2, col_range, key_2);
//    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
//    std::vector<RangesAndKey> ranges_and_keys{bottom, top};
//
//    std::vector<timestamp> bucket_boundaries{1, 500, 2500, 2999};
//
//    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
//    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
//    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 2);
//    ASSERT_EQ(ranges_and_keys[0], top);
//    ASSERT_EQ(ranges_and_keys[1], bottom);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {1}};
//    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
//}
//
//TEST(Resample, StructureForProcessingSubsumed) {
//    // Bucket boundaries such that the first processing unit needs all of the rows from the second and third row slices,
//    // such that there is only one element in the returned vector
//    // No column slicing
//    using namespace arcticdb;
//    StreamId sym{"sym"};
//    ColRange col_range{1, 2};
//    RowRange row_range_1{0, 100};
//    RowRange row_range_2{100, 200};
//    RowRange row_range_3{200, 300};
//    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    auto key_3 = AtomKeyBuilder().start_index(3000).end_index(4000).build<KeyType::TABLE_DATA>(sym);
//    RangesAndKey top(row_range_1, col_range, key_1);
//    RangesAndKey middle(row_range_2, col_range, key_2);
//    RangesAndKey bottom(row_range_3, col_range, key_3);
//    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
//    std::vector<RangesAndKey> ranges_and_keys{bottom, middle, top};
//
//    std::vector<timestamp> bucket_boundaries{1, 500, 4500};
//
//    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
//    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
//    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 3);
//    ASSERT_EQ(ranges_and_keys[0], top);
//    ASSERT_EQ(ranges_and_keys[1], middle);
//    ASSERT_EQ(ranges_and_keys[2], bottom);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1, 2}};
//    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
//}
//
//TEST(Resample, StructureForProcessingExactBoundary) {
//    // Bucket boundaries such that the first processing unit needs rows from the second row slice when the right
//    // boundary is closed, but does not when the left boundary is closed
//    // No column slicing
//    using namespace arcticdb;
//    StreamId sym{"sym"};
//    ColRange col_range{1, 2};
//    RowRange row_range_1{0, 100};
//    RowRange row_range_2{100, 200};
//    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
//    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
//    RangesAndKey top(row_range_1, col_range, key_1);
//    RangesAndKey bottom(row_range_2, col_range, key_2);
//    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
//    std::vector<RangesAndKey> ranges_and_keys{bottom, top};
//
//    std::vector<timestamp> bucket_boundaries{1, 500, 2000, 2500, 2999};
//
//    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
//    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
//    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 2);
//    ASSERT_EQ(ranges_and_keys[0], top);
//    ASSERT_EQ(ranges_and_keys[1], bottom);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids_left{{0}, {1}};
//    ASSERT_EQ(expected_proc_unit_ids_left, proc_unit_ids);
//
//    resample_clause.closed_boundary_ = ResampleClosedBoundary::RIGHT;
//    proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
//    ASSERT_EQ(ranges_and_keys.size(), 2);
//    ASSERT_EQ(ranges_and_keys[0], top);
//    ASSERT_EQ(ranges_and_keys[1], bottom);
//    std::vector<std::vector<size_t>> expected_proc_unit_ids_right{{0, 1}, {1}};
//    ASSERT_EQ(expected_proc_unit_ids_right, proc_unit_ids);
//}

//TEST(Resample, FindBuckets) {
//    using namespace arcticdb;
//    ResampleClause resample_left("left", ResampleClosedBoundary::LEFT);
//    ResampleClause resample_right("right", ResampleClosedBoundary::RIGHT);
//    // Enough bucket boundaries to test all the interesting cases
//    resample_left.set_bucket_boundaries({0, 10, 20, 30, 40});
//    resample_right.set_bucket_boundaries({0, 10, 20, 30, 40});
//    std::pair<std::vector<timestamp>::const_iterator, std::vector<timestamp>::const_iterator> left_res;
//    std::pair<std::vector<timestamp>::const_iterator, std::vector<timestamp>::const_iterator> right_res;
//
//    // Wholly contained in first bucket
//    left_res = resample_left.find_buckets(5, 6, true);
//    ASSERT_EQ(*left_res.first, 0);
//    ASSERT_EQ(*left_res.second, 10);
//    // Wholly contained in middle bucket
//    left_res = resample_left.find_buckets(12, 13, true);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 20);
//    // Wholly contained in last bucket
//    left_res = resample_left.find_buckets(35, 37, true);
//    ASSERT_EQ(*left_res.first, 30);
//    ASSERT_EQ(*left_res.second, 40);
//
//    // Spanning multiple buckets
//    left_res = resample_left.find_buckets(5, 15, true);
//    ASSERT_EQ(*left_res.first, 0);
//    ASSERT_EQ(*left_res.second, 20);
//    left_res = resample_left.find_buckets(15, 25, true);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 30);
//    left_res = resample_left.find_buckets(15, 35, true);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 40);
//
//    // Spanning multiple buckets, not responsible for the first bucket
//    left_res = resample_left.find_buckets(5, 15, false);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 20);
//    left_res = resample_left.find_buckets(15, 25, false);
//    ASSERT_EQ(*left_res.first, 20);
//    ASSERT_EQ(*left_res.second, 30);
//    left_res = resample_left.find_buckets(15, 35, false);
//    ASSERT_EQ(*left_res.first, 20);
//    ASSERT_EQ(*left_res.second, 40);
//
//    // First bucket starts after the first timestamp
//    left_res = resample_left.find_buckets(-5, 15, true);
//    ASSERT_EQ(*left_res.first, 0);
//    ASSERT_EQ(*left_res.second, 20);
//    // Last bucket ends before the last timestamp
//    left_res = resample_left.find_buckets(15, 45, true);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 40);
//
//    // Bucket boundary matching first and last timestamps
//    left_res = resample_left.find_buckets(10, 20, true);
//    ASSERT_EQ(*left_res.first, 10);
//    ASSERT_EQ(*left_res.second, 30);
//    right_res = resample_right.find_buckets(10, 20, true);
//    ASSERT_EQ(*right_res.first, 0);
//    ASSERT_EQ(*right_res.second, 20);
//}

TEST(Resample, ProcessOneSegment) {
    using namespace arcticdb;
    auto component_manager = std::make_shared<ComponentManager>();

    ResampleClause resample("dummy", ResampleClosedBoundary::LEFT);
    resample.set_component_manager(component_manager);
    resample.set_aggregations({{"sum_column", "sum"}});
    resample.set_bucket_boundaries({-1, 2, 5});

    using index_TDT = TypeDescriptorTag<DataTypeTag<DataType::NANOSECONDS_UTC64>, DimensionTag<Dimension ::Dim0>>;
    auto index_column = std::make_shared<Column>(static_cast<TypeDescriptor>(index_TDT{}), 0, false, true);
    using col_TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto sum_column = std::make_shared<Column>(static_cast<TypeDescriptor>(col_TDT{}), 0, false, true);
    size_t num_rows{5};
    for(size_t idx = 0; idx < num_rows; ++idx) {
        index_column->set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
        sum_column->set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
    }
    SegmentInMemory seg;
    seg.add_column(scalar_field(index_column->type().data_type(), "index"), index_column);
    seg.add_column(scalar_field(sum_column->type().data_type(), "sum_column"), sum_column);
    seg.set_row_id(num_rows - 1);

    auto proc_unit = ProcessingUnit{std::move(seg)};
    auto entity_ids = Composite<EntityIds>(push_entities(component_manager, std::move(proc_unit)));

    auto resampled = gather_entities(component_manager, resample.process(std::move(entity_ids))).as_range();
    ASSERT_EQ(1, resampled.size());
    ASSERT_TRUE(resampled[0].segments_.has_value());
    auto segments = resampled[0].segments_.value();
    ASSERT_EQ(1, segments.size());
    auto resampled_seg = *segments[0];

    auto index_column_index = resampled_seg.column_index("index");
    ASSERT_TRUE(index_column_index.has_value());
    auto& resampled_index_column = resampled_seg.column(*index_column_index);
    ASSERT_EQ(-1, resampled_index_column.scalar_at<int64_t>(0));
    ASSERT_EQ(2, resampled_index_column.scalar_at<int64_t>(1));

    auto sum_column_index = resampled_seg.column_index("sum_column");
    ASSERT_TRUE(sum_column_index.has_value());
    auto& resampled_sum_column = resampled_seg.column(*sum_column_index);
    ASSERT_EQ(1, resampled_sum_column.scalar_at<int64_t>(0));
    ASSERT_EQ(9, resampled_sum_column.scalar_at<int64_t>(1));
}
