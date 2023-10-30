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

TEST(Resample, StructureForProcessingBasic) {
    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
    // No column slicing
    using namespace arcticdb;
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    std::vector<timestamp> bucket_boundaries{1, 500, 1500, 2500, 2999};

    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0}, {1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingColumnSlicing) {
    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
    // Two column slices
    using namespace arcticdb;
    StreamId sym{"sym"};
    ColRange col_range_1{1, 2};
    ColRange col_range_2{2, 3};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_3 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    auto key_4 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top_left(row_range_1, col_range_1, key_1);
    RangesAndKey top_right(row_range_1, col_range_2, key_2);
    RangesAndKey bottom_left(row_range_2, col_range_1, key_3);
    RangesAndKey bottom_right(row_range_2, col_range_2, key_4);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{top_right, bottom_left, bottom_right, top_left};

    std::vector<timestamp> bucket_boundaries{1, 500, 1500, 2500, 2999};

    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 4);
    ASSERT_EQ(ranges_and_keys[0], top_left);
    ASSERT_EQ(ranges_and_keys[1], top_right);
    ASSERT_EQ(ranges_and_keys[2], bottom_left);
    ASSERT_EQ(ranges_and_keys[3], bottom_right);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {2, 3}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingOverlap) {
    // Bucket boundaries such that the first processing unit needs rows from the second row slice
    // No column slicing
    using namespace arcticdb;
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    std::vector<timestamp> bucket_boundaries{1, 500, 2500, 2999};

    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingSubsumed) {
    // Bucket boundaries such that the first processing unit needs all of the rows from the second and third row slices,
    // such that there is only one element in the returned vector
    // No column slicing
    using namespace arcticdb;
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    RowRange row_range_3{200, 300};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    auto key_3 = AtomKeyBuilder().start_index(3000).end_index(4000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey middle(row_range_2, col_range, key_2);
    RangesAndKey bottom(row_range_3, col_range, key_3);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, middle, top};

    std::vector<timestamp> bucket_boundaries{1, 500, 4500};

    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 3);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], middle);
    ASSERT_EQ(ranges_and_keys[2], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1, 2}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingExactBoundary) {
    // Bucket boundaries such that the first processing unit needs rows from the second row slice when the right
    // boundary is closed, but does not when the left boundary is closed
    // No column slicing
    using namespace arcticdb;
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    std::vector<timestamp> bucket_boundaries{1, 500, 2000, 2500, 2999};

    ResampleClause resample_clause{"dummy rule", ResampleClosedBoundary::LEFT};
    resample_clause.set_bucket_boundaries(std::move(bucket_boundaries));
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids_left{{0}, {1}};
    ASSERT_EQ(expected_proc_unit_ids_left, proc_unit_ids);

    resample_clause.closed_boundary_ = ResampleClosedBoundary::RIGHT;
    proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys, 0);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids_right{{0, 1}, {1}};
    ASSERT_EQ(expected_proc_unit_ids_right, proc_unit_ids);
}
