/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>

using namespace arcticdb;

TEST(CompactData, StructureForProcessingNoOp) {
    // No compaction needed
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 10};
    RowRange row_range_2{10, 20};
    RangesAndKey top(row_range_1, col_range, {});
    RangesAndKey bottom(row_range_2, col_range, {});
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    CompactDataClause compact_data_clause{10};
    auto proc_unit_ids = compact_data_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 0);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(CompactData, StructureForProcessingBasic) {
    // Two segments that need compacting into one
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 10};
    RowRange row_range_2{10, 20};
    RangesAndKey top(row_range_1, col_range, {});
    RangesAndKey bottom(row_range_2, col_range, {});
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    CompactDataClause compact_data_clause{100};
    auto proc_unit_ids = compact_data_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(CompactData, StructureForProcessingColumnSlicing) {
    // Four segments that need compacting into two
    StreamId sym{"sym"};
    ColRange col_range_1{1, 2};
    ColRange col_range_2{2, 3};
    RowRange row_range_1{0, 10};
    RowRange row_range_2{10, 20};
    RangesAndKey top_left(row_range_1, col_range_1, {});
    RangesAndKey top_right(row_range_1, col_range_2, {});
    RangesAndKey bottom_left(row_range_2, col_range_1, {});
    RangesAndKey bottom_right(row_range_2, col_range_2, {});
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom_left, top_right, bottom_right, top_left};

    CompactDataClause compact_data_clause{100};
    auto proc_unit_ids = compact_data_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 4);
    ASSERT_EQ(ranges_and_keys[0], top_left);
    ASSERT_EQ(ranges_and_keys[1], bottom_left);
    ASSERT_EQ(ranges_and_keys[2], top_right);
    ASSERT_EQ(ranges_and_keys[3], bottom_right);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {2, 3}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}
