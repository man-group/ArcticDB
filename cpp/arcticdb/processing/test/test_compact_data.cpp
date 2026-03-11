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

TEST(CompactDataStructureRowRanges, NoOp) {
    CompactDataClause clause{10};
    // A single slice of any size is always just returned as is
    auto row_ranges = std::set<RowRange>{{0, 4}};
    auto res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
    row_ranges = std::set<RowRange>{{0, 9}};
    res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
    row_ranges = std::set<RowRange>{{0, 12}};
    res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
    row_ranges = std::set<RowRange>{{0, 20}};
    res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
    // Everything is already perfectly sliced
    row_ranges = std::set<RowRange>{{0, 10}, {10, 20}, {20, 30}, {30, 36}};
    res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
    // Everything is within the acceptable range, which is 6-12 rows inclusive with rows_per_segment_ at 10 and will not
    // get better through re-slicing
    row_ranges = std::set<RowRange>{{0, 9}, {9, 15}, {15, 23}, {23, 33}};
    res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, row_ranges);
}

TEST(CompactDataStructureRowRanges, SmallAppend) {
    CompactDataClause clause{10};
    // Everything is already perfectly sliced except for small last row slice, which should be attached to the previous
    // slice
    std::set<RowRange> row_ranges{{0, 10}, {10, 20}, {20, 30}, {30, 35}};
    std::set<RowRange> expected{{0, 10}, {10, 20}, {20, 35}};
    auto res = clause.structure_row_ranges(row_ranges);
    ASSERT_EQ(res, expected);
}
