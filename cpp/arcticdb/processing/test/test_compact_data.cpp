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

//// First element is the input, second element is the expected output
// class CompactDataStructureRowRangesFixture
//     : public ::testing::TestWithParam<std::pair<std::set<RowRange>, std::set<RowRange>>> {};
//
// TEST_P(CompactDataStructureRowRangesFixture, All) {
//     const auto [row_ranges, expected_output] = GetParam();
//     CompactDataClause clause{10};
//     auto res = clause.structure_row_ranges(row_ranges);
//     ASSERT_EQ(res, expected_output);
// }
//
// std::vector<std::pair<std::set<RowRange>, std::set<RowRange>>> parameters{
//         // No-ops (output same as input)
//         // A single slice of any size is always just returned as is, regardless of size
//         {{{0, 4}}, {{0, 4}}},
//         {{{0, 9}}, {{0, 9}}},
//         {{{0, 12}}, {{0, 12}}},
//         {{{0, 20}}, {{0, 20}}},
//         {{{0, 4}}, {{0, 4}}},
//         // Everything is already perfectly sliced
//         {{{0, 10}, {10, 20}, {20, 30}, {30, 36}}, {{0, 10}, {10, 20}, {20, 30}, {30, 36}}},
//         // Everything is within the acceptable range, which is 6-12 rows inclusive with rows_per_segment_ == 10 and
//         will
//         // not get better through re-slicing
//         {{{0, 9}, {9, 15}, {15, 23}, {23, 33}}, {{0, 9}, {9, 15}, {15, 23}, {23, 33}}},
//         // Output differs to input
//         // Small append - everything is already perfectly sliced except for small last row slice, which should be
//         // attached to the previous slice
//         {{{0, 10}, {10, 20}, {20, 30}, {30, 35}}, {{0, 10}, {10, 20}, {20, 35}}},
//         // Small update - everything is already perfectly sliced except for a small row slice inserted into the
//         middle
//         {{{0, 10}, {10, 11}, {11, 21}}, {{0, 10}, {10, 21}}},
//         // Uniformly fragmented
//         {{{0, 5}, {5, 10}, {10, 15}, {15, 20}, {20, 25}}, {{0, 10}, {10, 25}}},
//         // Edge case - first slice is too small so will be combined with next slice even though this is as big as we
//         // want them to get. Final slice is also too small so will be combined with the previous slice.
//         {{{0, 5}, {5, 17}, {17, 22}}, {{0, 22}}}
// };
//
// INSTANTIATE_TEST_SUITE_P(
//         CompactDataStructureRowRanges, CompactDataStructureRowRangesFixture, ::testing::ValuesIn(parameters)
//);

TEST(CompactData, StructureForProcessingBasic) {
    ColRange col_range{1, 2};
    // First 2 row slices need no compaction, last 2 will be combined
    RangesAndKey first({0, 10}, col_range, {});
    RangesAndKey second({10, 20}, col_range, {});
    RangesAndKey third({20, 30}, col_range, {});
    RangesAndKey fourth({30, 35}, col_range, {});
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{third, second, fourth, first};
    CompactDataClause clause{10};
    auto proc_unit_ids = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], third);
    ASSERT_EQ(ranges_and_keys[1], fourth);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}
