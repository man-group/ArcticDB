/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <algorithm>
#include <random>

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/collection_utils.hpp>

using namespace arcticdb;

class CompactDataFixture : public ::testing::Test {
  protected:
    void SetUp() override {
        // There's nothing special about 1000 rows per segment, but using larger upper bounds leads to rapidcheck not
        // selecting very interesting cases (i.e. ones where no compaction is required)
        const auto rows_per_segment = *rc::gen::inRange<uint64_t>(1, 1000);
        auto row_range_boundaries =
                *rc::gen::unique<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(1, 10 * rows_per_segment));
        row_range_boundaries.emplace_back(0);
        RC_PRE(row_range_boundaries.size() >= 2);
        std::ranges::sort(row_range_boundaries);
        // Turn this sorted list of row range boundaries into a set of row ranges
        for (auto row_range_boundary = row_range_boundaries.cbegin();
             row_range_boundary != std::prev(row_range_boundaries.cend());
             ++row_range_boundary) {
            row_ranges.emplace(*row_range_boundary, *std::next(row_range_boundary));
        }
        clause = CompactDataClause{rows_per_segment};
        // If there are fewer total rows than min_rows_per_segment_ then everything will be combined into one
        min_rows_per_segment = std::min(clause->min_rows_per_segment_, row_range_boundaries.back());
    }

    // Optional because the default constructor of CompactDataClause is deleted, but we cannot instantiate it until
    // call to SetUp
    std::optional<CompactDataClause> clause;
    std::set<RowRange> row_ranges;
    uint64_t min_rows_per_segment;
};

RC_GTEST_FIXTURE_PROP(CompactDataFixture, StructureRowRanges, ()) {
    const auto res = clause->structure_row_ranges(row_ranges);
    for (const auto& row_range : res) {
        const auto rows = row_range.diff();
        RC_ASSERT(rows >= min_rows_per_segment);
    }
}

RC_GTEST_FIXTURE_PROP(CompactDataFixture, StructureForProcessing, ()) {
    const auto cols_per_segment = *rc::gen::inRange<uint64_t>(1, 1'000);
    const auto num_col_slices = *rc::gen::inRange<uint64_t>(1, 10);
    std::vector<ColRange> col_ranges;
    for (size_t idx = 0; idx < num_col_slices; ++idx) {
        col_ranges.emplace_back(idx * cols_per_segment, (idx + 1) * cols_per_segment);
    }
    std::vector<RangesAndKey> ranges_and_keys;
    for (const auto& row_range : row_ranges) {
        for (const auto& col_range : col_ranges) {
            ranges_and_keys.emplace_back(row_range, col_range, AtomKey{});
        }
    }
    auto proc_unit_ids = clause->structure_for_processing(ranges_and_keys);
    for (const auto& proc_unit : proc_unit_ids) {
        // There should be no empty processing units
        RC_ASSERT_FALSE(proc_unit.empty());
        const auto total_rows = ranges_and_keys.at(proc_unit.back()).row_range().second -
                                ranges_and_keys.at(proc_unit.front()).row_range().first;
        RC_ASSERT(total_rows >= min_rows_per_segment);
        const auto& col_range = ranges_and_keys.at(proc_unit.front()).col_range();
        for (auto idx = proc_unit.cbegin(); idx != proc_unit.cend(); ++idx) {
            // All segments in a processing unit should have the same column range with static schema
            RC_ASSERT(ranges_and_keys.at(*idx).col_range() == col_range);
            if (std::next(idx) != proc_unit.cend()) {
                // The second entry of the row range for each element of a processing unit should match the first entry
                // of the row range of the next element. e.g. [0, 10), [10, 20),...
                RC_ASSERT(
                        ranges_and_keys.at(*idx).row_range().second ==
                        ranges_and_keys.at(*std::next(idx)).row_range().first
                );
            }
        }
    }
    const auto flattened = util::flatten_vectors(std::move(proc_unit_ids));
    // This is asserting that the order of reading ranges_and_keys is 0, 1, 2,...
    if (!flattened.empty()) {
        for (auto idx = flattened.cbegin(); idx != std::prev(flattened.cend()); ++idx) {
            RC_ASSERT((*idx) + 1 == *std::next(idx));
        }
    }
}
