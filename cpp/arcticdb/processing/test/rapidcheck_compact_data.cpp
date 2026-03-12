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

using namespace arcticdb;

RC_GTEST_PROP(CompactData, StructureRowRanges, ()) {
    const auto rows_per_segment = *rc::gen::inRange<uint64_t>(1, 1'000'000'000);
    auto row_range_boundaries =
            *rc::gen::unique<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(1, rows_per_segment));
    row_range_boundaries.emplace_back(0);
    RC_PRE(row_range_boundaries.size() >= 2);
    std::ranges::sort(row_range_boundaries);
    // Turn this sorted list of row range boundaries into a set of row ranges
    std::set<RowRange> row_ranges;
    for (auto row_range_boundary = row_range_boundaries.cbegin();
         row_range_boundary != std::prev(row_range_boundaries.cend());
         ++row_range_boundary) {
        row_ranges.emplace(*row_range_boundary, *std::next(row_range_boundary));
    }
    CompactDataClause clause{rows_per_segment};
    const auto res = clause.structure_row_ranges(row_ranges);
    // If there are fewer total rows than min_rows_per_segment_ then everything will be combined into one
    const auto min_rows_per_segment = std::min(clause.min_rows_per_segment_, row_range_boundaries.back());
    const auto max_rows_per_segment = clause.max_rows_per_segment_;
    for (const auto& row_range : res) {
        const auto rows = row_range.diff();
        RC_ASSERT(rows >= min_rows_per_segment);
        // This invariant guarantees that the maximum number of rows passed to a single call to
        // CompactDataClause::process can be split in 2 to produce 2 row slices each with at most max_rows_per_segment
        RC_ASSERT(rows <= 2 * max_rows_per_segment);
    }
}

RC_GTEST_PROP(CompactData, StructureForProcessing, ()) {
    // TODO: Factor out common setup with test above into own function[s]
    const auto rows_per_segment = *rc::gen::inRange<uint64_t>(1, 1'000'000'000);
    auto row_range_boundaries =
            *rc::gen::unique<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(1, rows_per_segment));
    row_range_boundaries.emplace_back(0);
    RC_PRE(row_range_boundaries.size() >= 2);
    std::ranges::sort(row_range_boundaries);
    // Turn this sorted list of row range boundaries into a list of row ranges
    std::vector<RowRange> row_ranges;
    for (auto row_range_boundary = row_range_boundaries.cbegin();
         row_range_boundary != std::prev(row_range_boundaries.cend());
         ++row_range_boundary) {
        row_ranges.emplace_back(*row_range_boundary, *std::next(row_range_boundary));
    }
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
    CompactDataClause clause{rows_per_segment};
    const auto proc_unit_ids = clause.structure_for_processing(ranges_and_keys);
    // If there are fewer total rows than min_rows_per_segment_ then everything will be combined into one
    const auto min_rows_per_segment = std::min(clause.min_rows_per_segment_, row_range_boundaries.back());
    const auto max_rows_per_segment = clause.max_rows_per_segment_;
    for (const auto& proc_unit : proc_unit_ids) {
        RC_ASSERT_FALSE(proc_unit.empty());
        const auto& col_range = ranges_and_keys.at(proc_unit.front()).col_range();
        const auto total_rows = ranges_and_keys.at(proc_unit.back()).row_range().second -
                                ranges_and_keys.at(proc_unit.front()).row_range().first;
        RC_ASSERT(total_rows >= min_rows_per_segment);
        RC_ASSERT(total_rows <= max_rows_per_segment);
        for (auto idx = proc_unit.cbegin(); idx != proc_unit.cend(); ++idx) {
            RC_ASSERT(ranges_and_keys.at(*idx).col_range() == col_range);
            if (std::next(idx) != proc_unit.cend()) {
                RC_ASSERT(
                        ranges_and_keys.at(*idx).row_range().second ==
                        ranges_and_keys.at(*std::next(idx)).row_range().first
                );
            }
        }
    }
    //    const auto flattened = flatten_entities(std::move(proc_unit_ids));
}
