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
    auto rows_per_segment = *rc::gen::inRange<uint64_t>(1, 1'000'000'000);
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
    auto res = clause.structure_row_ranges(row_ranges);
    auto min_rows_per_segment = clause.min_rows_per_segment_;
    auto max_rows_per_segment = clause.max_rows_per_segment_;
    // If there are fewer total rows than min_rows_per_segment_ then everything will be combined into one
    min_rows_per_segment = std::min(min_rows_per_segment, row_range_boundaries.back());
    for (const auto& row_range : res) {
        auto rows = row_range.diff();
        RC_ASSERT(rows >= min_rows_per_segment);
        // This invariant guarantees that the maximum number of rows passed to a single call to
        // CompactDataClause::process can be split in 2 to produce 2 row slices each with at most max_rows_per_segment
        RC_ASSERT(rows <= 2 * max_rows_per_segment);
    }
}
