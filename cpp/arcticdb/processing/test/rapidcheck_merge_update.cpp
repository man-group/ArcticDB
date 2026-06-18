/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <set>
#include <utility>
#include <vector>

// structure_by_time_slice must split the row slices into maximal cliques of intersecting time ranges. The
// property checked here is exactly that: for every element of every output group the range intersects all
// other ranges it shares a group with, and does not intersect any range it never shares a group with. A
// range may bridge two adjacent cliques and therefore appear in more than one group, so "its group" is
// taken across all the groups it appears in.
RC_GTEST_PROP(StructureByTimeSlice, GroupsAreMaximalCliques, ()) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    constexpr timestamp ts_min = std::numeric_limits<timestamp>::min();
    constexpr timestamp ts_max = std::numeric_limits<timestamp>::max();

    // Number of column slices per row slice, between 1 and 5.
    const size_t num_col_slices = *rc::gen::inRange<size_t>(1, 6);

    // Number of row slices. Drawn with arbitrary<size_t> and bounded only so the input allocation and the
    // O(n^2) verification below stay tractable (arbitrary<size_t> on its own ranges up to the full 64 bits).
    constexpr size_t max_extra_row_slices = 256;
    const size_t num_row_slices = std::min(*rc::gen::arbitrary<size_t>(), max_extra_row_slices) + 1;

    // Generate the row slice time ranges as a monotonically non-decreasing sequence. All values stay below
    // ts_max - 1 and above ts_min so the +1/-1 arithmetic and the inRange bounds never overflow, which avoids
    // any need for RC_PRE. There is no bound on how large a span or a gap can be.
    //   start0      = inRange(ts_min + 1, ts_max - 2)        any value, negatives included
    //   end_i       = inRange(start_i + 1, ts_max - 1)       strictly greater so the slice is non-empty
    //   start_{i+1} = inRange(end_i - 1, ts_max - 2)         >= the last value of slice i, so a1 >= b0 - 1
    std::vector<TimestampRange> row_slice_time_ranges;
    row_slice_time_ranges.reserve(num_row_slices);
    timestamp slice_start = *rc::gen::inRange<timestamp>(ts_min + 1, ts_max - 2);
    for (size_t i = 0; i < num_row_slices; ++i) {
        const timestamp slice_end = *rc::gen::inRange<timestamp>(slice_start + 1, ts_max - 1);
        row_slice_time_ranges.emplace_back(slice_start, slice_end);
        slice_start = *rc::gen::inRange<timestamp>(slice_end - 1, ts_max - 2);
    }

    // Build the input: each row slice has num_col_slices contiguous column slices that share its time range.
    std::vector<RangesAndKey> ranges;
    ranges.reserve(num_row_slices * num_col_slices);
    for (size_t row_slice = 0; row_slice < num_row_slices; ++row_slice) {
        const auto& [range_start, range_end] = row_slice_time_ranges[row_slice];
        for (size_t col = 0; col < num_col_slices; ++col) {
            ranges.emplace_back(
                    RowRange{row_slice * 5, row_slice * 5 + 5},
                    ColRange{1 + col, 2 + col},
                    AtomKeyBuilder().start_index(range_start).end_index(range_end).build<KeyType::TABLE_DATA>("t")
            );
        }
    }

    // Shuffle so the result must not depend on input order (the function sorts internally).
    std::mt19937 generator(*rc::gen::arbitrary<uint32_t>());
    std::ranges::shuffle(ranges, generator);

    const std::vector<std::vector<size_t>> groups = structure_by_time_slice(ranges);

    // After the call `ranges` is sorted; the indexes in the groups refer to the sorted order.
    const size_t num_entries = ranges.size();
    const auto intersects = [&](size_t i, size_t j) {
        const TimestampRange a = ranges[i].key_.time_range();
        const TimestampRange b = ranges[j].key_.time_range();
        // Half-open [first, second) intervals share a value iff each starts before the other ends.
        return a.first < b.second && b.first < a.second;
    };

    // Every group is non-empty and references valid indexes, and every entry appears in at least one group.
    std::vector<bool> covered(num_entries, false);
    for (const std::vector<size_t>& group : groups) {
        RC_ASSERT(!group.empty());
        for (const size_t idx : group) {
            RC_ASSERT(idx < num_entries);
            covered[idx] = true;
        }
    }
    for (size_t idx = 0; idx < num_entries; ++idx) {
        RC_ASSERT(covered[idx]);
    }

    // The unordered index pairs that share at least one group.
    std::set<std::pair<size_t, size_t>> co_grouped;
    for (const std::vector<size_t>& group : groups) {
        for (size_t a = 0; a < group.size(); ++a) {
            for (size_t b = a + 1; b < group.size(); ++b) {
                co_grouped.insert({std::min(group[a], group[b]), std::max(group[a], group[b])});
            }
        }
    }

    // Correctness: two ranges share a group iff they intersect. This is both directions of the property - a
    // range intersects everything it is grouped with (each group is a clique) and does not intersect anything
    // it is never grouped with (the grouping is maximal).
    for (size_t i = 0; i < num_entries; ++i) {
        for (size_t j = i + 1; j < num_entries; ++j) {
            RC_ASSERT(co_grouped.contains({i, j}) == intersects(i, j));
        }
    }
}
