/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <utility>
#include <vector>
using namespace arcticdb;
using namespace arcticdb::pipelines;

namespace {
bool atom_key_range_intersects(const TimestampRange& left, const TimestampRange& right) {
    return left.first < right.second && right.first < left.second;
}

std::vector<RangesAndKey> generate_ranges_and_keys(
        std::span<const TimestampRange> time_ranges, size_t num_row_slices, size_t num_col_slices
) {
    std::vector<RangesAndKey> ranges;
    ranges.reserve(num_row_slices * num_col_slices);
    size_t row_count{};
    for (size_t row_slice = 0; row_slice < num_row_slices; ++row_slice) {
        const auto& [range_start, range_end] = time_ranges[row_slice];
        const size_t num_rows = *rc::gen::inRange<size_t>(1, 100'000);
        for (size_t col = 0; col < num_col_slices; ++col) {
            ranges.emplace_back(
                    RowRange{row_count, row_count + num_rows},
                    ColRange{1 + col, 2 + col},
                    AtomKeyBuilder().start_index(range_start).end_index(range_end).build<KeyType::TABLE_DATA>("t")
            );
        }
        row_count += num_rows;
    }
    return ranges;
}

std::vector<TimestampRange> generate_time_ranges(size_t num_row_slices) {
    constexpr static timestamp ts_min = std::numeric_limits<timestamp>::min();
    constexpr static timestamp ts_max = std::numeric_limits<timestamp>::max();
    std::vector<TimestampRange> row_slice_time_ranges;
    row_slice_time_ranges.reserve(num_row_slices);
    timestamp slice_start = *rc::gen::inRange<timestamp>(ts_min + 1, ts_max - 1);
    constexpr static timestamp max_slice_length = 1000;
    // Limit the gap between two time ranges to give more chance of receiving overlapping slices which is the more
    // interesting case to test.
    constexpr static timestamp max_slice_gap = 5;
    for (size_t i = 0; i < num_row_slices; ++i) {
        const timestamp current_slice_max_end =
                ts_max - max_slice_length > slice_start ? slice_start + max_slice_length : ts_max - 1;
        const timestamp slice_end = *rc::gen::inRange<timestamp>(slice_start + 1, current_slice_max_end);
        row_slice_time_ranges.emplace_back(slice_start, slice_end);
        const timestamp current_gap_max =
                ts_max - max_slice_gap > slice_end - 1 ? slice_end - 1 + max_slice_gap : ts_max - 1;
        slice_start = *rc::gen::inRange<timestamp>(slice_end - 1, current_gap_max);
    }
    return row_slice_time_ranges;
}
} // namespace

RC_GTEST_PROP(StructureByTimeSlice, Rapidcheck, ()) {

    const size_t num_col_slices = *rc::gen::inRange<size_t>(1, 4);
    const size_t num_row_slices = *rc::gen::inRange<size_t>(1, 2000);
    std::vector<TimestampRange> row_slice_time_ranges = generate_time_ranges(num_row_slices);
    std::vector<RangesAndKey> ranges = generate_ranges_and_keys(row_slice_time_ranges, num_row_slices, num_col_slices);
    std::mt19937 generator(*rc::gen::arbitrary<size_t>());
    std::ranges::shuffle(ranges, generator);
    const std::vector<std::vector<size_t>> groups = structure_by_time_slice(ranges);

    const size_t num_entries = ranges.size();

    std::vector<TimestampRange> group_ranges;
    group_ranges.reserve(num_entries);
    std::vector<unsigned char> covered(num_entries, 0);
    for (const auto& [group_idx, group] : folly::enumerate(groups)) {
        RC_ASSERT(!group.empty());
        TimestampRange acc{std::numeric_limits<timestamp>::max(), std::numeric_limits<timestamp>::min()};
        for (const auto [group_element_idx, group_element] : folly::enumerate(group)) {
            RC_ASSERT(group_element < num_entries);
            RC_ASSERT(++covered[group_element] <= 2);
            const TimestampRange& range = ranges[group_element].key_.time_range();
            // Check it's a valid time range
            RC_ASSERT(range.first < range.second);
            if (group_element_idx > 0) {
                // Checks that all time ranges in a group are ordered in non-decreasing fashion
                RC_ASSERT(
                        ranges[group[group_element_idx - 1]].key_.time_range() == range ||
                        ranges[group[group_element_idx - 1]].key_.time_range().second - 1 <= range.first
                );
            }
            acc = TimestampRange{std::min(acc.first, range.first), std::max(acc.second, range.second)};
        }
        if (!group_ranges.empty()) {
            // Ensure the group time ranges are in non-decreasing order
            RC_ASSERT(
                    group_ranges.back() == acc ||
                    (group_ranges.back().first < acc.first && group_ranges.back().second <= acc.second) ||
                    (group_ranges.back().first <= acc.first && group_ranges.back().second < acc.second)
            );
        }

        group_ranges.emplace_back(acc);
    }
    RC_ASSERT(std::ranges::all_of(covered, [](const unsigned char c) { return c > 0; }));

    for (const auto& [group_idx, group] : folly::enumerate(groups)) {
        for (const auto [group_element_idx, group_element] : folly::enumerate(group)) {
            const TimestampRange& current_range = ranges[group_element].key_.time_range();
            auto intersects_with_current_range = [&](const size_t idx) {
                return atom_key_range_intersects(current_range, ranges[idx].key_.time_range());
            };
            // All time ranges in a group must have at least one timestamp in common
            RC_ASSERT(
                    group_element_idx >= group.size() - 1 ||
                    std::all_of(group.begin() + group_element_idx, group.end(), intersects_with_current_range)
            );

            // A group can intersect with at most two other groups. The next group and the one after that.
            // Time ranges [0;1) [0;2) [1;3) [2;3) will be grouped: [[0, 1], [1, 2], [2, 3]]
            // [0;1) will intersect with all elements in group 1 and the first row slice of group 2
            if (group_idx + 1 < groups.size()) {
                const auto& next_group = groups[group_idx + 1];
                size_t check_for_group_intersection_after = group_idx + 1;
                if (intersects_with_current_range(next_group.front())) {
                    // In case of a chain both groups will contain the same row slice
                    const auto last_row_slice_of_current_group = std::span{group}.last(num_col_slices);
                    const auto first_row_slice_of_next_group = std::span{next_group}.first(num_col_slices);
                    RC_ASSERT(std::ranges::equal(last_row_slice_of_current_group, first_row_slice_of_next_group));
                    if (group_element_idx < group.size() - num_col_slices) {
                        // This is not the last row slice in the group. Only the last row slices will have intersection
                        // with all elements of the next group.
                        RC_ASSERT(std::ranges::all_of(first_row_slice_of_next_group, intersects_with_current_range));
                        RC_ASSERT(
                                next_group.size() == num_col_slices ||
                                std::ranges::none_of(
                                        std::span{next_group}.subspan(num_col_slices), intersects_with_current_range
                                )
                        );
                    } else {
                        RC_ASSERT(std::ranges::all_of(next_group, intersects_with_current_range));
                        if (group_idx + 2 < groups.size() &&
                            atom_key_range_intersects(
                                    ranges[next_group.back()].key_.time_range(),
                                    ranges[groups[group_idx + 2].front()].key_.time_range()
                            )) {
                            const auto& subsequent_group = groups[group_idx + 2];
                            // Time ranges [0;1) [0;2) [1;3) [2;3) will be grouped: [[0, 1], [1, 2], [2, 3]]
                            // [0;1) will intersect with all elements in group 1 and the first row slice of group 2
                            ++check_for_group_intersection_after;
                            RC_ASSERT(std::ranges::all_of(
                                    std::span{subsequent_group}.first(num_col_slices), intersects_with_current_range
                            ));
                            RC_ASSERT(std::ranges::none_of(
                                    std::span{subsequent_group}.subspan(num_col_slices), intersects_with_current_range
                            ));
                        }
                    }
                    ++check_for_group_intersection_after;
                }
                RC_ASSERT(
                        (check_for_group_intersection_after >= groups.size() ||
                         std::none_of(
                                 group_ranges.begin() + check_for_group_intersection_after,
                                 group_ranges.end(),
                                 [&](const TimestampRange& group_range) {
                                     return atom_key_range_intersects(current_range, group_range);
                                 }
                         ))
                );
            }
        }
    }
}
