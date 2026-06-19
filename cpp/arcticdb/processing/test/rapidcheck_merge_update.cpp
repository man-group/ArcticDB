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

RC_GTEST_PROP(StructureByTimeSlice, Rapidcheck, ()) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    constexpr static timestamp ts_min = std::numeric_limits<timestamp>::min();
    constexpr static timestamp ts_max = std::numeric_limits<timestamp>::max();

    // Number of column slices per row slice, between 1 and 5.
    const int64_t num_col_slices = *rc::gen::inRange<int64_t>(1, 6);
    const size_t num_row_slices = *rc::gen::inRange<size_t>(1, 1000);

    std::vector<TimestampRange> row_slice_time_ranges;
    row_slice_time_ranges.reserve(num_row_slices);
    timestamp slice_start = *rc::gen::inRange<timestamp>(ts_min + 1, ts_max - 1);
    for (size_t i = 0; i < num_row_slices; ++i) {
        const timestamp slice_end = *rc::gen::inRange<timestamp>(slice_start + 1, ts_max);
        row_slice_time_ranges.emplace_back(slice_start, slice_end);
        slice_start = *rc::gen::inRange<timestamp>(slice_end - 1, ts_max - 1);
    }

    std::vector<RangesAndKey> ranges;
    ranges.reserve(num_row_slices * num_col_slices);
    size_t row_count{};
    for (size_t row_slice = 0; row_slice < num_row_slices; ++row_slice) {
        const auto& [range_start, range_end] = row_slice_time_ranges[row_slice];
        const size_t range_size = range_end - range_start;
        const size_t num_rows = *rc::gen::inRange<size_t>(1, range_size + 1);
        for (size_t col = 0; col < static_cast<size_t>(num_col_slices); ++col) {
            ranges.emplace_back(
                    RowRange{row_count, row_count + num_rows},
                    ColRange{1 + col, 2 + col},
                    AtomKeyBuilder().start_index(range_start).end_index(range_end).build<KeyType::TABLE_DATA>("t")
            );
        }
        row_count += num_rows;
    }

    static std::random_device rd;
    std::mt19937 generator(rd());
    std::ranges::shuffle(ranges, generator);

    const std::vector<std::vector<size_t>> groups = structure_by_time_slice(ranges);

    const size_t num_entries = ranges.size();

    util::BitSet covered(num_entries);
    for (const std::vector<size_t>& group : groups) {
        RC_ASSERT(!group.empty());
        for (const size_t idx : group) {
            RC_ASSERT(idx < num_entries);
            covered[idx] = true;
        }
    }
    RC_ASSERT(covered.count() == covered.size());

    for (auto current_group = groups.begin(); current_group != groups.end(); ++current_group) {
        for (auto range_index = current_group->begin(); range_index != current_group->end(); ++range_index) {
            const TimestampRange& current_time_range = ranges[*range_index].key_.time_range();
            auto intersects_with_current_range = [&](const size_t idx) {
                const TimestampRange& range = ranges[idx].key_.time_range();
                return current_time_range.first < range.second && range.first < current_time_range.second;
            };
            RC_ASSERT(
                    current_group->end() - range_index > 1 ||
                    std::all_of(range_index + 1, current_group->end(), intersects_with_current_range)
            );
            if (groups.end() - current_group > 1) {
                const auto next_group_it = current_group + 1;
                if (intersects_with_current_range(next_group_it->front())) {
                    const auto last_row_slice_of_current_group = std::span{*current_group}.last(num_col_slices);
                    const auto first_row_slice_of_next_group = std::span{*next_group_it}.first(num_col_slices);
                    RC_ASSERT(std::ranges::equal(last_row_slice_of_current_group, first_row_slice_of_next_group));
                    if (std::distance(range_index, current_group->end()) > num_col_slices) {
                        RC_ASSERT(std::ranges::all_of(first_row_slice_of_next_group, intersects_with_current_range));
                        RC_ASSERT(std::ranges::none_of(
                                next_group_it->begin() + num_col_slices,
                                next_group_it->end(),
                                intersects_with_current_range
                        ));
                    } else {
                        RC_ASSERT(std::ranges::all_of(*next_group_it, intersects_with_current_range));
                    }
                } else {
                    RC_ASSERT(std::ranges::none_of(*next_group_it, intersects_with_current_range));
                }
            }
            RC_ASSERT(
                    groups.end() - current_group > 2 ||
                    std::all_of(
                            current_group + 2,
                            groups.end(),
                            [&](const auto& group) {
                                return std::ranges::none_of(group, intersects_with_current_range);
                            }
                    )
            );
        }
    }
}
