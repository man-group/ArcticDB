/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <algorithm>

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>

using namespace arcticdb;

auto generate_bucket_boundaries(std::vector<timestamp>&& bucket_boundaries) {
    return [bucket_boundaries = std::move(bucket_boundaries)](timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, ResampleOrigin) mutable {
        return bucket_boundaries;
    };
}

RC_GTEST_PROP(Resample, StructureForProcessing, ()) {
    StreamId sym{"sym"};
    const auto unsorted_index_values = *rc::gen::unique<std::vector<timestamp>>(rc::gen::arbitrary<timestamp>());
    RC_PRE(unsorted_index_values.size() > 0);
    const auto unsorted_bucket_boundaries = *rc::gen::unique<std::vector<timestamp>>(rc::gen::arbitrary<timestamp>());
    RC_PRE(unsorted_bucket_boundaries.size() > 1);
    const auto left_boundary_closed = *rc::gen::arbitrary<bool>();

    // Index values and bucket boundaries are sorted
    auto index_values = unsorted_index_values;
    std::sort(index_values.begin(), index_values.end());
    auto bucket_boundaries = unsorted_bucket_boundaries;
    std::sort(bucket_boundaries.begin(), bucket_boundaries.end());

    // Use a single column slice as the interesting behaviour to test is around row-slicing
    ColRange col_range{1, 2};

    // Only the start and end index values of each row-slice is used in structure_for_processing, so use 2 rows
    // per row-slice
    auto num_row_slices = index_values.size() / 2 + index_values.size() % 2;
    std::vector<RangesAndKey> sorted_ranges_and_keys;
    sorted_ranges_and_keys.reserve(num_row_slices);
    for (size_t idx = 0; idx < num_row_slices; idx++) {
        auto row_range_start = idx * 2;
        auto row_range_end = std::min((idx + 1) * 2, index_values.size());
        RowRange row_range{row_range_start, row_range_end};
        auto start_idx_value = index_values[row_range_start];
        auto end_idx_value = index_values[row_range_end - 1] + 1;
        auto key = AtomKeyBuilder().start_index(start_idx_value).end_index(end_idx_value).build<KeyType::TABLE_DATA>(sym);
        sorted_ranges_and_keys.emplace_back(row_range, col_range, key);
    }
    auto ranges_and_keys = sorted_ranges_and_keys;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(ranges_and_keys.begin(), ranges_and_keys.end(), gen);

    // Create vector of bucket boundary pairs, inclusive at both ends
    // bucket_id will be used to refer to the index in these vectors of a specific bucket
    std::vector<std::pair<timestamp, timestamp>> bucket_boundary_pairs;
    bucket_boundary_pairs.reserve(bucket_boundaries.size() - 1);
    for (size_t idx = 0; idx < bucket_boundaries.size() - 1; idx++) {
        if (left_boundary_closed) {
            bucket_boundary_pairs.emplace_back(bucket_boundaries[idx], bucket_boundaries[idx + 1] - 1);
        } else { // right_boundary_closed
            bucket_boundary_pairs.emplace_back(bucket_boundaries[idx] + 1, bucket_boundaries[idx + 1]);
        }
    }

    // Eliminate ranges that do not overlap with the buckets
    for (auto it = sorted_ranges_and_keys.begin(); it != sorted_ranges_and_keys.end();) {
        if (it->key_.start_time() > bucket_boundary_pairs.back().second ||
            it->key_.end_time() <= bucket_boundary_pairs.front().first) {
            it = sorted_ranges_and_keys.erase(it);
        } else {
            ++it;
        }
    }

    // Map from bucket_id to indexes in sorted_ranges_and_keys of row-slices needed for this bucket
    std::vector<std::vector<size_t>> bucket_to_row_range_map(bucket_boundary_pairs.size(), std::vector<size_t>());
    for (const auto& [bucket_id, bucket_boundary_pair]: folly::enumerate(bucket_boundary_pairs)) {
        for (const auto& [idx, range]: folly::enumerate(sorted_ranges_and_keys)) {
            if (range.key_.start_time() <= bucket_boundary_pair.second &&
                range.key_.end_time() > bucket_boundary_pair.first) {
                bucket_to_row_range_map[bucket_id].emplace_back(idx);
            }
        }
    }

    std::vector<std::vector<size_t>> expected_result;
    std::optional<size_t> current_range_idx;
    for (const auto& row_range_ids: bucket_to_row_range_map) {
        if (!row_range_ids.empty()) {
            if (current_range_idx.has_value() && row_range_ids.front() == *current_range_idx) {
                if (row_range_ids.front() != expected_result.back().front()) {
                    expected_result.emplace_back(row_range_ids);
                } else {
                    for (const auto &id: row_range_ids) {
                        if (id > expected_result.back().back()) {
                            expected_result.back().emplace_back(id);
                        }
                    }
                }
            } else {
                expected_result.emplace_back(row_range_ids);
                current_range_idx = row_range_ids.back();
            }
        }
    }

    if (left_boundary_closed) {
        ResampleClause<ResampleBoundary::LEFT> resample_clause{"dummy", ResampleBoundary::LEFT, generate_bucket_boundaries(std::move(bucket_boundaries)), 0, 0};
        auto result = resample_clause.structure_for_processing(ranges_and_keys);
        RC_ASSERT(expected_result == result);
    } else {
        ResampleClause<ResampleBoundary::RIGHT> resample_clause{"dummy", ResampleBoundary::RIGHT, generate_bucket_boundaries(std::move(bucket_boundaries)), 0, 0};
        auto result = resample_clause.structure_for_processing(ranges_and_keys);
        RC_ASSERT(expected_result == result);
    }
}
