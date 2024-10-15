/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause_utils.hpp>

namespace arcticdb {

using namespace pipelines;

std::vector<std::vector<EntityId>> structure_by_row_slice(ComponentManager& component_manager, std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
    auto [row_ranges, col_ranges] = component_manager.get_entities<std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(entity_ids, false);
    std::vector<RangesAndEntity> ranges_and_entities;
    ranges_and_entities.reserve(entity_ids.size());
    for (size_t idx=0; idx<entity_ids.size(); ++idx) {
        ranges_and_entities.emplace_back(entity_ids[idx], row_ranges[idx], col_ranges[idx]);
    }
    auto new_structure_indices = structure_by_row_slice(ranges_and_entities);
    std::vector<std::vector<EntityId>> res(new_structure_indices.size());
    for (const auto&& [outer_idx, vec]: folly::enumerate(new_structure_indices)) {
        res[outer_idx].reserve(vec.size());
        for (auto inner_idx: vec) {
            res[outer_idx].emplace_back(ranges_and_entities[inner_idx].id_);
        }
    }
    return res;
}

std::vector<std::vector<EntityId>> offsets_to_entity_ids(const std::vector<std::vector<size_t>>& offsets,
                                                         const std::vector<RangesAndEntity>& ranges_and_entities) {
    std::vector<std::vector<EntityId>> res(offsets.size());
    for (const auto&& [outer_idx, vec]: folly::enumerate(offsets)) {
        res[outer_idx].reserve(vec.size());
        for (auto inner_idx: vec) {
            res[outer_idx].emplace_back(ranges_and_entities[inner_idx].id_);
        }
    }
    return res;
}

/*
 * On exit from a clause, we need to push the elements of the newly created processing unit's into the component
 * manager. These will either be used by the next clause in the pipeline, or to present the output dataframe back to
 * the user if this is the final clause in the pipeline.
 */
std::vector<EntityId> push_entities(ComponentManager& component_manager, ProcessingUnit&& proc, EntityFetchCount entity_fetch_count) {
    std::vector<EntityFetchCount> entity_fetch_counts(proc.segments_->size(), entity_fetch_count);
    std::vector<EntityId> ids;
    if (proc.bucket_.has_value()) {
        std::vector<bucket_id> bucket_ids(proc.segments_->size(), *proc.bucket_);
        ids = component_manager.add_entities(
                std::move(*proc.segments_),
                std::move(*proc.row_ranges_),
                std::move(*proc.col_ranges_),
                std::move(entity_fetch_counts),
                std::move(bucket_ids));
    } else {
        ids = component_manager.add_entities(
                std::move(*proc.segments_),
                std::move(*proc.row_ranges_),
                std::move(*proc.col_ranges_),
                std::move(entity_fetch_counts));
    }
    return ids;
}

std::vector<EntityId> flatten_entities(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    size_t res_size = std::accumulate(entity_ids_vec.cbegin(),
                                      entity_ids_vec.cend(),
                                      size_t(0),
                                      [](size_t acc, const std::vector<EntityId>& vec) { return acc + vec.size(); });
    std::vector<EntityId> res;
    res.reserve(res_size);
    for (const auto& entity_ids: entity_ids_vec) {
        res.insert(res.end(), entity_ids.begin(), entity_ids.end());
    }
    return res;
}

std::vector<folly::FutureSplitter<pipelines::SegmentAndSlice>> split_futures(
        std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures) {
    std::vector<folly::FutureSplitter<pipelines::SegmentAndSlice>> res;
    res.reserve(segment_and_slice_futures.size());
    for (auto&& future: segment_and_slice_futures) {
        res.emplace_back(folly::splitFuture(std::move(future)));
    }
    return res;
}

std::shared_ptr<std::vector<EntityFetchCount>> generate_segment_fetch_counts(
        const std::vector<std::vector<size_t>>& processing_unit_indexes,
        size_t num_segments) {
    auto res = std::make_shared<std::vector<EntityFetchCount>>(num_segments, 0);
    for (const auto& list: processing_unit_indexes) {
        for (auto idx: list) {
            res->at(idx)++;
        }
    }
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(
            std::all_of(res->begin(), res->end(), [](const size_t& val) { return val != 0; }),
            "All segments should be needed by at least one ProcessingUnit");
    return res;
}

template<ResampleBoundary closed_boundary, typename T>
requires std::is_same_v<T, RangesAndKey> || std::is_same_v<T, RangesAndEntity>
std::vector<std::vector<size_t>> structure_by_time_bucket(
    std::vector<T>& ranges,
    const std::vector<timestamp>& bucket_boundaries) {
    std::erase_if(ranges, [&bucket_boundaries](const T &range) {
        auto start_index = range.start_time();
        auto end_index = range.end_time();
        return index_range_outside_bucket_range<closed_boundary>(start_index, end_index, bucket_boundaries);
    });
    auto res = structure_by_row_slice(ranges);
    // Element i of res also needs the values from element i+1 if there is a bucket which incorporates the last index
    // value of row-slice i and the first value of row-slice i+1
    // Element i+1 should be removed if the last bucket involved in element i covers all the index values in element i+1
    auto bucket_boundaries_it = std::cbegin(bucket_boundaries);
    // Exit if res_it == std::prev(res.end()) as this implies the last row slice was not incorporated into an earlier processing unit
    for (auto res_it = res.begin(); res_it != res.end() && res_it != std::prev(res.end());) {
        auto last_index_value_in_row_slice = ranges[res_it->at(0)].end_time();
        advance_boundary_past_value<closed_boundary>(bucket_boundaries, bucket_boundaries_it, last_index_value_in_row_slice);
        // bucket_boundaries_it now contains the end value of the last bucket covering the row-slice in res_it, or an end iterator if the last bucket ends before the end of this row-slice
        if (bucket_boundaries_it != bucket_boundaries.end()) {
            Bucket<closed_boundary> current_bucket{ *std::prev(bucket_boundaries_it), *bucket_boundaries_it };
            auto next_row_slice_it = std::next(res_it);
            while (next_row_slice_it != res.end()) {
                // end_index from the key is 1 nanosecond larger than the index value of the last row in the row-slice
                TimestampRange next_row_slice_timestamp_range{
                        ranges[next_row_slice_it->at(0)].start_time(),
                        ranges[next_row_slice_it->at(0)].end_time() };
                if (current_bucket.contains(next_row_slice_timestamp_range.first)) {
                    // The last bucket in the current processing unit overlaps with the first index value in the next row slice, so add segments into current processing unit
                    res_it->insert(res_it->end(), next_row_slice_it->begin(), next_row_slice_it->end());
                    if (current_bucket.contains(next_row_slice_timestamp_range.second)) {
                        // The last bucket in the current processing unit wholly contains the next row slice, so remove it from the result
                        next_row_slice_it = res.erase(next_row_slice_it);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            // This is the last bucket, and all the required row-slices have been incorporated into the current processing unit, so erase the rest
            if (bucket_boundaries_it == std::prev(bucket_boundaries.end())) {
                res.erase(next_row_slice_it, res.end());
                break;
            }
            res_it = next_row_slice_it;
        }
    }
    return res;
}

template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::LEFT, RangesAndKey>(
    std::vector<RangesAndKey>& ranges,
    const std::vector<timestamp>& bucket_boundaries);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::RIGHT, RangesAndKey>(
    std::vector<RangesAndKey>& ranges,
    const std::vector<timestamp>& bucket_boundaries);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::LEFT, RangesAndEntity>(
    std::vector<RangesAndEntity>& ranges,
    const std::vector<timestamp>& bucket_boundaries);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::RIGHT, RangesAndEntity>(
    std::vector<RangesAndEntity>& ranges,
    const std::vector<timestamp>& bucket_boundaries);

}
