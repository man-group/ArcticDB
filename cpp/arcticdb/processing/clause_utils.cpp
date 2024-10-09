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

}
