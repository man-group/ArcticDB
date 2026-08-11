/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/collection_utils.hpp>
#include <arcticdb/python/normalization_utils.hpp>

namespace arcticdb {
namespace ranges = std::ranges;
using namespace pipelines;
using namespace proto::descriptors;

std::vector<std::vector<EntityId>> structure_by_row_slice(
        ComponentManager& component_manager, std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    auto entity_ids = util::flatten_vectors(std::move(entity_ids_vec));
    return structure_by_row_slice(component_manager, std::move(entity_ids));
}

std::vector<std::vector<EntityId>> structure_by_row_slice(
        ComponentManager& component_manager, std::vector<EntityId>&& entity_ids
) {
    auto [row_ranges, col_ranges] =
            component_manager.get_entities<std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(entity_ids);
    std::vector<RangesAndEntity> ranges_and_entities;
    ranges_and_entities.reserve(entity_ids.size());
    for (size_t idx = 0; idx < entity_ids.size(); ++idx) {
        ranges_and_entities.emplace_back(entity_ids[idx], row_ranges[idx], col_ranges[idx]);
    }
    auto new_structure_indices = structure_by_row_slice(ranges_and_entities);
    std::vector<std::vector<EntityId>> res(new_structure_indices.size());
    for (const auto&& [outer_idx, vec] : folly::enumerate(new_structure_indices)) {
        res[outer_idx].reserve(vec.size());
        for (auto inner_idx : vec) {
            res[outer_idx].emplace_back(ranges_and_entities[inner_idx].id_);
        }
    }
    return res;
}

template<typename T>
requires util::any_of<T, RangesAndKey, RangesAndEntity>
std::vector<std::vector<size_t>> structure_by_row_slice(std::vector<T>& ranges) {
    std::ranges::sort(ranges, [](const T& left, const T& right) {
        return std::tie(left.row_range().first, left.col_range().first) <
               std::tie(right.row_range().first, right.col_range().first);
    });

    std::vector<std::vector<size_t>> res;
    RowRange previous_row_range{std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max()};
    for (const auto& [idx, ranges_and_key] : folly::enumerate(ranges)) {
        RowRange current_row_range{ranges_and_key.row_range()};
        if (current_row_range != previous_row_range) {
            res.emplace_back();
        }
        res.back().emplace_back(idx);
        previous_row_range = current_row_range;
    }
    return res;
}

template std::vector<std::vector<size_t>> structure_by_row_slice(std::vector<RangesAndKey>& ranges);
template std::vector<std::vector<size_t>> structure_by_row_slice(std::vector<RangesAndEntity>& ranges);

std::vector<std::vector<size_t>> structure_by_time_slice(std::span<RangesAndKey> ranges) {
    std::ranges::sort(ranges, [](const RangesAndKey& left, const RangesAndKey& right) {
        return std::tie(left.row_range().first, left.col_range().first) <
               std::tie(right.row_range().first, right.col_range().first);
    });
    std::vector<std::vector<size_t>> res;
    size_t first_group_slice = 0;
    while (first_group_slice < ranges.size()) {
        const timestamp group_end_time = ranges[first_group_slice].key_.end_time();
        std::vector<size_t>& group = res.emplace_back();
        size_t next_group_start_first_slice = first_group_slice;
        for (size_t i = first_group_slice; i < ranges.size() && ranges[i].key_.start_time() < group_end_time; ++i) {
            group.emplace_back(i);
            if (next_group_start_first_slice == i && ranges[i].key_.end_time() == group_end_time) {
                ++next_group_start_first_slice;
            }
        }
        first_group_slice = next_group_start_first_slice;
    }
    return res;
}

std::vector<std::vector<EntityId>> offsets_to_entity_ids(
        const std::vector<std::vector<size_t>>& offsets, const std::vector<RangesAndEntity>& ranges_and_entities
) {
    std::vector<std::vector<EntityId>> res(offsets.size());
    for (const auto&& [outer_idx, vec] : folly::enumerate(offsets)) {
        res[outer_idx].reserve(vec.size());
        for (auto inner_idx : vec) {
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
std::vector<EntityId> push_entities(
        ComponentManager& component_manager, ProcessingUnit&& proc, EntityFetchCount entity_fetch_count
) {
    std::vector<EntityFetchCount> entity_fetch_counts(proc.segments_->size(), entity_fetch_count);
    std::vector<EntityId> ids;
    if (proc.bucket_.has_value()) {
        std::vector<bucket_id> bucket_ids(proc.segments_->size(), *proc.bucket_);
        ids = component_manager.add_entities(
                std::move(*proc.segments_),
                std::move(*proc.row_ranges_),
                std::move(*proc.col_ranges_),
                std::move(entity_fetch_counts),
                std::move(bucket_ids)
        );
    } else {
        ids = component_manager.add_entities(
                std::move(*proc.segments_),
                std::move(*proc.row_ranges_),
                std::move(*proc.col_ranges_),
                std::move(entity_fetch_counts)
        );
    }
    return ids;
}

using SegmentAndSlice = pipelines::SegmentAndSlice;

std::vector<FutureOrSplitter> split_futures(
        std::vector<folly::Future<SegmentAndSlice>>&& segment_and_slice_futures,
        std::vector<EntityFetchCount>& segment_fetch_counts
) {
    std::vector<FutureOrSplitter> res;
    res.reserve(segment_and_slice_futures.size());
    for (auto&& [index, future] : folly::enumerate(segment_and_slice_futures)) {
        if (segment_fetch_counts[index] > 1)
            res.emplace_back(folly::splitFuture(std::move(future)));
        else
            res.emplace_back(std::move(future));
    }
    return res;
}

std::shared_ptr<std::vector<EntityFetchCount>> generate_segment_fetch_counts(
        const std::span<const std::vector<size_t>> processing_unit_indexes, const size_t num_segments
) {
    auto res = std::vector<EntityFetchCount>(num_segments, 0);
    for (const auto& list : processing_unit_indexes) {
        for (const auto idx : list) {
            res[idx]++;
        }
    }
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            ranges::none_of(res, [](const size_t val) { return val == 0; }),
            "All segments should be needed by at least one ProcessingUnit"
    );
    return std::make_shared<std::vector<EntityFetchCount>>(std::move(res));
}

template<ResampleBoundary closed_boundary, typename T>
requires std::is_same_v<T, RangesAndKey> || std::is_same_v<T, RangesAndEntity>
std::vector<std::vector<size_t>> structure_by_time_bucket(
        std::vector<T>& ranges, const std::vector<timestamp>& bucket_boundaries
) {
    std::erase_if(ranges, [&bucket_boundaries](const T& range) {
        auto start_index = range.start_time();
        auto end_index = range.end_time();
        return index_range_outside_bucket_range<closed_boundary>(start_index, end_index, bucket_boundaries);
    });
    auto res = structure_by_row_slice(ranges);
    // Element i of res also needs the values from element i+1 if there is a bucket which incorporates the last index
    // value of row-slice i and the first value of row-slice i+1
    // Element i+1 should be removed if the last bucket involved in element i covers all the index values in element i+1
    auto bucket_boundaries_it = std::cbegin(bucket_boundaries);
    // Exit if res_it == std::prev(res.end()) as this implies the last row slice was not incorporated into an earlier
    // processing unit
    for (auto res_it = res.begin(); res_it != res.end() && res_it != std::prev(res.end());) {
        auto last_index_value_in_row_slice = ranges[res_it->at(0)].end_time();
        advance_boundary_past_value<closed_boundary>(
                bucket_boundaries, bucket_boundaries_it, last_index_value_in_row_slice
        );
        // bucket_boundaries_it now contains the end value of the last bucket covering the row-slice in res_it, or an
        // end iterator if the last bucket ends before the end of this row-slice
        if (bucket_boundaries_it != bucket_boundaries.end()) {
            Bucket<closed_boundary> current_bucket{*std::prev(bucket_boundaries_it), *bucket_boundaries_it};
            auto next_row_slice_it = std::next(res_it);
            while (next_row_slice_it != res.end()) {
                // end_index from the key is 1 nanosecond larger than the index value of the last row in the row-slice
                TimestampRange next_row_slice_timestamp_range{
                        ranges[next_row_slice_it->at(0)].start_time(), ranges[next_row_slice_it->at(0)].end_time()
                };
                if (current_bucket.contains(next_row_slice_timestamp_range.first)) {
                    // The last bucket in the current processing unit overlaps with the first index value in the next
                    // row slice, so add segments into current processing unit
                    res_it->insert(res_it->end(), next_row_slice_it->begin(), next_row_slice_it->end());
                    if (current_bucket.contains(next_row_slice_timestamp_range.second)) {
                        // The last bucket in the current processing unit wholly contains the next row slice, so remove
                        // it from the result
                        next_row_slice_it = res.erase(next_row_slice_it);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            // This is the last bucket, and all the required row-slices have been incorporated into the current
            // processing unit, so erase the rest
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
        std::vector<RangesAndKey>& ranges, const std::vector<timestamp>& bucket_boundaries
);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::RIGHT, RangesAndKey>(
        std::vector<RangesAndKey>& ranges, const std::vector<timestamp>& bucket_boundaries
);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::LEFT, RangesAndEntity>(
        std::vector<RangesAndEntity>& ranges, const std::vector<timestamp>& bucket_boundaries
);
template std::vector<std::vector<size_t>> structure_by_time_bucket<ResampleBoundary::RIGHT, RangesAndEntity>(
        std::vector<RangesAndEntity>& ranges, const std::vector<timestamp>& bucket_boundaries
);

static auto first_missing_column(OutputSchema& output_schema, const std::unordered_set<std::string>& required_columns) {
    const auto& column_types = output_schema.column_types();
    for (auto input_column_it = required_columns.begin(); input_column_it != required_columns.end();
         ++input_column_it) {
        if (!column_types.contains(*input_column_it) &&
            !column_types.contains(stream::mangled_name(*input_column_it))) {
            return input_column_it;
        }
    }
    return required_columns.end();
}

void check_column_presence(
        OutputSchema& output_schema, const std::unordered_set<std::string>& required_columns,
        std::string_view clause_name
) {
    const auto first_missing = first_missing_column(output_schema, required_columns);
    schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
            first_missing == required_columns.end(),
            "{}Clause requires column '{}' to exist in input data",
            clause_name,
            first_missing == required_columns.end() ? "" : *first_missing
    );
}

void check_is_timeseries(const StreamDescriptor& stream_descriptor, std::string_view clause_name) {
    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            stream_descriptor.index().type() == IndexDescriptor::Type::TIMESTAMP &&
                    stream_descriptor.index().field_count() >= 1 &&
                    stream_descriptor.field(0).type() == make_scalar_type(DataType::NANOSECONDS_UTC64),
            "{}Clause can only be applied to timeseries",
            clause_name
    );
}

} // namespace arcticdb