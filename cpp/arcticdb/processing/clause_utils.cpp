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

namespace arcticdb {
namespace ranges = std::ranges;
using namespace pipelines;
using namespace proto::descriptors;

std::vector<std::vector<EntityId>> structure_by_row_slice(
        ComponentManager& component_manager, std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
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

std::vector<EntityId> flatten_entities(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    size_t res_size = std::accumulate(
            entity_ids_vec.cbegin(),
            entity_ids_vec.cend(),
            size_t(0),
            [](size_t acc, const std::vector<EntityId>& vec) { return acc + vec.size(); }
    );
    std::vector<EntityId> res;
    res.reserve(res_size);
    for (const auto& entity_ids : entity_ids_vec) {
        res.insert(res.end(), entity_ids.begin(), entity_ids.end());
    }
    return res;
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
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(
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

std::pair<StreamDescriptor, NormalizationMetadata> join_indexes(std::vector<OutputSchema>& input_schemas) {
    StreamDescriptor stream_desc{StreamId{}, generate_index_descriptor(input_schemas)};
    // Returns a set of indices of index fields where not all the input schema field names matched, which is needed to
    // generate the output norm metadata
    auto non_matching_name_indices = add_index_fields(stream_desc, input_schemas);
    auto norm_meta = generate_norm_meta(input_schemas, std::move(non_matching_name_indices));
    return {std::move(stream_desc), std::move(norm_meta)};
}

IndexDescriptorImpl generate_index_descriptor(const std::vector<OutputSchema>& input_schemas) {
    // Ensure:
    //  - Type is the same
    //  - Field count is the same
    std::optional<IndexDescriptor::Type> index_type;
    std::optional<uint32_t> index_desc_field_count;
    for (const auto& schema : input_schemas) {
        const auto& index_desc = schema.stream_descriptor().index();
        if (!index_type.has_value()) {
            index_type = index_desc.type();
        } else {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    index_desc.type() == *index_type, "Mismatching IndexDescriptor in schema join"
            );
        }
        if (!index_desc_field_count.has_value()) {
            index_desc_field_count = index_desc.field_count();
        } else {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    index_desc.field_count() == *index_desc_field_count, "Mismatching IndexDescriptor in schema join"
            );
        }
    }
    return {*index_type, *index_desc_field_count};
}

std::unordered_set<size_t> add_index_fields(StreamDescriptor& stream_desc, std::vector<OutputSchema>& input_schemas) {
    std::unordered_set<size_t> non_matching_name_indices;
    // If the first schema is multiindexed then use this field count
    // It will be checked later if the other norm metas are compatible
    const auto& first_norm_meta = input_schemas.front().norm_metadata_;
    auto required_fields_count = index::required_fields_count(stream_desc, first_norm_meta);
    if (required_fields_count == 0) {
        return non_matching_name_indices;
    }
    // FieldCollection does not support renaming fields, so use a vector of FieldRef and then turn this into a
    // FieldCollection at the end
    std::vector<FieldRef> index_fields;
    bool first_schema{true};
    for (auto& schema : input_schemas) {
        const auto& fields = schema.stream_descriptor().fields();
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                fields.size() >= required_fields_count,
                "Expected at least {} fields for index, but received {}",
                required_fields_count,
                fields.size()
        );
        if (first_schema) {
            for (size_t idx = 0; idx < required_fields_count; ++idx) {
                const auto& field = fields.at(idx);
                index_fields.emplace_back(field.type(), field.name());
                // Index columns, and the first non-index column in the case of Series are always included, so remove
                // from the column types map so they are not considered in inner/outer join
                schema.column_types().erase(std::string(field.name()));
            }
            first_schema = false;
        } else {
            for (size_t idx = 0; idx < required_fields_count; ++idx) {
                const auto& field = fields.at(idx);
                auto& current_type = index_fields.at(idx).type_;
                auto opt_common_type = has_valid_common_type(current_type, field.type());
                if (opt_common_type.has_value()) {
                    current_type = *opt_common_type;
                } else {
                    schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                            "No common type between {} and {} when joining schemas", current_type, field.type()
                    );
                }
                // Index columns, and the first non-index column in the case of Series are always included, so remove
                // from the column types map so they are not considered in inner/outer join
                schema.column_types().erase(std::string(field.name()));
                const auto& current_name = index_fields.at(idx).name_;
                if (current_name != field.name()) {
                    non_matching_name_indices.emplace(idx);
                }
            }
        }
    }
    for (size_t idx = 0; idx < index_fields.size(); ++idx) {
        if (non_matching_name_indices.contains(idx)) {
            // This is the same naming scheme used in _normalization.py for unnamed multiindex levels. Ensures that any
            // subsequent processing that checks for columns of this format will continue to work
            stream_desc.fields().add_field(
                    index_fields.at(idx).type(), idx == 0 ? "index" : fmt::format("__fkidx__{}", idx)
            );
        } else {
            stream_desc.add_field(index_fields.at(idx));
        }
    }
    return non_matching_name_indices;
}

NormalizationMetadata generate_norm_meta(
        const std::vector<OutputSchema>& input_schemas, std::unordered_set<size_t>&& non_matching_name_indices
) {
    // Ensure:
    // All are Series or all are DataFrames
    // All have PandasIndex OR PandasMultiIndex
    // If PandasIndex:
    //  - name/is_int/fake_name - if all the same maintain, otherwise "index"/false/true
    //  - tz - if all the same maintain, otherwise empty string
    //  - is_physically stored must all be the same
    //  - RangeIndex
    //    - start==0/step==1 - maintain
    //    - All steps the same, use start from first schema and maintain step
    //    - Otherwise, log warning, set start==0/step==1
    // If PandasMultiIndex:
    //  - name/is_int - if all the same maintain, otherwise "index"/false
    //  - field_count must all be same
    //  - tz - if all the same maintain, otherwise empty string
    //  - fake_field_pos - unioned together, along with non_matching_name_indices
    //      fake_field_pos.contains(0) serves same purpose as fake_name flag on PandasIndex
    //  - timezone - Like tz, for a given key all values must be same, otherwise set value to empty string
    util::check(!input_schemas.empty(), "Cannot join empty list of schemas");
    auto res = input_schemas.front().norm_metadata_;
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            res.has_series() || res.has_df(), "Multi-symbol joins only supported with Series and DataFrames"
    );
    auto* res_common = res.has_series() ? res.mutable_series()->mutable_common() : res.mutable_df()->mutable_common();
    if (res_common->has_multi_index()) {
        for (auto pos : res_common->multi_index().fake_field_pos()) {
            non_matching_name_indices.insert(pos);
        }
    }
    for (auto it = std::next(input_schemas.cbegin()); it != input_schemas.cend(); ++it) {
        const auto& input_schema = *it;
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                input_schema.norm_metadata_.has_series() || input_schema.norm_metadata_.has_df(),
                "Multi-symbol joins only supported with Series and DataFrames"
        );
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                res.has_series() == input_schema.norm_metadata_.has_series(),
                "Multi-symbol joins cannot join a Series to a DataFrame"
        );
        const auto& common = res.has_series() ? input_schema.norm_metadata_.series().common()
                                              : input_schema.norm_metadata_.df().common();
        if (res.has_series()) {
            if (res_common->name() != common.name() || !common.has_name() || !res_common->has_name()) {
                res_common->set_name("");
                res_common->set_has_name(false);
            }
        }
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                common.has_multi_index() == res_common->has_multi_index(), "Mismatching norm metadata in schema join"
        );
        if (res_common->has_multi_index()) {
            auto* res_index = res_common->mutable_multi_index();
            const auto& index = common.multi_index();
            if ((index.name() != res_index->name()) || (index.is_int() != res_index->is_int())) {
                res_index->clear_name();
                res_index->set_is_int(false);
            }
            if (index.tz() != res_index->tz()) {
                res_index->clear_tz();
            }
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    index.field_count() == res_index->field_count(), "Mismatching norm metadata in schema join"
            );
            for (const auto& [idx, idx_timezone] : index.timezone()) {
                (*res_index->mutable_timezone())[idx] =
                        (*res_index->mutable_timezone())[idx] == idx_timezone ? idx_timezone : "";
            }
            for (auto pos : index.fake_field_pos()) {
                // Do not modify the result fake_field_pos directly as it would likely result in many duplicate values
                // Track in this set and then just insert them all into the result at the end
                non_matching_name_indices.insert(pos);
            }
        } else {
            auto* res_index = res_common->mutable_index();
            const auto& index = common.index();
            if ((index.name() != res_index->name()) || (index.is_int() != res_index->is_int()) || index.fake_name() ||
                res_index->fake_name()) {
                res_index->set_name("index");
                res_index->set_is_int(false);
                res_index->set_fake_name(true);
            }
            if (index.tz() != res_index->tz()) {
                res_index->clear_tz();
            }
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    index.is_physically_stored() == res_index->is_physically_stored(),
                    "Mismatching norm metadata in schema join"
            );
            if (index.step() != res_index->step()) {
                log::version().warn("Mismatching RangeIndexes being combined, setting to start=0, step=1");
                res_index->set_start(0);
                res_index->set_step(1);
            }
        }
    }
    if (res_common->has_multi_index()) {
        auto* index = res_common->mutable_multi_index();
        index->clear_fake_field_pos();
        for (auto idx : non_matching_name_indices) {
            index->add_fake_field_pos(idx);
        }
        if (non_matching_name_indices.contains(0)) {
            index->set_name("index");
        }
    }
    return res;
}

void inner_join(StreamDescriptor& stream_desc, std::vector<OutputSchema>& input_schemas) {
    if (input_schemas.empty()) {
        return;
    }
    // Value must be optional to handle the case covered in MatchingNamesIncompatibleTypesOnUnusedColumns test in
    // test_join_schemas.cpp
    // i.e. There may be a column where the types between two input schemas to not match, but it doesn't matter, as the
    // column is missing from another schema
    // Cannot use ankerl::unordered_dense as iterators are not stable on erase
    std::unordered_map<std::string, std::optional<DataType>> columns_to_keep;
    bool first_element{true};
    for (auto& schema : input_schemas) {
        if (first_element) {
            // Start with the columns in the first element, and remove anything that isn't present in all other elements
            for (const auto& [name, data_type] : schema.column_types()) {
                columns_to_keep.emplace(name, data_type);
            }
            first_element = false;
        } else {
            // Iterate through the columns we are currently planning to keep
            for (auto columns_to_keep_it = columns_to_keep.begin(); columns_to_keep_it != columns_to_keep.end();) {
                const auto& column_name = columns_to_keep_it->first;
                if (auto it = schema.column_types().find(column_name); it != schema.column_types().end()) {
                    // Current set of columns under consideration contains column_name, so ensure types are compatible
                    // and if necessary modify the columns_to_keep value to a type capable of representing all
                    auto& current_data_type = columns_to_keep_it->second;
                    if (current_data_type.has_value()) {
                        auto opt_promotable_type =
                                promotable_type(make_scalar_type(*current_data_type), make_scalar_type(it->second));
                        if (opt_promotable_type.has_value()) {
                            current_data_type = opt_promotable_type->data_type();
                        } else {
                            current_data_type.reset();
                        }
                    }
                    ++columns_to_keep_it;
                } else {
                    columns_to_keep_it = columns_to_keep.erase(columns_to_keep_it);
                }
            }
        }
    }
    // All the columns we are retaining were in every schema. Just use the order from the first schema
    for (const auto& field : input_schemas.front().stream_descriptor().fields()) {
        std::string column_name(field.name());
        if (auto it = columns_to_keep.find(column_name); it != columns_to_keep.end()) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    it->second.has_value(), "No common type for column {} when joining schemas", it->first
            );
            stream_desc.add_scalar_field(it->second.value(), column_name);
        }
    }
}

void outer_join(StreamDescriptor& stream_desc, std::vector<OutputSchema>& input_schemas) {
    // If the first schema is multiindexed then use this field count
    // It will be checked later if the other norm metas are compatible
    const auto& first_norm_meta = input_schemas.front().norm_metadata_;
    auto required_fields_count = index::required_fields_count(stream_desc, first_norm_meta);
    ankerl::unordered_dense::map<std::string, DataType> columns_to_keep;
    // Maintain the order that columns appeared in through the schemas
    std::vector<std::string> column_names_to_keep;
    bool first_element{true};
    for (auto& schema : input_schemas) {
        if (first_element) {
            // Start with the columns in the first element, and add in anything that is present in all other elements
            columns_to_keep = schema.column_types();
            for (size_t idx = required_fields_count; idx < schema.stream_descriptor().field_count(); ++idx) {
                column_names_to_keep.emplace_back(schema.stream_descriptor().field(idx).name());
            }
            first_element = false;
        } else {
            const auto& column_types = schema.column_types();
            // Iterate through the columns of this element
            // Have to use stream descriptor instead of column_types() to get the output order right
            for (const auto& field : schema.stream_descriptor().fields()) {
                std::string column_name(field.name());
                // column_types has had all index names erased
                if (auto it = column_types.find(column_name); it != column_types.end()) {
                    const auto& data_type = it->second;
                    if (auto columns_to_keep_it = columns_to_keep.find(column_name);
                        columns_to_keep_it != columns_to_keep.end()) {
                        // Current set of columns under consideration contains column_name, so ensure types are
                        // compatible and if necessary modify the columns_to_keep value to a type capable of
                        // representing all
                        auto& current_data_type = columns_to_keep_it->second;
                        auto opt_promotable_type =
                                promotable_type(make_scalar_type(current_data_type), make_scalar_type(data_type));
                        if (opt_promotable_type.has_value()) {
                            current_data_type = opt_promotable_type->data_type();
                        } else {
                            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                                    "No common type between {} and {} when joining schemas",
                                    current_data_type,
                                    data_type
                            );
                        }
                    } else {
                        // This column is new, add it in
                        auto [_, inserted] = columns_to_keep.emplace(column_name, data_type);
                        util::check(inserted, "Adding same column name to map twice in outer_join");
                        column_names_to_keep.emplace_back(std::move(column_name));
                    }
                }
            }
        }
    }
    for (const auto& column_name : column_names_to_keep) {
        stream_desc.add_scalar_field(columns_to_keep.at(column_name), column_name);
    }
}

} // namespace arcticdb