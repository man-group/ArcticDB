/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/index_utils.hpp>

namespace arcticdb::pipelines {

struct ContextData {
    size_t context_index_;
    size_t column_index_;
};

struct FrameSliceMap {
    ankerl::unordered_dense::map<std::string_view, std::map<RowRange, ContextData>> columns_;
    std::shared_ptr<PipelineContext> context_;

    FrameSliceMap(std::shared_ptr<PipelineContext> context, bool dynamic_schema) : context_(std::move(context)) {
        const entity::StreamDescriptor& descriptor = context_->descriptor();
        const auto true_index_field_count = descriptor.index().field_count();
        const auto required_fields_count = static_cast<bool>(context_->norm_meta_)
                                                   ? index::required_fields_count(descriptor, *context_->norm_meta_)
                                                   : index::required_fields_count(descriptor);
        std::optional<size_t> min_col_index;
        for (const auto& context_row : *context_) {
            const auto& row_range = context_row.slice_and_key().slice_.row_range;
            const auto& col_range = context_row.slice_and_key().slice_.col_range;
            if (!min_col_index.has_value()) {
                min_col_index.emplace(col_range.first);
            }
            const size_t first_col = col_range.first;
            const bool first_col_slice = first_col == *min_col_index;

            const auto& fields = context_row.descriptor().fields();
            for (const auto& field : folly::enumerate(fields)) {
                if (!context_->is_in_filter_columns_set(field->name())) {
                    ARCTICDB_DEBUG(log::version(), "{} not present in filtered columns, skipping", field->name());
                    continue;
                }

                const entity::DataType row_range_type = field->type().data_type();
                if (!dynamic_schema && !is_sequence_type(row_range_type)) {
                    // In case we end up with static schema and empty we must check the type of the whole column
                    // Because we could be reading an empty segment of a string column. Example: start with [None],
                    // then append ["string"]. If we read with row range [0;1) we would end up reading the empty segment
                    // On read the empty type handler will fill the segment with not_a_string() and the reducer must
                    // run over them.
                    // TODO: This logic won't be needed when we move string handling into separate type handler
                    if (is_empty_type(row_range_type)) {
                        const size_t global_field_idx = descriptor.find_field(field->name()).value();
                        const Field& global_field = descriptor.field(global_field_idx);
                        const entity::DataType global_field_type = global_field.type().data_type();
                        if (!is_sequence_type(global_field_type)) {
                            ARCTICDB_DEBUG(
                                    log::version(), "{} not a string type in dynamic schema, skipping", field->name()
                            );
                            continue;
                        }
                    } else {
                        ARCTICDB_DEBUG(
                                log::version(), "{} not a string type in dynamic schema, skipping", field->name()
                        );
                        continue;
                    }
                }
                // If we are in the first col slice, then we just need to check if the field index is less than the
                // required fields count
                // Otherwise, we need to ignore true index fields (as they are grabbed from the first column slice), and
                // offset by first_col - true_index_field_count
                const bool required_field =
                        first_col_slice
                                ? field.index < required_fields_count
                                : true_index_field_count <= field.index &&
                                          required_fields_count >= first_col - true_index_field_count &&
                                          field.index < required_fields_count - (first_col - true_index_field_count);
                // If required_field is true, this is a required column in the output. The name in slice stream
                // descriptor may not match that in the global stream descriptor, so use the global name here
                // e.g. If 2 timeseries are joined that had differently named indexes
                // All other columns use names to match the source with the destination
                const auto& field_name =
                        required_field ? descriptor.field(field.index + (first_col_slice ? 0 : first_col)).name()
                                       : field->name();
                auto& column = columns_[field_name];
                column.emplace(row_range, ContextData{context_row.index_, field.index});
            }
        }
    }
};

} // namespace arcticdb::pipelines