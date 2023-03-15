/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <unordered_map>
#include <map>

namespace arcticdb::pipelines {

struct ContextData {
    size_t context_index_;
    size_t column_index_;
};

struct FrameSliceMap {
    robin_hood::unordered_flat_map<std::string_view, std::map<RowRange, ContextData>> columns_;
    std::shared_ptr<PipelineContext> context_;

    FrameSliceMap(const std::shared_ptr<PipelineContext>& context, bool dynamic_schema) :
        context_(std::move(context)) {

        for (const auto &context_row: *context_) {
            const auto& row_range = context_row.slice_and_key().slice_.row_range;

            const auto& fields = context_row.descriptor().fields();
            for(const auto& field : folly::enumerate(fields)) {
                if (!context_->is_in_filter_columns_set(field->name())) {
                    ARCTICDB_DEBUG(log::version(), "{} not present in filtered columns, skipping", field->name());
                    continue;
                }

                if(!dynamic_schema && !is_sequence_type(type_desc_from_proto(field->type_desc()).data_type())) {
                    ARCTICDB_DEBUG(log::version(), "{} not a string type in dynamic schema, skipping", field->name());
                    continue;
                }

                auto& column = columns_[field->name()];

                ContextData data{context_row.index_, field.index};
                column.insert(std::make_pair(row_range, data));
            }
        }
    }
};

} //namespace arcticdb::pipelines