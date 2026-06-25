/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <vector>
#include <unordered_set>
#include <arcticdb/entity/descriptors.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

namespace arcticdb {

namespace entity {
struct OutputSchema;
}

namespace pipelines {
struct InputFrame;
} // namespace pipelines

/**
 * The new frame for append/update is compatible with the existing index. Throws various exceptions if not.
 */
void fix_normalization_or_throw(
        bool is_append, const TimeseriesDescriptor& existing_tsd, const pipelines::InputFrame& new_frame,
        bool dynamic_schema
);

proto::descriptors::NormalizationMetadata generate_norm_meta(
        const std::vector<entity::OutputSchema>& input_schemas, std::unordered_set<size_t>&& non_matching_name_indices
);

void accumulate_norm_metadata_column_names(
        proto::descriptors::NormalizationMetadata& accumulated,
        const proto::descriptors::NormalizationMetadata& new_entry
);
} // namespace arcticdb
