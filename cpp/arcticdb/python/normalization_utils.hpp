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

namespace arcticdb {

namespace proto {
namespace descriptors = arcticc::pb2::descriptors_pb2;
} // namespace proto

namespace entity {
struct OutputSchema;
}

namespace pipelines {
struct InputFrame;
namespace index {
struct IndexSegmentReader;
} // namespace index
} // namespace pipelines

/**
 * The new frame for append/update is compatible with the existing index. Throws various exceptions if not.
 */
void fix_normalization_or_throw(
        bool is_append, const pipelines::index::IndexSegmentReader& existing_isr, const pipelines::InputFrame& new_frame
);

proto::descriptors::NormalizationMetadata generate_norm_meta(
        const std::vector<entity::OutputSchema>& input_schemas, std::unordered_set<size_t>&& non_matching_name_indices
);
} // namespace arcticdb
