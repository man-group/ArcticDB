/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/normalization_utils.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

namespace arcticdb {

/// The pandas input types the Arrow metadata's own oneof mirrors, and so the ones that can be described in Arrow terms.
bool is_embeddable_pandas(const proto::descriptors::NormalizationMetadata& norm_meta);

/// Copy a pandas normalization metadata into the arm of the Arrow metadata's oneof that mirrors its input type.
void embed_pandas(
        const proto::descriptors::NormalizationMetadata& pandas,
        proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow
);

/// A pandas schema described in Arrow terms: the pandas message embedded whole, plus what Arrow records separately -
/// whether the first column is the index, whether the object is one-dimensional, a Series' name, and a column metadata
/// entry per timestamp column carrying its timezone.
///
/// Arrow output is generated from Arrow metadata, and combining Arrow with pandas is combining two Arrow metadatas one
/// of which has pandas metadata embedded in it, so both paths convert first and then have a single shape to handle.
proto::descriptors::NormalizationMetadata arrow_norm_from_pandas(
        const proto::descriptors::NormalizationMetadata& norm_meta, const entity::StreamDescriptor& desc
);

} // namespace arcticdb
