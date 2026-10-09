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

#include <optional>

namespace arcticdb {

bool is_pandas_convertible_to_arrow(const proto::descriptors::NormalizationMetadata& norm_meta);

void embed_pandas(
        const proto::descriptors::NormalizationMetadata& pandas,
        proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow
);

std::optional<proto::descriptors::NormalizationMetadata> embedded_pandas(
        const proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow
);

/// Constructs pandas metadata for Arrow data, using `pandas_template` for shape information Arrow does not carry.
proto::descriptors::NormalizationMetadata derive_pandas_from_arrow(
        const proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow, const entity::StreamDescriptor& desc,
        const proto::descriptors::NormalizationMetadata& pandas_template
);

/// Raises unless a Series' name and each index level's timezone agree between Arrow and its embedded pandas metadata.
void check_embedded_pandas_agrees(
        const proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow, const entity::StreamDescriptor& desc
);

/// The descriptor supplies what only it knows: whether the index is the symbol's timeseries index, and the name of the
/// column holding each index level.
proto::descriptors::NormalizationMetadata arrow_norm_from_pandas(
        const proto::descriptors::NormalizationMetadata& norm_meta, const entity::StreamDescriptor& desc
);

} // namespace arcticdb
