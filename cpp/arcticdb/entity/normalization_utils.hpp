/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/descriptors.hpp>

namespace arcticdb {

/// DataFrames, Series and TimeFrames all describe their index through the same Pandas submessage, as does Arrow data
/// that was combined with one of those; every other input type - an ndarray, a pickled object, Arrow data of its own -
/// has none, and returns nullptr. An input type this build does not know about is reached by reflection, so that data
/// written by a newer client is still read correctly.
const proto::descriptors::NormalizationMetadata_Pandas* pandas_common(
        const proto::descriptors::NormalizationMetadata& norm_meta
);

proto::descriptors::NormalizationMetadata_Pandas* mutable_pandas_common(
        proto::descriptors::NormalizationMetadata& norm_meta
);

/// The pandas metadata Arrow data carries once it has been combined with pandas data. Nullptr for Arrow-only data, and
/// for input types that are pandas in their own right - use pandas_common for those.
const proto::descriptors::NormalizationMetadata_Pandas* embedded_pandas_common(
        const proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow_meta
);

proto::descriptors::NormalizationMetadata_Pandas* mutable_embedded_pandas_common(
        proto::descriptors::NormalizationMetadata_ExperimentalArrow& arrow_meta
);

/// True for the input types that are pandas objects in their own right: a DataFrame, a Series or a TimeFrame. False for
/// Arrow data that merely carries pandas metadata, whose shape is described by the Arrow fields.
bool is_pandas_input_type(const proto::descriptors::NormalizationMetadata& norm_meta);

/// In case both indexes are row-ranged sanity checks will be performed:
/// * Both indexes must have the same step
/// * The new index must start at the point where the old one ends
/// If the checks above pass update the new normalization index so that it spans the whole index (old + new).
/// A no-op for input types that have no pandas index, such as an ndarray or a pickled object.
/// @throws In case the row-ranged indexes are incompatible
void update_rowrange_norm_for_append(
        const proto::descriptors::NormalizationMetadata& old_norm, proto::descriptors::NormalizationMetadata& new_norm,
        size_t old_length
);
} // namespace arcticdb
