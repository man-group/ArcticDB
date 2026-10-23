/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <ankerl/unordered_dense.h>

#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/util/string_utils.hpp>

namespace arcticdb {

class SegmentInMemory;

struct ArrowTransformedSchema {
    OutputSchema schema_;
    ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>
            column_renames_;
};

std::optional<ArrowTransformedSchema> make_schema_arrow_compatible(
        const OutputSchema& input_schema, const std::optional<std::vector<std::string>>& index_columns = std::nullopt
);

/// Renames the fields of pandas data read as Arrow to valid and unique Arrow column names, first converting pandas
/// `norm` to Arrow. Afterwards these parts of `norm` are keyed by the new names:
/// - Arrow's `columns`.
/// - The embedded pandas `col_names`, which records the original pandas label of every column, index levels included,
///   whose new name differs from it.
void prepare_for_arrow_output(StreamDescriptor& desc, proto::descriptors::NormalizationMetadata& norm);

void prepare_for_arrow_output(SegmentInMemory& segment, proto::descriptors::NormalizationMetadata& norm);

} // namespace arcticdb