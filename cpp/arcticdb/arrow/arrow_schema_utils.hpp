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

namespace arcticdb {

// TODO: Better name
struct ArrowTransformedSchema {
    bool changed_;
    OutputSchema schema_;
    ankerl::unordered_dense::map<std::string, std::string> column_renames_;
};

ArrowTransformedSchema make_schema_arrow_compatible(
        const OutputSchema& input_schema, const std::optional<std::vector<std::string>>& index_columns = std::nullopt
);

} // namespace arcticdb