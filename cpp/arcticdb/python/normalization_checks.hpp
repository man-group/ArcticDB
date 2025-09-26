/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/error_code.hpp>
#include <arcticdb/version/schema_checks.hpp>

namespace arcticdb {

using NormalizationException = ArcticCategorizedException<ErrorCategory::NORMALIZATION>;

namespace pipelines {
struct InputTensorFrame;
namespace index {
struct IndexSegmentReader;
} // namespace index
} // namespace pipelines

/**
 * The new frame for append/update is compatible with the existing index. Throws various exceptions if not.
 */
void fix_normalization_or_throw(
        NormalizationOperation op, const pipelines::index::IndexSegmentReader& existing_isr,
        const pipelines::InputTensorFrame& new_frame
);
} // namespace arcticdb
