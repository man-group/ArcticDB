/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <string>

#include <arcticdb/pipeline/filter_range.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

struct ExpressionContext;

// Apply row-level truncation to a decoded segment.
// Handles both timestamp-based (date_range) and row-based (row_range/LIMIT) truncation.
// The segment is modified in-place; rows outside the filter range are removed.
void apply_truncation(
        SegmentInMemory& segment, const pipelines::RowRange& slice_row_range, const FilterRange& row_filter
);

// Apply a FilterClause expression to a decoded segment.
// Returns true if the segment has rows remaining after filtering, false if empty.
// The segment is modified in-place; rows not matching the expression are removed.
bool apply_filter_clause(
        SegmentInMemory& segment, const std::shared_ptr<ExpressionContext>& expression_context,
        const std::string& filter_root_node_name
);

// Estimate the uncompressed size in bytes of a segment described by a SliceAndKey.
// Used by the dual-cap backpressure system to prevent OOM with wide tables.
size_t estimate_segment_bytes(const pipelines::SliceAndKey& sk, const StreamDescriptor& descriptor);

} // namespace arcticdb
