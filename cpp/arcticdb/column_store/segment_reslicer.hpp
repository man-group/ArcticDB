/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <vector>

#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

// Given a maximum number of rows per slice, reslices a set of segments into a new shape, with at most
// max_rows_per_slice_ rows in each one. The input segments are assumed to be stacked vertically (i.e. from the same
// column slice in static schema).
// This is used in CompactDataClause to simultaneously combine and split data segments into appropriate sizes with the
// minimum number of copies. It is a deliberately destructive process in order to free memory from the input data as
// early as possible.
// The nature of the implementation and API mean that it can be used for 2 potentially more generally useful
// applications:
// - Combining an arbitrary number of segments into a single one - by providing max_rows_per_segment to the constructor
//   that is >= the total number of rows in the input segments
// - Splitting a segment into a set of (approximately) equally sized smaller segments
// Currently does not support sparse data, this limitation will be removed in future PRs.
class SegmentReslicer {
  public:
    explicit SegmentReslicer(uint64_t max_rows_per_segment);

    ARCTICDB_NO_MOVE_OR_COPY(SegmentReslicer)

    std::vector<SegmentInMemory> reslice_segments(std::vector<SegmentInMemory>&& segments);

  private:
    const uint64_t max_rows_per_segment_;
};

} // namespace arcticdb
