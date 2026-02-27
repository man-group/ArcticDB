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
// Currently only supports dense numeric data and static schema, these limitations will be removed in future PRs.
class SegmentReslicer {
  public:
    struct SlicingInfo {
        SlicingInfo(uint64_t _total_rows, uint64_t max_rows_per_segment) : total_rows(_total_rows) {
            num_segments = 1;
            rows_per_segment = total_rows;
            if (total_rows > max_rows_per_segment) {
                num_segments = (total_rows / max_rows_per_segment) + (total_rows % max_rows_per_segment == 0 ? 0 : 1);
                rows_per_segment = (total_rows / num_segments) + (total_rows % num_segments == 0 ? 0 : 1);
            }
        }
        uint64_t total_rows;
        uint64_t num_segments;
        uint64_t rows_per_segment;
    };

    explicit SegmentReslicer(uint64_t max_rows_per_segment);

    ARCTICDB_NO_MOVE_OR_COPY(SegmentReslicer)

    std::vector<SegmentInMemory> reslice_segments(std::vector<SegmentInMemory>&& segments);
    std::vector<std::optional<Column>> reslice_columns(
            std::vector<std::optional<std::shared_ptr<Column>>>&& columns, const SlicingInfo& slicing_info,
            std::vector<StringPool>& string_pools
    );
    std::vector<std::optional<Column>> reslice_dense_numeric_static_schema_columns(
            std::vector<std::optional<std::shared_ptr<Column>>>&& columns, const SlicingInfo& slicing_info
    );

  private:
    const uint64_t max_rows_per_segment_;
};

} // namespace arcticdb
