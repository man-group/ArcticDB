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
#include <arcticdb/processing/expression_node.hpp>

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
    class SlicingInfo {
      public:
        SlicingInfo(uint64_t _total_rows, uint64_t _rows_per_segment) :
            total_rows(_total_rows),
            num_segments((total_rows + _rows_per_segment - 1) / _rows_per_segment),
            rows_per_segment(total_rows / num_segments),
            num_remainder_segments(total_rows % num_segments),
            num_exact_segments(num_segments - num_remainder_segments) {
            auto output_rows = num_exact_segments * rows_per_segment + num_remainder_segments * (rows_per_segment + 1);
            util::check(
                    output_rows == total_rows,
                    "SlicingInfo input rows does not match constructed output rows {} != {}",
                    total_rows,
                    output_rows
            );
        }

        uint64_t rows_in_slice(uint64_t idx) const {
            return idx < num_exact_segments ? rows_per_segment : rows_per_segment + 1;
        }

        const uint64_t total_rows;
        const uint64_t num_segments;

      private:
        // This is how many rows most segments will have
        const uint64_t rows_per_segment;
        // This is how many segments will have rows_per_segment+1 rows
        const uint64_t num_remainder_segments;
        // This is how many segments will have exactly rows_per_segment rows
        const uint64_t num_exact_segments;
    };

    explicit SegmentReslicer(uint64_t max_rows_per_segment);

    ARCTICDB_NO_MOVE_OR_COPY(SegmentReslicer)

    // Main entry point. The other methods are public for testing purposes only
    std::vector<SegmentInMemory> reslice_segments(std::vector<SegmentInMemory>&& segments);
    std::vector<std::optional<Column>> reslice_columns(
            std::vector<std::variant<ColumnWithStrings, size_t>>&& cols_with_strings, const SlicingInfo& slicing_info,
            std::vector<StringPool>& string_pools
    );
    std::vector<std::optional<Column>> reslice_dense_numeric_static_schema_columns(
            std::vector<std::shared_ptr<Column>>&& cols, const SlicingInfo& slicing_info
    );
    std::vector<std::optional<Column>> reslice_dense_numeric_dynamic_schema_columns(
            std::vector<std::shared_ptr<Column>>&& cols, const SlicingInfo& slicing_info,
            const TypeDescriptor& common_type
    );
    std::vector<std::optional<Column>> reslice_dense_string_columns(
            std::vector<ColumnWithStrings>&& cols_with_strings, const SlicingInfo& slicing_info,
            std::vector<StringPool>& string_pools
    );

  private:
    const uint64_t max_rows_per_segment_;
};

} // namespace arcticdb
