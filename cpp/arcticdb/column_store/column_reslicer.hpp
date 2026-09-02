/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/expression_node.hpp>

namespace arcticdb {

[[nodiscard]] size_t min_rows_per_segment(size_t rows_per_segment);

[[nodiscard]] size_t max_rows_per_segment(size_t rows_per_segment);

// Helper class used by both the column and segment reslicer classes
class ReslicingInfo {
  public:
    ReslicingInfo(uint64_t _total_rows, uint64_t _rows_per_segment) :
        total_rows_(_total_rows),
        num_segments_((total_rows_ + _rows_per_segment - 1) / _rows_per_segment),
        rows_per_segment(total_rows_ / num_segments_),
        num_remainder_segments(total_rows_ % num_segments_),
        num_exact_segments(num_segments_ - num_remainder_segments) {
        auto output_rows = num_exact_segments * rows_per_segment + num_remainder_segments * (rows_per_segment + 1);
        util::check(
                output_rows == total_rows_,
                "SlicingInfo input rows does not match constructed output rows {} != {}",
                total_rows_,
                output_rows
        );
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ReslicingInfo)

    [[nodiscard]] uint64_t rows_in_slice(uint64_t idx) const {
        return idx < num_exact_segments ? rows_per_segment : rows_per_segment + 1;
    }

    [[nodiscard]] uint64_t num_segments() const { return num_segments_; }

    [[nodiscard]] uint64_t total_rows() const { return total_rows_; }

    /// Inverse of ReslicingInfo::rows_in_slice: given a row index into the combined [0, total_rows) output, returns
    /// the (slice index, offset within that slice) pair that rows_in_slice's slicing would place it in.
    [[nodiscard]] std::pair<uint64_t, uint64_t> slice_and_offset_for_row(uint64_t global_row) const {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                global_row < total_rows_,
                "ReslicingInfo::slice_and_offset_for_row: row {} is out of bounds for {} total rows",
                global_row,
                total_rows_
        );
        const uint64_t exact_rows = num_exact_segments * rows_per_segment;
        if (global_row < exact_rows) {
            return {global_row / rows_per_segment, global_row % rows_per_segment};
        }
        const uint64_t remainder_row = global_row - exact_rows;
        return {num_exact_segments + remainder_row / (rows_per_segment + 1), remainder_row % (rows_per_segment + 1)};
    }

  private:
    uint64_t total_rows_;
    uint64_t num_segments_;

    // This is how many rows most segments will have
    uint64_t rows_per_segment;
    // This is how many segments will have rows_per_segment+1 rows
    uint64_t num_remainder_segments;
    // This is how many segments will have exactly rows_per_segment rows
    uint64_t num_exact_segments;
};

// Given a maximum number of rows per slice, reslices a set of columns into a new shape, with at most
// max_rows_per_slice_ rows in each one.
// This is used in SegmentReslicer to simultaneously combine and split data segments into appropriate sizes with the
// minimum number of copies. It is a deliberately destructive process in order to free memory from the input data as
// early as possible.
// The nature of the implementation and API mean that it can be used for 2 potentially more generally useful
// applications:
// - Combining an arbitrary number of columns into a single one - by providing max_rows_per_segment to the constructor
//   that is >= the total number of rows in the input segments
// - Splitting a column into a set of (approximately) equally sized smaller columns
class ColumnReslicer {
  public:
    explicit ColumnReslicer(const size_t num_input_slices, const ReslicingInfo& reslicing_info);

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnReslicer)

    void push_back(std::shared_ptr<Column> column, std::shared_ptr<StringPool> string_pool);
    void push_back(size_t row_count);
    // There should be as many provided string pools as there will be output columns as these are for the output
    // segments
    std::vector<Column> reslice_columns(std::vector<StringPool>& string_pools);
    // Public only for benchmarking
    std::vector<Column> initialise_output_columns() const;

  private:
    // Note that both of these methods care about sparsity only when calling initialise_output_columns
    // Once the output buffers have been allocated, dense and sparse inputs and outputs work in the same way, as every
    // value from the input must be copied to an element of the output.
    std::vector<Column> reslice_by_memcpy();
    std::vector<Column> reslice_by_iteration(std::vector<StringPool>& string_pools);

    ReslicingInfo reslicing_info_;
    // Holds either a column along with its string pool, or the number of skipped rows if a row-slice was missing with
    // dynamic schema
    std::vector<std::variant<ColumnWithStrings, size_t>> cols_or_row_counts_;
    std::optional<TypeDescriptor> type_;
    // This can either be an explicitly sparse input column (not yet implemented), or dynamic schema with a missing
    // row slice. Only stored as we can optimise out the global bitset calculation in the dense case.
    bool sparse_{false};
    // In this case we can memcpy instead of iterating, as we must with string columns to reconstruct the string
    // pool, or with changing numeric types where we need to static cast
    bool numeric_types_all_same_{true};
};

} // namespace arcticdb
