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
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/expression_node.hpp>

namespace arcticdb {

// Helper class used by both the column and segment reslicer classes
class ReslicingInfo {
  public:
    ReslicingInfo(uint64_t _total_rows, uint64_t _rows_per_segment) :
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

    ARCTICDB_MOVE_COPY_DEFAULT(ReslicingInfo)

    uint64_t rows_in_slice(uint64_t idx) const {
        return idx < num_exact_segments ? rows_per_segment : rows_per_segment + 1;
    }

    uint64_t total_rows;
    uint64_t num_segments;

  private:
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
// - Splitting a columns into a set of (approximately) equally sized smaller columns
class ColumnReslicer {
  public:
    explicit ColumnReslicer(const ReslicingInfo& reslicing_info);

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnReslicer)

    void push_back(std::shared_ptr<Column> column, std::shared_ptr<StringPool> string_pool);
    void push_back(size_t row_count);
    // There should be as many provided string pools as there will be output columns as these are for the output
    // segments
    std::vector<Column> reslice_columns(std::vector<StringPool>& string_pools);

  private:
    // Note that both of these methods care about sparsity only when calling initialise_output_columns
    // Once the output buffers have been allocated, dense and sparse inputs and outputs work in the same way, as every
    // value from the input must be copied to an element of the output.
    std::vector<Column> reslice_by_memcpy();
    std::vector<Column> reslice_by_iteration(std::vector<StringPool>& string_pools);
    std::vector<Column> initialise_output_columns() const;

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
