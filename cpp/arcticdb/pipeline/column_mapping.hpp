/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <optional>

namespace arcticdb::pipelines {
struct PipelineContextRow;
}

namespace arcticdb {
class Column;
class SegmentInMemory;

struct ColumnTruncation {
    ColumnTruncation(std::optional<size_t> start, std::optional<size_t> end) : start_(start), end_(end) {}

    ColumnTruncation() = default;

    bool requires_truncation() const { return start_ || end_; }

    std::optional<size_t> start_;
    std::optional<size_t> end_;
};

struct ColumnMapping {
    const entity::TypeDescriptor source_type_desc_;
    const entity::TypeDescriptor dest_type_desc_;
    const entity::Field& frame_field_descriptor_;
    const size_t dest_size_;
    const size_t num_rows_;
    const size_t first_row_;
    const size_t offset_bytes_;
    const size_t dest_bytes_;
    const size_t dest_col_;
    ColumnTruncation truncate_;

    ColumnMapping(SegmentInMemory& frame, size_t dst_col, size_t field_col, pipelines::PipelineContextRow& context);

    ColumnMapping(
            const entity::TypeDescriptor source_type_desc, const entity::TypeDescriptor dest_type_desc,
            const entity::Field& frame_field_descriptor, const size_t dest_size, const size_t num_rows,
            const size_t first_row, const size_t offset_bytes, const size_t dest_bytes, const size_t dest_col
    );

    void set_truncate(ColumnTruncation truncate) { truncate_ = std::move(truncate); }

    bool requires_truncation() const { return truncate_.requires_truncation(); }
};

struct StaticColumnMappingIterator {
    const size_t index_fieldcount_;
    const size_t field_count_;
    const size_t first_slice_col_offset_;
    const size_t last_slice_col_offset_;
    ssize_t prev_col_offset_ = 0;
    size_t source_col_ = 0;
    size_t source_field_pos_ = 0;
    size_t dst_col_ = 0;
    bool invalid_ = false;
    const std::optional<util::BitSet>& bit_set_;

    StaticColumnMappingIterator(pipelines::PipelineContextRow& context, size_t index_fieldcount);

    void advance();
    [[nodiscard]] std::optional<size_t> get_next_source_col() const;
    [[nodiscard]] bool invalid() const;
    [[nodiscard]] bool has_next() const;
    [[nodiscard]] bool at_end_of_selected() const;
    [[nodiscard]] size_t remaining_fields() const;
    [[nodiscard]] size_t prev_col_offset() const;
    [[nodiscard]] size_t source_field_pos() const;
    [[nodiscard]] size_t source_col() const;
    [[nodiscard]] size_t first_slice_col_offset() const;
    [[nodiscard]] size_t last_slice_col_offset() const;
    [[nodiscard]] size_t dest_col() const;
    [[nodiscard]] size_t field_count() const;
    [[nodiscard]] size_t index_fieldcount() const;
};

void handle_truncation(Column& dest_column, const ColumnTruncation& truncate);

void handle_truncation(Column& dest_column, const ColumnMapping& mapping);

void handle_truncation(util::BitSet& bv, const ColumnTruncation& truncate);

void create_dense_bitmap(
        size_t offset, const util::BitSet& sparse_map, Column& dest_column, entity::AllocationType allocation_type
);

void create_dense_bitmap_all_zeros(
        size_t offset, size_t num_bits, Column& dest_column, entity::AllocationType allocation_type
);

} // namespace arcticdb
