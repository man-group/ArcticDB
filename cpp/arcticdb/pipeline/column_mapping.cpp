/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>

namespace arcticdb {
ColumnMapping::ColumnMapping(
        SegmentInMemory& frame, size_t dst_col, size_t field_col, pipelines::PipelineContextRow& context
) :
    source_type_desc_(context.descriptor().fields(field_col).type()),
    dest_type_desc_(frame.field(dst_col).type()),
    frame_field_descriptor_(frame.field(dst_col)),
    dest_size_(data_type_size(dest_type_desc_)),
    num_rows_(context.slice_and_key().slice_.row_range.diff()),
    first_row_(context.slice_and_key().slice_.row_range.first - frame.offset()),
    offset_bytes_(dest_size_ * first_row_),
    dest_bytes_(dest_size_ * num_rows_),
    dest_col_(dst_col) {}

ColumnMapping::ColumnMapping(
        const entity::TypeDescriptor source_type_desc, const entity::TypeDescriptor dest_type_desc,
        const entity::Field& frame_field_descriptor, const size_t dest_size, const size_t num_rows,
        const size_t first_row, const size_t offset_bytes, const size_t dest_bytes, const size_t dest_col
) :
    source_type_desc_(source_type_desc),
    dest_type_desc_(dest_type_desc),
    frame_field_descriptor_(frame_field_descriptor),
    dest_size_(dest_size),
    num_rows_(num_rows),
    first_row_(first_row),
    offset_bytes_(offset_bytes),
    dest_bytes_(dest_bytes),
    dest_col_(dest_col) {}

StaticColumnMappingIterator::StaticColumnMappingIterator(
        pipelines::PipelineContextRow& context, size_t index_fieldcount
) :
    index_fieldcount_(index_fieldcount),
    field_count_(context.slice_and_key().slice_.col_range.diff() + index_fieldcount),
    first_slice_col_offset_(context.slice_and_key().slice_.col_range.first),
    last_slice_col_offset_(context.slice_and_key().slice_.col_range.second),
    bit_set_(context.get_selected_columns()) {
    prev_col_offset_ = first_slice_col_offset_ - 1;
    if (bit_set_) {
        source_col_ = (*bit_set_)[bv_size(first_slice_col_offset_)]
                              ? first_slice_col_offset_
                              : bit_set_->get_next(bv_size(first_slice_col_offset_));
        if ((*bit_set_)[bv_size(first_slice_col_offset_)]) {
            source_col_ = first_slice_col_offset_;
        } else {
            auto next_pos = bit_set_->get_next(bv_size(first_slice_col_offset_));
            // We have to do this extra check in bitmagic, get_next returns 0 in case no next present
            if (next_pos == 0 && bit_set_->size() > 0 && !bit_set_->test(0))
                invalid_ = true;
            else
                source_col_ = next_pos;
        }
        if (source_col_ < first_slice_col_offset_)
            invalid_ = true;

    } else {
        source_col_ = first_slice_col_offset_;
    }

    dst_col_ = bit_set_ ? bit_set_->count_range(0, bv_size(source_col_)) - 1 : source_col_;
    source_field_pos_ = (source_col_ - first_slice_col_offset_) + index_fieldcount_;
}

std::optional<size_t> StaticColumnMappingIterator::get_next_source_col() const {
    if (!bit_set_) {
        return source_col_ + 1;
    } else {
        auto next_pos = bit_set_->get_next(bv_size(source_col_));
        if (next_pos == 0)
            return std::nullopt;
        else
            return next_pos;
    }
}

void StaticColumnMappingIterator::advance() {
    ++dst_col_;
    prev_col_offset_ = source_col_;
    auto new_source_col = get_next_source_col();
    if (new_source_col) {
        source_col_ = *new_source_col;
        source_field_pos_ = (source_col_ - first_slice_col_offset_) + index_fieldcount_;
    } else {
        source_field_pos_ = field_count_;
        source_col_ = last_slice_col_offset_;
    }
}

bool StaticColumnMappingIterator::invalid() const { return invalid_; }

bool StaticColumnMappingIterator::has_next() const { return source_field_pos_ < field_count_; }

bool StaticColumnMappingIterator::at_end_of_selected() const {
    return !source_col_ || source_col_ >= last_slice_col_offset_;
}

size_t StaticColumnMappingIterator::remaining_fields() const { return field_count_ - source_field_pos_; }

size_t StaticColumnMappingIterator::prev_col_offset() const { return prev_col_offset_; }

size_t StaticColumnMappingIterator::source_field_pos() const { return source_field_pos_; }

size_t StaticColumnMappingIterator::source_col() const { return source_col_; }

size_t StaticColumnMappingIterator::first_slice_col_offset() const { return first_slice_col_offset_; }

size_t StaticColumnMappingIterator::last_slice_col_offset() const { return last_slice_col_offset_; }

size_t StaticColumnMappingIterator::dest_col() const { return dst_col_; }

size_t StaticColumnMappingIterator::field_count() const { return field_count_; }

size_t StaticColumnMappingIterator::index_fieldcount() const { return index_fieldcount_; }

void handle_truncation(Column& dest_column, const ColumnTruncation& truncate) {
    if (dest_column.num_blocks() == 1 && truncate.start_ && truncate.end_) {
        dest_column.truncate_single_block(*truncate.start_, *truncate.end_);
    } else {
        if (truncate.start_)
            dest_column.truncate_first_block(*truncate.start_);
        if (truncate.end_)
            dest_column.truncate_last_block(*truncate.end_);
    }
}

void handle_truncation(Column& dest_column, const ColumnMapping& mapping) {
    handle_truncation(dest_column, mapping.truncate_);
}

void handle_truncation(util::BitSet& bv, const ColumnTruncation& truncate) {
    if (truncate.start_) {
        bv = util::truncate_sparse_map(bv, *truncate.start_, truncate.end_.value_or(bv.size()));
    } else if (truncate.end_) {
        // More efficient than util::truncate_sparse_map as it avoids a copy
        bv.resize(*truncate.end_);
    }
}

void create_dense_bitmap(
        size_t offset, const util::BitSet& sparse_map, Column& dest_column, AllocationType allocation_type
) {
    auto& sparse_buffer = dest_column.create_extra_buffer(
            offset, ExtraBufferType::BITMAP, bitset_packed_size_bytes(sparse_map.size()), allocation_type
    );

    bitset_to_packed_bits(sparse_map, sparse_buffer.data());
}

void create_dense_bitmap_all_zeros(
        size_t offset, size_t num_bits, Column& dest_column, AllocationType allocation_type
) {
    auto num_bytes = bitset_packed_size_bytes(num_bits);
    auto& sparse_buffer = dest_column.create_extra_buffer(offset, ExtraBufferType::BITMAP, num_bytes, allocation_type);
    std::memset(sparse_buffer.data(), 0, num_bytes);
}

} // namespace arcticdb