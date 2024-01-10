/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

inline std::size_t sizeof_datatype(const TypeDescriptor &td) {
    return td.visit_tag([](auto dt) {
        using RawType = typename std::decay_t<decltype(dt)>::DataTypeTag::raw_type;
        // Array types are stored on disk as flat sequences. The python layer cannot work with this. We need to pass
        // it pointers to an array type (at numpy arrays at the moment). When we allocate a column for an array we
        // need to allocate space for one pointer per row. This also affects how we handle arrays to python as well.
        // Check cpp/arcticdb/column_store/column_utils.hpp::array_at and cpp/arcticdb/column_store/column.hpp
        return dt.dimension() == Dimension::Dim0 ? sizeof(RawType) : sizeof(void*);
    });
}

struct ColumnMapping {
    const TypeDescriptor source_type_desc_;
    const TypeDescriptor dest_type_desc_;
    const Field& frame_field_descriptor_;
    const size_t dest_size_;
    const size_t num_rows_;
    const size_t first_row_;
    const size_t offset_bytes_;
    const size_t dest_bytes_;

    ColumnMapping(SegmentInMemory &frame, size_t dst_col, size_t field_col, pipelines::PipelineContextRow &context) :
        source_type_desc_(context.descriptor().fields(field_col).type()),
        dest_type_desc_(frame.field(dst_col).type()),
        frame_field_descriptor_(frame.field(dst_col)),
        dest_size_(sizeof_datatype(dest_type_desc_)),
        num_rows_(context.slice_and_key().slice_.row_range.diff()),
        first_row_(context.slice_and_key().slice_.row_range.first - frame.offset()),
        offset_bytes_(dest_size_ * first_row_),
        dest_bytes_(dest_size_ * num_rows_) {
    }
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
    
    StaticColumnMappingIterator(pipelines::PipelineContextRow& context, size_t index_fieldcount) :
        index_fieldcount_(index_fieldcount),
        field_count_(context.slice_and_key().slice_.col_range.diff() + index_fieldcount),
        first_slice_col_offset_(context.slice_and_key().slice_.col_range.first),
        last_slice_col_offset_(context.slice_and_key().slice_.col_range.second),
        bit_set_(context.get_selected_columns()) {
            prev_col_offset_ = first_slice_col_offset_ - 1;
            if(bit_set_) {
                source_col_ = (*bit_set_)[bv_size(first_slice_col_offset_)] ? first_slice_col_offset_ : bit_set_->get_next(bv_size(first_slice_col_offset_));
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
                if(source_col_ < first_slice_col_offset_)
                    invalid_ = true;

            } else {
                source_col_ = first_slice_col_offset_;
            }

            dst_col_ = bit_set_ ? bit_set_->count_range(0, bv_size(source_col_)) - 1 : source_col_;
            source_field_pos_ = (source_col_ - first_slice_col_offset_) + index_fieldcount_;
    }

    std::optional<size_t> get_next_source_col() const {
        if(!bit_set_) {
            return source_col_ + 1;
        } else {
            auto next_pos = bit_set_->get_next(bv_size(source_col_));
            if(next_pos == 0)
                return std::nullopt;
            else
                return next_pos;
        }
    }

    void advance() {
        ++dst_col_;
        prev_col_offset_ = source_col_;
        auto new_source_col = get_next_source_col();
        if(new_source_col) {
            source_col_ = *new_source_col;
            source_field_pos_ = (source_col_ - first_slice_col_offset_) + index_fieldcount_;
        } else {
            source_field_pos_ = field_count_;
            source_col_ = last_slice_col_offset_;
        }
    }

    bool invalid() const {
        return invalid_;
    }

    bool has_next() const {
        return source_field_pos_ < field_count_;
    }

    bool at_end_of_selected() const {
        return !source_col_ || source_col_ >= last_slice_col_offset_;
    }

    size_t remaining_fields() const {
        return field_count_ - source_field_pos_;
    }

    size_t prev_col_offset() const {
        return prev_col_offset_;
    }

    size_t source_field_pos() const {
        return source_field_pos_;
    }

    size_t source_col() const {
        return source_col_;
    }

    size_t first_slice_col_offset() const {
        return first_slice_col_offset_;
    }

    size_t last_slice_col_offset() const {
        return last_slice_col_offset_;
    }

    size_t dest_col() const {
        return dst_col_;
    }

    size_t field_count() const {
        return field_count_;
    }

    size_t index_fieldcount() const {
        return index_fieldcount_;
    }
};


} // namespace arcticdb
