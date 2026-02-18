/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/typed_block_data.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/buffer.hpp>

namespace arcticdb {

/// Block-level range for iterating over typed blocks in a column's buffer.
/// Holds mutable position state (pos_, shape_pos_) for sequential iteration.
/// This replaces the block iteration aspect of ColumnData.
class ColumnBlockRange {
  public:
    ColumnBlockRange(const ChunkedBuffer& data, const Buffer* shapes, const TypeDescriptor& type) :
        data_(&data),
        shapes_(shapes),
        type_(type) {}

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> next() {
        IMemBlock* block = nullptr;
        do {
            if (pos_ == num_blocks())
                return std::nullopt;

            block = data_->blocks().at(pos_++);
        } while (!block);

        return next_typed_block<TDT>(block);
    }

    template<typename TDT>
    std::optional<TypedBlockData<TDT>> last() {
        if (data_->blocks().empty())
            return std::nullopt;

        pos_ = num_blocks() - 1;
        auto block = data_->blocks().at(pos_);
        return next_typed_block<TDT>(block);
    }

    void reset() {
        pos_ = 0;
        shape_pos_ = 0;
    }

    [[nodiscard]] size_t num_blocks() const { return data_->blocks().size(); }

    [[nodiscard]] TypeDescriptor type() const { return type_; }

  private:
    template<typename TDT>
    TypedBlockData<TDT> next_typed_block(IMemBlock* block) {
        size_t num_elements = 0;
        const shape_t* shape_ptr = nullptr;

        constexpr auto dim = TDT::DimensionTag::value;
        if constexpr (dim == Dimension::Dim0) {
            auto logical_size = block->logical_size();
            num_elements = logical_size / get_type_size(type_.data_type());
        } else {
            util::check(!data_->has_extra_bytes_per_block(), "Can't have `extra_bytes_per_block` when using dim > 0");
            if (shapes_->empty()) {
                num_elements = block->logical_size() / get_type_size(type_.data_type());
            } else {
                shape_ptr = shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
                size_t size = 0;
                constexpr auto raw_type_sz = sizeof(typename TDT::DataTypeTag::raw_type);
                while (current_tensor_is_empty() || size < block->logical_size()) {
                    if constexpr (dim == Dimension::Dim1) {
                        size += next_shape() * raw_type_sz;
                    } else {
                        const shape_t row_count = next_shape();
                        const shape_t column_count = next_shape();
                        util::check(
                                row_count > 0 || (row_count == 0 && column_count == 0),
                                "Tensor column count must be zero when the row count is 0"
                        );
                        size += row_count * column_count * raw_type_sz;
                    }
                    ++num_elements;
                }
                util::check(
                        size == block->logical_size(),
                        "Element size vs block size overrun: {} > {}",
                        size,
                        block->logical_size()
                );
            }
        }

        return TypedBlockData<TDT>{
                reinterpret_cast<const typename TDT::DataTypeTag::raw_type*>(block->data()),
                shape_ptr,
                block->physical_bytes(),
                num_elements,
                block
        };
    }

    [[nodiscard]] bool current_tensor_is_empty() const {
        return shape_pos_ < shapes_->bytes() && *shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t)) == 0;
    }

    shape_t next_shape() {
        auto shape = *shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t));
        shape_pos_ += sizeof(shape_t);
        return shape;
    }

    const ChunkedBuffer* data_;
    const Buffer* shapes_;
    size_t pos_ = 0;
    size_t shape_pos_ = 0;
    TypeDescriptor type_;
};

} // namespace arcticdb
