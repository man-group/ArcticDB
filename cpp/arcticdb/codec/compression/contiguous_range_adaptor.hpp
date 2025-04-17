#pragma once

#include <arcticdb/column_store/column_data.hpp>

namespace arcticdb {

// Forward and random-access adaptors for multi-block columns where access is
// required to contiguous memory. N.B. these are performance-focused and unchecked,
// so it's up to the caller to ensure that the required ranges are available

template <typename T, size_t size>
struct ContiguousRangeForwardAdaptor {
    ColumnData column_data_;
    const ChunkedBuffer::BlockVectorType& blocks_;
    std::optional<MemBlock*> block_;
    size_t block_pos_ = 0;
    size_t block_num_ = 0;
    std::array<T, size> buffer_;

    [[nodiscard]] constexpr size_t t_size(size_t rows) {
        return rows * sizeof(T);
    }

    [[nodiscard]] constexpr size_t rows(size_t bytes) {
        return bytes / sizeof(T);
    }

    const MemBlock& block() {
        return **block_;
    }

    [[nodiscard]] bool valid() const {
        return static_cast<bool>(block_);
    }

    void advance_block() {
        block_pos_ = 0UL;
        ++block_num_;
        block_ = block_num_ < blocks_.size() ? std::make_optional(blocks_.at(block_num_)) : std::nullopt;

    }

    ContiguousRangeForwardAdaptor(ColumnData data) :
        column_data_(data),
        blocks_(column_data_.buffer().blocks()),
        block_(blocks_[0]){
    }

    const T* current() {
        return reinterpret_cast<const T*>(block().data() + block_pos_);
    }

    const T* next() {
        if(block().bytes() >= block_pos_ + t_size(size))
            return current();

        auto required = size;
        size_t dest_offset = 0;
        while(required > 0) {
            if(block().bytes() > t_size(block_pos_)) {
                const auto bytes_available = block().bytes() - block_pos_;
                const auto bytes_to_copy = std::min(bytes_available, t_size(required));
                memcpy(buffer_.data() + dest_offset, current(), bytes_to_copy);
                required -= rows(bytes_to_copy);
                dest_offset += rows(bytes_to_copy);
                block_pos_ += bytes_to_copy;
            } else {
                advance_block();
            }
        }
        return buffer_.data();
    }
};

template <typename T, size_t size>
struct ContiguousRangeRandomAccessAdaptor {
    ColumnData column_data_;
    std::array<T, size> buffer_;

    ContiguousRangeRandomAccessAdaptor(ColumnData data) :
        column_data_(data) {
    }

    [[nodiscard]] constexpr size_t t_size(size_t rows) {
        return rows * sizeof(T);
    }


    [[nodiscard]] constexpr size_t rows(size_t bytes) {
        return bytes / sizeof(T);
    }

    const T* at(size_t row) {
        const auto bytes_offset = t_size(row);
        auto block_and_offset = column_data_.buffer().block_and_offset(bytes_offset);
        auto required = size;
        auto block_num = block_and_offset.block_index_;
        auto block = block_and_offset.block_;
        auto block_pos = block_and_offset.offset_;

        if(block->bytes() >= block_pos + t_size(size))
            return reinterpret_cast<const T*>(block->data() + block_pos);

        size_t dest_offset = 0;
        while(required > 0) {
            if(block->bytes() > block_pos) {
                const auto bytes_available = block->bytes() - block_pos;
                const auto bytes_to_copy = std::min(bytes_available, t_size(required));
                memcpy(buffer_.data() + dest_offset, block->data() + block_pos, bytes_to_copy);
                required -= rows(bytes_to_copy);
                dest_offset += rows(bytes_to_copy);
                block_pos += bytes_to_copy;
            } else {
                ++block_num;
                util::check(block_num < column_data_.buffer().num_blocks(), "Ran out of blocks in contiguous range adaptor");
                block = column_data_.buffer().blocks()[block_num];
                block_pos = 0UL;
            }
        }

        return buffer_.data();
    }
};

template <typename T>
struct DynamicRangeRandomAccessAdaptor {
    ColumnData column_data_;
    std::vector<T> buffer_;

    DynamicRangeRandomAccessAdaptor(ColumnData data)
        : column_data_(data)
    {
    }

    [[nodiscard]] constexpr size_t t_size(size_t count) const {
        return count * sizeof(T);
    }

    [[nodiscard]] constexpr size_t rows(size_t bytes) const {
        return bytes / sizeof(T);
    }

    // Return a pointer to a contiguous segment of 'count' T elements,
    // starting at the logical row 'row'.
    const T* at(size_t row, size_t count) {
        // byte offset corresponding to the row.
        const auto bytes_offset = t_size(row);
        auto block_and_offset = column_data_.buffer().block_and_offset(bytes_offset);
        size_t required = count;  // number of T elements still needed
        size_t block_num = block_and_offset.block_index_;
        MemBlock* block = block_and_offset.block_;
        size_t block_pos = block_and_offset.offset_;  // source offset (in bytes) in the current block

        if (block->bytes() >= block_pos + t_size(count))
            return reinterpret_cast<const T*>(block->data() + block_pos);

        // Ensure our internal buffer is large enough.
        if(buffer_.size() < count)
            buffer_.resize(count);

        size_t dest_offset = 0;
        while (required > 0) {
            if (block->bytes() > block_pos) {
                size_t bytes_available = block->bytes() - block_pos;
                size_t current_elements_to_copy = std::min(required, rows(bytes_available));
                size_t bytes_to_copy = t_size(current_elements_to_copy);
                memcpy(buffer_.data() + dest_offset, block->data() + block_pos, bytes_to_copy);
                required   -= current_elements_to_copy;
                dest_offset += current_elements_to_copy;
                block_pos  += bytes_to_copy;
            } else {
                ++block_num;
                util::check(block_num < column_data_.buffer().num_blocks(), "Ran out of blocks in contiguous range adaptor");
                block = column_data_.buffer().blocks()[block_num];
                block_pos = 0;
            }
        }
        return buffer_.data();
    }
};





} // namespace arcticdb