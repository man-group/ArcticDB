#pragma once

#include <arcticdb/column_store/column_data.hpp>

#include <cstddef>
#include <climits>

namespace arcticdb {

constexpr size_t round_up_bits(size_t bits) noexcept {
    return (bits + (CHAR_BIT - 1)) / CHAR_BIT;
}

template <typename T>
constexpr size_t round_up_bits_in_t(size_t bits) noexcept {
    constexpr size_t t_bits = sizeof(T) * CHAR_BIT;
    return (bits + (t_bits - 1)) / t_bits;
}

template <typename T>
constexpr std::size_t round_up_bytes_in_t(size_t bytes) {
    constexpr auto t_size = sizeof(T);
    return ((bytes + t_size - 1) / t_size) * t_size;
}

template <typename T>
void fill_remainder_array(
    std::array<T, alp::config::VECTOR_SIZE>& remainder_data,
    const ColumnData& input,
    size_t remainder_offset,
    size_t remainder_size) {
    util::check(remainder_size <= alp::config::VECTOR_SIZE, "Remainder size {} too large for remainder array", remainder_size);
    const size_t start_byte = remainder_offset * sizeof(T);
    const size_t total_bytes = remainder_size * sizeof(T);
    size_t copied_bytes = 0;

    auto pos_info = input.buffer().block_and_offset(start_byte);
    size_t block_index = pos_info.block_index_;
    size_t local_offset = pos_info.offset_;

    const auto &blocks = input.buffer().blocks();

    while (copied_bytes < total_bytes) {
        MemBlock* block = blocks[block_index];
        size_t available_bytes = block->bytes() - local_offset;
        size_t bytes_to_copy = std::min(total_bytes - copied_bytes, available_bytes);

        memcpy(
            reinterpret_cast<uint8_t*>(remainder_data.data()) + copied_bytes,
            reinterpret_cast<const uint8_t *>(block->data()) + local_offset,
            bytes_to_copy
        );

        copied_bytes += bytes_to_copy;

        ++block_index;
        local_offset = 0;
    }
}
} // namespace arcticdb