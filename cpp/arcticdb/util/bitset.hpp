/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>

#include <bitmagic/bm.h>
#include <bitmagic/bmalgo.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace arcticdb {

namespace util {

using BitSet = bm::bvector<>;
using BitMagic = bm::bvector<>;
using BitMagicStart = SmallMagicNum<'M', 's'>;
using BitMagicEnd = SmallMagicNum<'M', 'e'>;
using BitSetSizeType = bm::bvector<>::size_type;
using BitIndex = bm::bvector<>::rs_index_type;

template<typename functor>
requires std::is_invocable_r_v<void, functor, size_t>
struct BitSetVisitor {
    functor& f;
    int add_bits(BitSet::size_type off, const unsigned char* bits, unsigned n) {
        for (unsigned i = 0; i < n; ++i) {
            f(off + bits[i]);
        }
        return 0;
    }
    int add_range(BitSet::size_type off, BitSet::size_type n) {
        for (BitSet::size_type i = 0; i < n; ++i) {
            f(off + i);
        }
        return 0;
    }
};

/// @brief Get the combined size of the magic words used as delimiters for the sparse bitmaps
/// When sparse bitmaps are encoded we use two different magic words to mark the start and the end of the bitmap
[[nodiscard]] inline constexpr size_t combined_bit_magic_delimiters_size() {
    return sizeof(BitMagicStart) + sizeof(BitMagicEnd);
}
} // namespace util

constexpr bm::bvector<>::size_type bv_size(uint64_t val) { return static_cast<bm::bvector<>::size_type>(val); }

// The number of bytes needed to hold num_bits in a packed bitset
constexpr size_t bitset_packed_size_bytes(size_t num_bits) { return (num_bits + 7) / 8; }

void bitset_to_packed_bits(const bm::bvector<>& bv, uint8_t* dest_ptr);

void packed_bits_to_buffer(const uint8_t* packed_bits, size_t num_bits, size_t offset, uint8_t* dest_ptr);

void bools_to_packed_bits(const bool* src, size_t num_bools, uint8_t* dest);

inline bool get_bit_at(const uint8_t* packed_bits, size_t bit_pos) {
    return (packed_bits[bit_pos >> 3] >> (bit_pos & 7)) & 1;
}

inline void set_bit_at(uint8_t* packed_bits, size_t bit_pos, bool value) {
    const size_t byte_idx = bit_pos >> 3;
    const auto bit_idx = static_cast<unsigned>(bit_pos & 7);
    packed_bits[byte_idx] &= static_cast<uint8_t>(~(1u << bit_idx));                        // Unset bit
    packed_bits[byte_idx] |= static_cast<uint8_t>(static_cast<unsigned>(value) << bit_idx); // Set bit
}

// Calls f(i) for every i in [0, num_bits) whose bit (bit_offset + i) of packed_bits is unset, in ascending order.
// Scans 64 bits at a time so the cost is proportional to the number of unset bits rather than to num_bits.
// Only the bytes holding bits [bit_offset, bit_offset + num_bits) are read, so the caller need only guarantee that
// range is in bounds.
template<typename functor>
requires std::is_invocable_r_v<void, functor, size_t>
void for_each_unset_bit(const uint8_t* packed_bits, size_t bit_offset, size_t num_bits, functor&& f) {
    static_assert(std::endian::native == std::endian::little, "Word-at-a-time bit scan assumes a little-endian host");
    if (num_bits == 0) {
        return;
    }
    const uint8_t* const base = packed_bits + (bit_offset >> 3);
    // Logical bit i is bit (shift + i) of the bit stream starting at *base, so shift < 8 is the only misalignment
    const size_t shift = bit_offset & 7;
    const size_t total_bits = shift + num_bits;
    const size_t total_bytes = bitset_packed_size_bytes(total_bits);
    for (size_t word_start = 0; word_start < total_bits; word_start += 64) {
        const size_t byte_idx = word_start >> 3;
        const size_t available_bytes = std::min(size_t{8}, total_bytes - byte_idx);
        uint64_t word = 0;
        std::memcpy(&word, base + byte_idx, available_bytes);
        // Bits [lo, hi) of this word map to a logical bit. lo is only non-zero for the first word, hi only clips the
        // last one. Bits outside [lo, hi) are masked off, which also discards the zero-padding of a partial load.
        const size_t lo = word_start == 0 ? shift : 0;
        const size_t hi = std::min(size_t{64}, total_bits - word_start);
        const uint64_t mask = (hi == 64 ? ~uint64_t{0} : ((uint64_t{1} << hi) - 1)) & (~uint64_t{0} << lo);
        uint64_t unset = ~word & mask;
        while (unset != 0) {
            const auto bit = static_cast<size_t>(std::countr_zero(unset));
            f(word_start + bit - shift);
            unset &= unset - 1;
        }
    }
}

void copy_packed_bits(const uint8_t* src, size_t src_bit_offset, size_t num_bits, uint8_t* dest);

template<typename functor>
requires std::is_invocable_r_v<void, functor, size_t>
void iterate_over_set_positions(const bm::bvector<>& bv, size_t from, size_t to, functor&& f) {
    util::check(to >= from, "Invalid bit set iteration range: from {} to {}", from, to);
    if (from == to) {
        return;
    }
    util::BitSetVisitor visitor{f};
    // the range that bitmagic requires is inclusive
    bm::for_each_bit_range(bv, from, to - 1, visitor);
}

template<typename functor>
requires std::is_invocable_r_v<void, functor, size_t>
void iterate_over_set_positions(const bm::bvector<>& bv, functor&& f) {
    util::BitSetVisitor visitor{f};
    bm::for_each_bit(bv, visitor);
}

} // namespace arcticdb
