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

namespace arcticdb {

namespace util {

using BitSet = bm::bvector<>;
using BitMagic = bm::bvector<>;
using BitMagicStart = SmallMagicNum<'M', 's'>;
using BitMagicEnd = SmallMagicNum<'M', 'e'>;
using BitSetSizeType = bm::bvector<>::size_type;
using BitIndex = bm::bvector<>::rs_index_type;

template<typename functor>
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

bool get_bit_at(const uint8_t* packed_bits, size_t bit_pos);

void set_bit_at(uint8_t* packed_bits, size_t bit_pos, bool value);

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
