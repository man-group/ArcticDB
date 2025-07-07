/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/magic_num.hpp>

#include <bitmagic/bm.h>

namespace arcticdb {

namespace util {

using BitSet = bm::bvector<>;
using BitMagic = bm::bvector<>;
using BitMagicStart = SmallMagicNum<'M', 's'>;
using BitMagicEnd = SmallMagicNum<'M', 'e'>;
using BitSetSizeType = bm::bvector<>::size_type;
using BitIndex = bm::bvector<>::rs_index_type;

/// @brief Get the combined size of the magic words used as delimiters for the sparse bitmaps
/// When sparse bitmaps are encoded we use two different magic words to mark the start and the end of the bitmap
[[nodiscard]] inline constexpr size_t combined_bit_magic_delimiters_size() {
    return sizeof(BitMagicStart) + sizeof(BitMagicEnd);
}
} // namespace util

constexpr bm::bvector<>::size_type bv_size(uint64_t val) {
    return static_cast<bm::bvector<>::size_type>(val);
}

constexpr size_t bitset_unpacked_size(size_t size) {
    return (size + 63) / 64 * sizeof(uint64_t);
}

inline void bitset_to_packed_bits(const bm::bvector<>& bv, uint64_t* dest_ptr) {
    std::memset(dest_ptr, 0, bitset_unpacked_size(bv.size()));

    auto en = bv.first();
    auto last = bv.end();
    for (; en != last; ++en) {
        size_t bit_pos = *en;

        size_t word_idx = bit_pos / 64;
        size_t bit_idx = bit_pos % 64;

        dest_ptr[word_idx] |= (1ULL << bit_idx);
    }
}

}
