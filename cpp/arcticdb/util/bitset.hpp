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

// adapted from bm_algo.h
template <typename Func>
struct BitVisitorFunctor {
    Func func_;
    uint64_t rank_ = 0;

    BitVisitorFunctor(Func&& func) :
        func_(func) {
    }

    using size_type = util::BitSetSizeType;

    int add_bits(
        size_type offset,
        const unsigned char* bits,
        unsigned size) {
        for (unsigned i = 0; i < size; ++i) {
            func_(offset + bits[i], rank_);
            ++rank_;
        }
        return 0;
    }

    int add_range(size_type offset, size_type size) {
        for (size_type i = 0; i < size; ++i){
            func_(offset + i, rank_);
            ++rank_;
        }
        return 0;
    }
};
}
