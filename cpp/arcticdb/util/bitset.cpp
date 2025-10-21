/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/bitset.hpp>

namespace arcticdb {

void bitset_to_packed_bits(const bm::bvector<>& bv, uint8_t* dest_ptr) {
    std::memset(dest_ptr, 0, bitset_packed_size_bytes(bv.size()));
    auto last = bv.end();
    for (auto en = bv.first(); en != last; ++en) {
        size_t bit_pos = *en;
        size_t byte_idx = bit_pos / 8;
        size_t bit_idx = bit_pos % 8;
        dest_ptr[byte_idx] |= (uint8_t(1) << bit_idx);
    }
}

void packed_bits_to_buffer(const uint8_t* packed_bits, size_t num_bits, size_t offset, uint8_t* dest_ptr) {
    packed_bits += offset / 8;
    auto shift = offset % 8;
    auto initial_bits = shift == 0 ? 0 : std::min(8 - shift, num_bits);
    if (initial_bits > 0) {
        uint8_t byte = *packed_bits++;

        for (size_t bit = shift; bit < shift + initial_bits; ++bit) {
            *dest_ptr++ = (byte >> bit) & 1;
        }
    }
    auto leftover_bits = (num_bits - initial_bits) % 8;
    size_t num_bytes = (num_bits - initial_bits) / 8;
    // Experimentally, this approach is ~33% faster than the naive bit iteration approach used for the initial/leftover
    // bits.
    // Explanation: the mask has a 1 in every eighth bit:
    // 0000000100000001000000010000000100000001000000010000000100000001
    // The input byte is then shifted by multiples of 7 so that this mask picks out the correct bit from the input for
    // each byte of the output. e.g. and input byte of 10000001 will produce the following 8 bytes to AND with the mask,
    // where bits that are zero in the mask are marked with an X as they are irrelevant
    // XXXXXXX1XXXXXXX0XXXXXXX0XXXXXXX0XXXXXXX0XXXXXXX0XXXXXXX0XXXXXXX1
    auto dest_word = reinterpret_cast<uint64_t*>(dest_ptr);
    constexpr uint64_t mask = 1ULL | (1ULL << 8) | (1ULL << 16) | (1ULL << 24) | (1ULL << 32) | (1ULL << 40) |
                              (1ULL << 48) | (1ULL << 56);
    for (size_t idx = 0; idx < num_bytes; ++idx, ++packed_bits) {
        uint64_t byte = static_cast<uint64_t>(*packed_bits);
        *dest_word++ = (byte | (byte << 7) | (byte << 14) | (byte << 21) | (byte << 28) | (byte << 35) | (byte << 42) |
                        (byte << 49)) &
                       mask;
    }
    if (leftover_bits > 0) {
        dest_ptr += 8 * num_bytes;
        uint8_t byte = *packed_bits;
        for (size_t bit = 0; bit < leftover_bits; ++bit) {
            *dest_ptr++ = (byte >> bit) & 1;
        }
    }
}

} // namespace arcticdb
