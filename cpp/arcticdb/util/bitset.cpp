/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/bitset.hpp>

#include <algorithm>
#include <bit>
#include <cstring>

namespace arcticdb {

namespace {
// Sets bits [first, last] of LSB-first packed bits
void set_packed_bit_range(uint8_t* dest, size_t first, size_t last) {
    const size_t first_byte = first / 8;
    const size_t last_byte = last / 8;
    const auto head = static_cast<uint8_t>(0xFFu << (first % 8));
    const auto tail = static_cast<uint8_t>(0xFFu >> (7 - last % 8));
    if (first_byte == last_byte) {
        dest[first_byte] |= head & tail;
        return;
    }
    dest[first_byte] |= head;
    std::memset(dest + first_byte + 1, 0xFF, last_byte - first_byte - 1);
    dest[last_byte] |= tail;
}
} // namespace

void bitset_to_packed_bits(const bm::bvector<>& bv, uint8_t* dest_ptr) {
    const size_t num_bits = bv.size();
    // BitMagic's bit blocks are arrays of 32-bit words with bit k of word w at position 32 * w + k, which on a
    // little-endian host is the same byte layout as Arrow's LSB-first packed bits, so a block can be copied as is
    static_assert(std::endian::native == std::endian::little);
    const size_t num_bytes = bitset_packed_size_bytes(num_bits);
    std::memset(dest_ptr, 0, num_bytes);
    constexpr size_t block_bytes = bm::gap_max_bits / 8;
    const auto& blocks = bv.get_blocks_manager();
    for (size_t block = 0; block * block_bytes < num_bytes; ++block) {
        unsigned i, j;
        bm::get_block_coord(bm::bvector<>::block_idx_type(block), i, j);
        // Full blocks come back as FULL_BLOCK_REAL_ADDR, a block of all ones
        const bm::word_t* bits = blocks.get_block(i, j);
        if (!bits)
            continue;
        uint8_t* dest = dest_ptr + block * block_bytes;
        const size_t dest_bits = std::min(block_bytes, num_bytes - block * block_bytes) * 8;
        if (BM_IS_GAP(bits)) {
            // A GAP block is a list of run ends, alternating clear and set from the value of the first run
            const bm::gap_word_t* gap = BMGAP_PTR(bits);
            bool set = (gap[0] & 1) != 0;
            size_t run_start = 0;
            for (unsigned k = 1; k < bm::gap_length(gap) && run_start < dest_bits; ++k) {
                const size_t run_end = gap[k];
                if (set)
                    set_packed_bit_range(dest, run_start, std::min(run_end, dest_bits - 1));
                set = !set;
                run_start = run_end + 1;
            }
        } else {
            std::memcpy(dest, bits, dest_bits / 8);
        }
    }
    if (num_bits % 8 != 0)
        dest_ptr[num_bytes - 1] &= static_cast<uint8_t>((1u << (num_bits % 8)) - 1);
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

// Benchmarked ~13x faster than branchless bit-by-bit assignment.
// Each loop iteration reads 8 contiguous bytes and writes 1 output byte with no cross-iteration
// dependencies, which allows the compiler to auto-vectorize.
void bools_to_packed_bits(const bool* src, size_t num_bools, uint8_t* dest) {
    const size_t num_full_bytes = num_bools / 8;
    auto as_byte = reinterpret_cast<const uint8_t*>(src);
    for (size_t i = 0; i < num_full_bytes; ++i) {
        const uint8_t* b = as_byte + i * 8;
        // The bool* src may have been `reinterpret_cast`-ed from uint8_t* (which could contain values > 1).
        // So we need to `static_cast<bool>` or `!=0` and `!=0` was benchmarked to be 3% faster.
        dest[i] = (b[0] != 0) | ((b[1] != 0) << 1) | ((b[2] != 0) << 2) | ((b[3] != 0) << 3) | ((b[4] != 0) << 4) |
                  ((b[5] != 0) << 5) | ((b[6] != 0) << 6) | ((b[7] != 0) << 7);
    }
    for (size_t i = num_full_bytes * 8; i < num_bools; ++i) {
        set_bit_at(dest, i, src[i]);
    }
}

bool get_bit_at(const uint8_t* packed_bits, size_t bit_pos) {
    auto dv = std::div(bit_pos, 8);
    size_t byte_idx = dv.quot;
    size_t bit_idx = dv.rem;
    return (packed_bits[byte_idx] >> bit_idx) & 1;
}

void set_bit_at(uint8_t* packed_bits, size_t bit_pos, bool value) {
    auto dv = std::div(bit_pos, 8);
    size_t byte_idx = dv.quot;
    size_t bit_idx = dv.rem;
    packed_bits[byte_idx] &= ~(1 << bit_idx);                        // Unset bit
    packed_bits[byte_idx] |= static_cast<uint8_t>(value) << bit_idx; // Set bit
}

void copy_packed_bits(const uint8_t* src, size_t src_bit_offset, size_t num_bits, uint8_t* dest) {
    if (src_bit_offset % 8 == 0) {
        memcpy(dest, src + src_bit_offset / 8, bitset_packed_size_bytes(num_bits));
    } else {
        for (size_t i = 0; i < num_bits; ++i) {
            set_bit_at(dest, i, get_bit_at(src, src_bit_offset + i));
        }
    }
}

} // namespace arcticdb
