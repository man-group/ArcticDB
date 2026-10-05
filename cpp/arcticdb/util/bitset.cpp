/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/bitset.hpp>

#include <array>
#include <vector>

namespace arcticdb {

void bitset_to_packed_bits(const bm::bvector<>& bv, uint8_t* dest_ptr) {
    // A BitMagic bit block holds its bits least-significant first, which on a little-endian host is byte for byte the
    // packed layout, so each block is copied whole rather than enumerating its set bits.
    static_assert(std::endian::native == std::endian::little, "Block copy assumes a little-endian host");
    const size_t num_bits = bv.size();
    const size_t num_bytes = bitset_packed_size_bytes(num_bits);
    constexpr size_t block_bytes = bm::gap_max_bits / 8;
    const auto& blocks = bv.get_blocks_manager();
    alignas(16) bm::word_t gap_scratch[bm::set_block_size];
    for (size_t block_idx = 0, byte_offset = 0; byte_offset < num_bytes; ++block_idx, byte_offset += block_bytes) {
        const size_t len = std::min(block_bytes, num_bytes - byte_offset);
        unsigned i, j;
        bm::get_block_coord(static_cast<bm::bvector<>::block_idx_type>(block_idx), i, j);
        // get_block returns nullptr for an all-zero block and a real all-ones block for a full one
        const bm::word_t* block = blocks.get_block(i, j);
        if (block == nullptr) {
            std::memset(dest_ptr + byte_offset, 0, len);
        } else if (BM_IS_GAP(block)) {
            bm::gap_convert_to_bitset(gap_scratch, BMGAP_PTR(block));
            std::memcpy(dest_ptr + byte_offset, gap_scratch, len);
        } else {
            std::memcpy(dest_ptr + byte_offset, block, len);
        }
    }
    if (const size_t tail_bits = num_bits % 8; tail_bits != 0) {
        dest_ptr[num_bytes - 1] &= static_cast<uint8_t>((1u << tail_bits) - 1);
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
        set_bit_at(dest, i, as_byte[i] != 0);
    }
}

namespace {
// deposit_nibble[mask][bits] places the low popcount(mask) bits of bits at the set positions of the 4-bit mask.
constexpr auto deposit_nibble = [] {
    std::array<std::array<uint8_t, 16>, 16> table{};
    for (unsigned mask = 0; mask < 16; ++mask) {
        for (unsigned bits = 0; bits < 16; ++bits) {
            unsigned out = 0;
            unsigned next = 0;
            for (unsigned pos = 0; pos < 4; ++pos) {
                if ((mask >> pos) & 1u) {
                    out |= ((bits >> next++) & 1u) << pos;
                }
            }
            table[mask][bits] = static_cast<uint8_t>(out);
        }
    }
    return table;
}();
} // namespace

void scatter_bools_to_packed_bits(
        const bool* values, size_t num_values, const uint8_t* validity, size_t num_bits, uint8_t* dest
) {
    // Pack the values first, which vectorises, then deposit them a validity nibble at a time. The 8 bytes of slack let
    // every read of the packed values be one unaligned 64-bit load.
    std::vector<uint8_t> packed_values(bitset_packed_size_bytes(num_values) + 8, 0);
    bools_to_packed_bits(values, num_values, packed_values.data());
    const size_t num_bytes = bitset_packed_size_bytes(num_bits);
    size_t value_pos = 0;
    for (size_t byte_idx = 0; byte_idx < num_bytes; ++byte_idx) {
        unsigned mask = validity[byte_idx];
        if (byte_idx == num_bytes - 1 && num_bits % 8 != 0) {
            mask &= (1u << (num_bits % 8)) - 1;
        }
        uint64_t window;
        std::memcpy(&window, packed_values.data() + (value_pos >> 3), sizeof(window));
        const auto bits = static_cast<unsigned>(window >> (value_pos & 7));
        const unsigned lo = mask & 0xFu;
        const unsigned hi = mask >> 4;
        const auto lo_count = static_cast<unsigned>(std::popcount(lo));
        dest[byte_idx] = static_cast<uint8_t>(
                deposit_nibble[lo][bits & 0xFu] | (deposit_nibble[hi][(bits >> lo_count) & 0xFu] << 4)
        );
        value_pos += lo_count + static_cast<unsigned>(std::popcount(hi));
    }
    util::check(
            value_pos == num_values, "scatter_bools_to_packed_bits: {} values for {} set bits", num_values, value_pos
    );
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
