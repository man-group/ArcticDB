/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/bitset.hpp>

#include <random>
#include <vector>

using namespace arcticdb;

TEST(BoolsToPacked, StandardValues) {
    // 9 bools: 1 full byte + 1 bit remainder
    bool src[] = {true, false, true, true, false, false, true, false, true};
    uint8_t dest[2];
    bools_to_packed_bits(src, 9, dest);
    EXPECT_EQ(dest[0], 0b01001101);
    EXPECT_EQ(dest[1] % 2, 0b1);
}

TEST(BoolsToPacked, NonCanonicalTruthyValues) {
    // Bool bytes with non-1 true values (e.g. the byte 0b00000010 is still true).
    // We encode and decode as uint8s so theoretically we could end up with bool = 2.
    // We should still decode 2 to a 1 in the correct place in the packed bits.
    uint8_t src[] = {2, 0, 4, 0, 8, 0, 16, 0, 0x80, 0xFF, 3, 0, 0, 0, 7, 42};
    auto* bool_src = reinterpret_cast<const bool*>(src);
    uint8_t dest[2];
    bools_to_packed_bits(bool_src, 16, dest);
    // True positions: 0,2,4,6,8,9,10,14,15
    EXPECT_EQ(dest[0], 0b01010101);
    EXPECT_EQ(dest[1], 0b11000111);
}

TEST(BoolsToPacked, AllZero) {
    bool src[16] = {};
    memset(src, 0, 16);
    uint8_t dest[2];
    bools_to_packed_bits(src, 16, dest);
    EXPECT_EQ(dest[0], 0b00000000);
    EXPECT_EQ(dest[1], 0b00000000);
}

TEST(BoolsToPacked, AllOne) {
    bool src[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    uint8_t dest[2];
    bools_to_packed_bits(src, 16, dest);
    EXPECT_EQ(dest[0], 0b11111111);
    EXPECT_EQ(dest[1], 0b11111111);
}

TEST(BoolsToPacked, NonAligned) {
    // 3 bools: only 3 bits are defined in the output byte
    bool src[] = {true, false, true};
    uint8_t dest[1];
    bools_to_packed_bits(src, 3, dest);
    EXPECT_EQ(dest[0] % 8, 0b101);
}

namespace {

// The buffer is sized to exactly the bytes holding [bit_offset, bit_offset + num_bits), so a scan that reads past
// the last relevant byte is a heap overflow that sanitizer builds will catch.
std::vector<uint8_t> packed_from_bools(const std::vector<bool>& bits) {
    std::vector<uint8_t> packed(bitset_packed_size_bytes(bits.size()), 0);
    for (size_t i = 0; i < bits.size(); ++i) {
        set_bit_at(packed.data(), i, bits[i]);
    }
    return packed;
}

std::vector<size_t> unset_positions_naive(const std::vector<uint8_t>& packed, size_t bit_offset, size_t num_bits) {
    std::vector<size_t> positions;
    for (size_t i = 0; i < num_bits; ++i) {
        if (!get_bit_at(packed.data(), bit_offset + i)) {
            positions.push_back(i);
        }
    }
    return positions;
}

std::vector<size_t> unset_positions_scan(const std::vector<uint8_t>& packed, size_t bit_offset, size_t num_bits) {
    std::vector<size_t> positions;
    for_each_unset_bit(packed.data(), bit_offset, num_bits, [&](size_t i) { positions.push_back(i); });
    return positions;
}

void check_matches_naive(const std::vector<bool>& bits, size_t bit_offset, size_t num_bits) {
    const auto packed = packed_from_bools(bits);
    ASSERT_EQ(unset_positions_scan(packed, bit_offset, num_bits), unset_positions_naive(packed, bit_offset, num_bits))
            << "bit_offset=" << bit_offset << " num_bits=" << num_bits;
}

} // namespace

TEST(ForEachUnsetBit, Empty) {
    const std::vector<uint8_t> packed{0x00, 0xFF};
    size_t calls = 0;
    for_each_unset_bit(packed.data(), 3, 0, [&](size_t) { ++calls; });
    EXPECT_EQ(calls, 0u);
}

TEST(ForEachUnsetBit, KnownValues) {
    // 0b10110100, 0b00001111 -> set positions 2, 4, 5, 7, 8, 9, 10, 11
    const std::vector<uint8_t> packed{0b10110100, 0b00001111};
    EXPECT_EQ(unset_positions_scan(packed, 0, 16), (std::vector<size_t>{0, 1, 3, 6, 12, 13, 14, 15}));
    // Starting at bit 3 shifts every position down by 3 and drops those before it
    EXPECT_EQ(unset_positions_scan(packed, 3, 13), (std::vector<size_t>{0, 3, 9, 10, 11, 12}));
    EXPECT_EQ(unset_positions_scan(packed, 6, 2), (std::vector<size_t>{0}));
}

// Exhaustive over every length up to two words and every start bit spanning nine bytes, so every combination of
// head shift, whole-word body and partial tail is covered for each pattern.
TEST(ForEachUnsetBit, ExhaustiveAgainstNaive) {
    constexpr size_t max_num_bits = 130;
    constexpr size_t max_bit_offset = 71;
    std::mt19937 rng(42);
    for (size_t bit_offset = 0; bit_offset <= max_bit_offset; ++bit_offset) {
        for (size_t num_bits = 0; num_bits <= max_num_bits; ++num_bits) {
            const size_t total = bit_offset + num_bits;
            std::vector<bool> all_set(total, true);
            std::vector<bool> all_unset(total, false);
            std::vector<bool> alternating(total);
            std::vector<bool> random_bits(total);
            for (size_t i = 0; i < total; ++i) {
                alternating[i] = (i % 2) == 0;
                random_bits[i] = (rng() & 1) != 0;
            }
            check_matches_naive(all_set, bit_offset, num_bits);
            check_matches_naive(all_unset, bit_offset, num_bits);
            check_matches_naive(alternating, bit_offset, num_bits);
            check_matches_naive(random_bits, bit_offset, num_bits);
        }
    }
}

// Sparse patterns at sizes well past the exhaustive range, matching the shapes seen in Arrow validity bitmaps.
TEST(ForEachUnsetBit, RandomLargeAgainstNaive) {
    std::mt19937 rng(1337);
    for (const size_t num_bits : {1u, 63u, 64u, 65u, 127u, 128u, 1000u, 4096u, 10'007u}) {
        for (const double set_probability : {0.0, 0.01, 0.5, 0.99, 1.0}) {
            for (size_t bit_offset = 0; bit_offset < 8; ++bit_offset) {
                std::bernoulli_distribution dist(set_probability);
                std::vector<bool> bits(bit_offset + num_bits);
                for (size_t i = 0; i < bits.size(); ++i) {
                    bits[i] = dist(rng);
                }
                check_matches_naive(bits, bit_offset, num_bits);
            }
        }
    }
}
