/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/bitset.hpp>

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
