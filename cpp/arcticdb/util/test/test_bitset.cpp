/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/bitset.hpp>

#include <bitmagic/bmserial.h>

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

TEST(BoolsToPacked, NonCanonicalTruthyTail) {
    // The bools past the last full byte must also pack a truthy byte other than 1 to a single set bit
    uint8_t src[] = {0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 37, 0x80, 0};
    uint8_t dest[2] = {0, 0xFF};
    bools_to_packed_bits(reinterpret_cast<const bool*>(src), 13, dest);
    EXPECT_EQ(dest[0], 0);
    EXPECT_EQ(dest[1] & 0x1F, 0b01101);
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

namespace {

std::vector<uint8_t> packed_from_bitset_naive(const bm::bvector<>& bv) {
    std::vector<uint8_t> packed(bitset_packed_size_bytes(bv.size()), 0);
    for (size_t i = 0; i < bv.size(); ++i) {
        set_bit_at(packed.data(), i, bv.test(bv_size(i)));
    }
    return packed;
}

// Sizes straddle BitMagic's 65536-bit blocks; the patterns produce empty, full, GAP and bit blocks.
std::vector<bm::bvector<>> block_layout_bitsets() {
    std::mt19937_64 rng(7);
    std::vector<bm::bvector<>> bitsets;
    for (const size_t n : {1u, 7u, 8u, 9u, 63u, 65'535u, 65'536u, 65'537u, 131'085u, 300'001u}) {
        for (int pattern = 0; pattern < 6; ++pattern) {
            bm::bvector<> bv;
            bv.resize(bv_size(n));
            if (pattern == 1) {
                bv.set_range(0, bv_size(n - 1));
            } else if (pattern == 2 && n > 10) {
                bv.set_range(3, bv_size(n / 2));
            } else if (pattern == 3) {
                for (size_t i = 0; i < n; i += 3)
                    bv.set(bv_size(i));
            } else if (pattern == 4) {
                for (size_t i = 0; i < n; ++i)
                    if (rng() % 1000 == 0)
                        bv.set(bv_size(i));
            } else if (pattern == 5) {
                bv.set_range(0, bv_size(n - 1));
                for (size_t i = 0; i < n; i += 977)
                    bv.set(bv_size(i), false);
            }
            bv.optimize();
            // A deserialized bitset is what reads see, and may lay its blocks out differently
            bm::serializer<bm::bvector<>> serializer;
            bm::serializer<bm::bvector<>>::buffer buffer;
            serializer.serialize(bv, buffer);
            bm::bvector<> deserialized;
            bm::deserialize(deserialized, buffer.buf());
            deserialized.resize(bv_size(n));
            bitsets.push_back(std::move(bv));
            bitsets.push_back(std::move(deserialized));
        }
    }
    return bitsets;
}

} // namespace

TEST(BitsetToPackedBits, BlockLayoutsAgainstNaive) {
    for (const auto& bv : block_layout_bitsets()) {
        // Pre-filled so a byte the export forgets to write shows up as a mismatch
        std::vector<uint8_t> packed(bitset_packed_size_bytes(bv.size()), 0xAB);
        bitset_to_packed_bits(bv, packed.data());
        ASSERT_EQ(packed, packed_from_bitset_naive(bv)) << "size=" << bv.size() << " count=" << bv.count();
    }
}

TEST(BitsetToPackedBits, SizeBeyondLastAllocatedBlock) {
    bm::bvector<> bv;
    bv.set(5);
    bv.resize(200'003);
    std::vector<uint8_t> packed(bitset_packed_size_bytes(bv.size()), 0xAB);
    bitset_to_packed_bits(bv, packed.data());
    EXPECT_EQ(packed, packed_from_bitset_naive(bv));
}

TEST(BitsetToPackedBits, ExhaustiveSmallSizes) {
    std::mt19937 rng(42);
    for (size_t n = 0; n <= 130; ++n) {
        bm::bvector<> bv;
        bv.resize(bv_size(n));
        for (size_t i = 0; i < n; ++i)
            bv.set(bv_size(i), (rng() & 1) != 0);
        std::vector<uint8_t> packed(bitset_packed_size_bytes(n), 0xAB);
        bitset_to_packed_bits(bv, packed.data());
        ASSERT_EQ(packed, packed_from_bitset_naive(bv)) << "n=" << n;
    }
}

namespace {

// Reference: walk the validity bits, taking the next dense value at each set bit and writing zero at each unset one
std::vector<uint8_t> scatter_naive(
        const std::vector<uint8_t>& values, const std::vector<uint8_t>& validity, size_t num_bits
) {
    std::vector<uint8_t> packed(bitset_packed_size_bytes(num_bits), 0);
    size_t next = 0;
    for (size_t i = 0; i < num_bits; ++i) {
        if (get_bit_at(validity.data(), i)) {
            set_bit_at(packed.data(), i, values[next++] != 0);
        }
    }
    return packed;
}

void check_scatter_matches_naive(const bm::bvector<>& bv, std::mt19937_64& rng) {
    const size_t num_bits = bv.size();
    std::vector<uint8_t> validity(bitset_packed_size_bytes(num_bits));
    bitset_to_packed_bits(bv, validity.data());
    // Truthy bytes other than 1 must still pack to 1
    std::vector<uint8_t> values(bv.count());
    for (auto& value : values) {
        value = static_cast<uint8_t>(rng() % 3 == 0 ? 0 : 1 + rng() % 255);
    }
    std::vector<uint8_t> packed(bitset_packed_size_bytes(num_bits), 0xAB);
    scatter_bools_to_packed_bits(
            reinterpret_cast<const bool*>(values.data()), values.size(), validity.data(), num_bits, packed.data()
    );
    ASSERT_EQ(packed, scatter_naive(values, validity, num_bits)) << "size=" << num_bits << " count=" << values.size();
}

} // namespace

TEST(ScatterBoolsToPackedBits, BlockLayoutsAgainstNaive) {
    std::mt19937_64 rng(11);
    for (const auto& bv : block_layout_bitsets()) {
        check_scatter_matches_naive(bv, rng);
    }
}

TEST(ScatterBoolsToPackedBits, ExhaustiveSmallSizes) {
    std::mt19937_64 rng(42);
    for (size_t n = 0; n <= 130; ++n) {
        for (const unsigned density : {0u, 1u, 2u, 3u}) {
            bm::bvector<> bv;
            bv.resize(bv_size(n));
            for (size_t i = 0; i < n; ++i) {
                // density 0: none set, 3: all set, otherwise random
                bv.set(bv_size(i), density == 3 || (density != 0 && rng() % (density + 1) != 0));
            }
            check_scatter_matches_naive(bv, rng);
        }
    }
}

TEST(ScatterBoolsToPackedBits, IgnoresValidityBitsPastTheEnd) {
    const std::vector<uint8_t> validity{0xFF, 0xFF};
    const bool values[] = {true, false, true, true, false, true, true, false, true, true};
    std::vector<uint8_t> packed(2, 0xAB);
    scatter_bools_to_packed_bits(values, 10, validity.data(), 10, packed.data());
    EXPECT_EQ(packed[0], 0b01101101);
    EXPECT_EQ(packed[1], 0b11);
}

TEST(ScatterBoolsToPackedBits, ThrowsOnCountMismatch) {
    const std::vector<uint8_t> validity{0b00000111};
    const bool values[] = {true, true};
    std::vector<uint8_t> packed(1);
    EXPECT_ANY_THROW(scatter_bools_to_packed_bits(values, 2, validity.data(), 8, packed.data()));
}

namespace {

std::vector<uint8_t> range_packed_naive(const bm::bvector<>& bv, size_t start, size_t end) {
    std::vector<uint8_t> packed(bitset_packed_size_bytes(end - start), 0);
    for (size_t i = start; i < end; ++i) {
        set_bit_at(packed.data(), i - start, bv.test(bv_size(i)));
    }
    return packed;
}

void check_range_matches_naive(const bm::bvector<>& bv, size_t start, size_t end) {
    // Sized exactly and pre-filled, so an overrun is a heap overflow and a skipped byte is a mismatch
    std::vector<uint8_t> packed(bitset_packed_size_bytes(end - start), 0xAB);
    bitset_range_to_packed_bits(bv, start, end, packed.data());
    ASSERT_EQ(packed, range_packed_naive(bv, start, end))
            << "size=" << bv.size() << " start=" << start << " end=" << end;
}

} // namespace

TEST(BitsetRangeToPackedBits, ExhaustiveSmallRanges) {
    std::mt19937 rng(42);
    constexpr size_t n = 140;
    for (int pattern = 0; pattern < 4; ++pattern) {
        bm::bvector<> bv;
        bv.resize(bv_size(n));
        for (size_t i = 0; i < n; ++i) {
            bv.set(bv_size(i), pattern == 1 || (pattern == 2 && i % 2 == 0) || (pattern == 3 && (rng() & 1) != 0));
        }
        for (size_t start = 0; start <= n; ++start) {
            for (size_t end = start; end <= n; ++end) {
                check_range_matches_naive(bv, start, end);
            }
        }
    }
}

TEST(BitsetRangeToPackedBits, BlockLayoutsAgainstNaive) {
    constexpr size_t block_bits = bm::gap_max_bits;
    for (const auto& bv : block_layout_bitsets()) {
        const size_t n = bv.size();
        // Aligned and unaligned starts, and ranges that begin, end or cross at a block boundary
        for (const size_t start :
             {size_t{0}, size_t{3}, size_t{8}, size_t{13}, block_bits - 5, block_bits, block_bits + 8, n / 2, n - 1, n
             }) {
            for (const size_t length : {size_t{0}, size_t{1}, size_t{9}, size_t{100'000}, n}) {
                if (start <= n) {
                    check_range_matches_naive(bv, start, std::min(n, start + length));
                }
            }
        }
    }
}
