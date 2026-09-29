/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/string_stat_encoding.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/util/test/string_stat_test_utils.hpp>

#include <vector>

namespace arcticdb {

namespace {
// A fixed-width UTF column's string pool holds UTF-32, which reaches the packer as raw bytes.
std::string_view bytes_of(const std::u32string& utf32) {
    return {reinterpret_cast<const char*>(utf32.data()), utf32.size() * sizeof(char32_t)};
}

constexpr std::string_view cjk_utf8{"\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"}; // 日本語, 3 bytes each
constexpr std::string_view emoji_utf8{"\xF0\x9F\x98\x80"};                   // 😀, 4 bytes
} // namespace

TEST(StringStatEncoding, EmptyStringPacksToZero) { ASSERT_EQ(pack_string_stat(""), 0ULL); }

TEST(StringStatEncoding, PrefixInHighBytesLengthInLowByte) {
    ASSERT_EQ(pack_string_stat("a"), 0x6100000000000001ULL);
    ASSERT_EQ(pack_string_stat("ab"), 0x6162000000000002ULL);
    ASSERT_EQ(pack_string_stat("abc"), 0x6162630000000003ULL);
    ASSERT_EQ(pack_string_stat("abcdef"), 0x6162636465660006ULL);
    ASSERT_EQ(pack_string_stat("abcdefg"), 0x6162636465666707ULL);
}

TEST(StringStatEncoding, LongerThanPrefixIsMarkedTruncated) {
    ASSERT_EQ(pack_string_stat("abcdefgh"), 0x61626364656667FFULL);
    // Anything sharing the first seven bytes collapses onto the same stat, which is safe but
    // imprecise: it can only ever widen the recorded min/max range, never narrow it.
    ASSERT_EQ(pack_string_stat("abcdefgz"), pack_string_stat("abcdefgh"));
    ASSERT_EQ(pack_string_stat("abcdefghijklmnop"), pack_string_stat("abcdefgh"));
    // The truncation marker is the largest byte, so an exact seven-byte value sorts below it.
    ASSERT_LT(pack_string_stat("abcdefg"), pack_string_stat("abcdefgh"));
}

// pack_string_stat copies the prefix in one go and byte swaps it into place rather than shifting a
// byte at a time, so this pins the result at every length against the shift the format specifies.
// The bytes are chosen to expose a wrong byte order or a stray byte read past the end of the string.
TEST(StringStatEncoding, PackedPrefixMatchesPerByteShift) {
    const std::string source{"\x01\x7F\x80\xFEqrstuvwx", 12};
    for (size_t length = 0; length <= source.size(); ++length) {
        // Rebuilding the prefix in its own allocation means there are no trailing bytes of source
        // after it to accidentally pick up, so a copy reading too far shows as a mismatch here.
        const std::string exact{std::string_view{source}.substr(0, length)};
        const auto packed = pack_string_stat(exact);

        uint64_t expected{0};
        for (size_t i = 0; i < std::min(length, truncated_prefix_bytes); ++i) {
            expected |= static_cast<uint64_t>(static_cast<uint8_t>(exact[i])) << ((truncated_prefix_bytes - i) * 8);
        }
        expected |= length > truncated_prefix_bytes ? truncated_string_length_marker : length;

        ASSERT_EQ(packed, expected) << "length " << length;
    }
}

TEST(StringStatEncoding, PackingPreservesBytewiseOrder) {
    // The invariant the whole design rests on. Bytewise sorted; packing must be non-decreasing
    // across it. Not strictly increasing, because values sharing a full seven-byte prefix collapse.
    const std::vector<std::string> bytewise_sorted{
            "", "a", "aa", "aaaaaaa", "aaaaaaaa", "aaaaaaaz", "ab", "az", "b", "bbbb", "bbbba", "bbbbbbb", "bbbbbbbz"
    };
    for (size_t i = 1; i < bytewise_sorted.size(); ++i) {
        ASSERT_LT(bytewise_sorted[i - 1], bytewise_sorted[i]) << "test data is not bytewise sorted at " << i;
        ASSERT_LE(pack_string_stat(bytewise_sorted[i - 1]), pack_string_stat(bytewise_sorted[i]))
                << bytewise_sorted[i - 1] << " vs " << bytewise_sorted[i];
    }
}

TEST(StringStatEncoding, ShorterStringWithSmallerFirstByteSortsBelow) {
    // Copying the bytes into the uint64_t rather than shifting them would reverse the prefix on a
    // little-endian host and invert both of these.
    ASSERT_LT(pack_string_stat("az"), pack_string_stat("b"));
    ASSERT_LT(pack_string_stat("abc"), pack_string_stat("b"));
}

TEST(StringStatEncoding, QueryOutsideStoredRangeCompares) {
    const auto min = pack_string_stat("aaaa");
    const auto max = pack_string_stat("bbbb");
    ASSERT_LT(pack_string_stat("aaa"), min);   // sorts below every stored value, so prunable
    ASSERT_GT(pack_string_stat("bbbba"), max); // sorts above every stored value, so prunable
    ASSERT_GE(pack_string_stat("ab"), min);
    ASSERT_LE(pack_string_stat("ab"), max);
    // A truncated max must never prune a query sharing its prefix, whatever follows.
    const auto truncated_max = pack_string_stat("bbbbbbbz");
    ASSERT_EQ(pack_string_stat("bbbbbbbc"), truncated_max);
}

TEST(StringStatEncoding, MultiByteCodepointsPackWithoutSignExtension) {
    // char is signed here, so bytes above 0x7F must be widened as unsigned or the packed value is
    // garbage and non-ASCII min/max is wrong.
    ASSERT_EQ(pack_string_stat(cjk_utf8), 0xE697A5E69CACE8FFULL);
    ASSERT_EQ(pack_string_stat(emoji_utf8), 0xF09F988000000004ULL);
    ASSERT_GT(pack_string_stat(cjk_utf8), pack_string_stat("zzzzzzz"));
}

TEST(StringStatEncoding, TruncationSplittingACodepointIsStable) {
    const auto unpacked = unpack_string(pack_string_stat(cjk_utf8));
    ASSERT_TRUE(unpacked.was_truncated);
    // Nine bytes cut at seven leaves 日本 plus the first byte of 語, which is not valid UTF-8.
    ASSERT_EQ(unpacked.text, cjk_utf8.substr(0, truncated_prefix_bytes));
}

TEST(StringStatEncoding, SplitAtEveryOffsetWithinACodepoint) {
    const std::string emoji{emoji_utf8};
    ASSERT_EQ(pack_string_stat("ab" + emoji), 0x6162F09F98800006ULL);
    ASSERT_EQ(pack_string_stat("abc" + emoji), 0x616263F09F988007ULL); // exactly seven, not truncated
    ASSERT_EQ(pack_string_stat("abcd" + emoji), 0x61626364F09F98FFULL);
    ASSERT_EQ(pack_string_stat("abcde" + emoji), 0x6162636465F09FFFULL);
    ASSERT_EQ(pack_string_stat("abcdef" + emoji), 0x616263646566F0FFULL);
    ASSERT_EQ(pack_string_stat("abcdefg" + emoji), 0x61626364656667FFULL);
    // Order still tracks bytewise order across a split codepoint.
    ASSERT_GT("abc" + emoji, "abcd" + emoji);
    ASSERT_GT(pack_string_stat("abc" + emoji), pack_string_stat("abcd" + emoji));
}

TEST(StringStatEncoding, InvalidUtf8PacksWithoutThrowing) {
    // Packing is bytewise, so it must not validate. Column stats generation cannot be the thing
    // that fails a write of data the rest of the engine accepts.
    ASSERT_EQ(pack_string_stat("\xFF\xFE"), 0xFFFE000000000002ULL);
    ASSERT_NO_THROW(pack_string_stat("\x80"));
    ASSERT_NO_THROW(pack_string_stat("\xE6"));
}

TEST(StringStatEncoding, Utf32SourceTranscodesToTheSamePackedValue) {
    // Makes stats comparable across a dynamic schema symbol whose slices differ in string type.
    const auto utf32 = util::utf8_to_u32(cjk_utf8);
    ASSERT_EQ(pack_string(bytes_of(utf32), DataType::UTF_FIXED64), pack_string(cjk_utf8, DataType::UTF_DYNAMIC64));
    ASSERT_EQ(pack_string(bytes_of(utf32), DataType::UTF_FIXED64), 0xE697A5E69CACE8FFULL);
    // Without the transcode the UTF-32 bytes would pack as themselves, which is a different value.
    ASSERT_NE(pack_string_stat(bytes_of(utf32)), 0xE697A5E69CACE8FFULL);
}

TEST(StringStatEncoding, Utf32EmbeddedNullIsNotATerminator) {
    const std::string with_null{"a\0b", 3};
    ASSERT_EQ(pack_string(bytes_of(util::utf8_to_u32(with_null)), DataType::UTF_FIXED64), pack_string_stat(with_null));
    ASSERT_GT(pack_string(bytes_of(util::utf8_to_u32(with_null)), DataType::UTF_FIXED64), pack_string_stat("a"));
}

TEST(StringStatEncoding, EveryStringDataTypeAgreesOnAsciiText) {
    const auto expected = pack_string_stat("ab");
    ASSERT_EQ(pack_string("ab", DataType::ASCII_FIXED64), expected);
    ASSERT_EQ(pack_string("ab", DataType::ASCII_DYNAMIC64), expected);
    ASSERT_EQ(pack_string("ab", DataType::UTF_DYNAMIC64), expected);
    ASSERT_EQ(pack_string(bytes_of(util::utf8_to_u32("ab")), DataType::UTF_FIXED64), expected);
}

TEST(StringStatEncoding, Utf32PaddingIsNotPacked) {
    // Fixed-width pools pad with nulls. If they reached the packer, "ab" would look like a
    // seven-byte value rather than a two-byte one.
    auto padded = util::utf8_to_u32("ab");
    padded.resize(8, char32_t{0});
    ASSERT_EQ(pack_string(bytes_of(padded), DataType::UTF_FIXED64), pack_string_stat("ab"));
}

TEST(StringStatEncoding, AsciiFixedWidthPaddingIsNotPacked) {
    // An ASCII fixed-width pool pads at one byte per character. The packer must strip that itself
    // rather than rely on the caller, so that a packed stat never depends on how the caller decided
    // how wide a character is.
    std::string padded{"ab"};
    padded.resize(8, '\0');
    ASSERT_EQ(pack_string(padded, DataType::ASCII_FIXED64), pack_string_stat("ab"));
    // Only the padding goes: an interior null is data, exactly as in the UTF-32 case.
    std::string with_null{"a\0b", 3};
    with_null.resize(8, '\0');
    ASSERT_EQ(pack_string(with_null, DataType::ASCII_FIXED64), pack_string_stat(std::string{"a\0b", 3}));
}

TEST(StringStatEncoding, AllPaddingPacksToZero) {
    // A fixed-width column can hold the empty string, which reaches the packer as pure padding.
    ASSERT_EQ(pack_string(std::string(8, '\0'), DataType::ASCII_FIXED64), 0ULL);
    ASSERT_EQ(pack_string(std::string(32, '\0'), DataType::UTF_FIXED64), 0ULL);
}

TEST(StringStatEncoding, UnpackRoundTripsUntruncatedValues) {
    for (const std::string source : {"", "a", "ab", "abcdef", "abcdefg"}) {
        const auto unpacked = unpack_string(pack_string_stat(source));
        ASSERT_EQ(unpacked.text, source);
        ASSERT_FALSE(unpacked.was_truncated) << source;
    }
}

TEST(StringStatEncoding, UnpackReportsTruncation) {
    const auto unpacked = unpack_string(pack_string_stat("abcdefghij"));
    ASSERT_EQ(unpacked.text, "abcdefg");
    ASSERT_TRUE(unpacked.was_truncated);
}

TEST(StringStatEncoding, UnpackClampsImpossibleLengthBytes) {
    // Corrupt or future on-disk bytes must not make unpack read past the prefix, which would shift
    // by a negative amount and be undefined.
    for (const uint64_t bogus_length : {8ULL, 9ULL, 100ULL, 254ULL}) {
        const auto unpacked = unpack_string(0x6162636465666700ULL | bogus_length);
        ASSERT_EQ(unpacked.text, "abcdefg") << bogus_length;
        ASSERT_FALSE(unpacked.was_truncated) << bogus_length;
    }
}

TEST(StringStatEncoding, UnpackZeroGivesEmptyUntruncated) {
    const auto unpacked = unpack_string(0);
    ASSERT_TRUE(unpacked.text.empty());
    ASSERT_FALSE(unpacked.was_truncated);
}

} // namespace arcticdb
