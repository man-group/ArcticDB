/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/string_utils.hpp>

TEST(StringUtils, SafeEncodeNoSpecials) {
    using namespace arcticdb;
    std::string simple("testwithnospecialchars");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeSpecial) {
    using namespace arcticdb;
    std::string simple("testwith/slash");
    auto enc = util::safe_encode(simple);
    ASSERT_EQ(enc, "testwith~2Fslash");
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeEscapeChar) {
    using namespace arcticdb;
    std::string simple("testwith~escapechar");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeEncodeCharEnd) {
    using namespace arcticdb;
    std::string simple("testwith/");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeEncodeCharStartEnd) {
    using namespace arcticdb;
    std::string simple("/testwithboth/");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeMultiple) {
    using namespace arcticdb;
    std::string simple("~test~with");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeMixed) {
    using namespace arcticdb;
    std::string simple("~test~with/andstuff/");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, SafeEncodeMixedReverse) {
    using namespace arcticdb;
    std::string simple("/test~with/andstuff~");
    auto enc = util::safe_encode(simple);
    auto dec = util::safe_decode(enc);
    ASSERT_EQ(simple, dec);
}

TEST(StringUtils, StripAsciiPaddingRemovesTrailingNulls) {
    using namespace arcticdb;
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("ab\0\0", 4)), "ab");
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("ab", 2)), "ab");
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("", 0)), "");
}

TEST(StringUtils, StripAsciiPaddingKeepsInteriorAndLeadingNulls) {
    using namespace arcticdb;
    // Only the trailing run is padding. Interior and leading nulls survive a numpy round trip, so
    // they are data, and stripping them would make a stat compare unequal to the value it came from.
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("a\0b\0", 4)), std::string_view("a\0b", 3));
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("\0ab\0", 4)), std::string_view("\0ab", 3));
}

TEST(StringUtils, StripAsciiPaddingOfAllNullsIsEmpty) {
    using namespace arcticdb;
    ASSERT_EQ(util::strip_ascii_padding(std::string_view("\0\0\0\0", 4)), "");
}

namespace {
// A fixed-width UTF pool entry as the string pool holds it: UCS-4, null padded out to the column width.
std::string padded_utf32(std::string_view utf8, size_t width) {
    auto utf32 = arcticdb::util::utf8_to_u32(utf8);
    utf32.resize(width, char32_t{0});
    return {reinterpret_cast<const char*>(utf32.data()), utf32.size() * sizeof(char32_t)};
}
} // namespace

TEST(StringUtils, Utf32ToU8StripsTrailingPadding) {
    using namespace arcticdb;
    ASSERT_EQ(util::utf32_to_u8(padded_utf32("ab", 8)), "ab");
    ASSERT_EQ(util::utf32_to_u8(padded_utf32("abcd", 4)), "abcd");
    ASSERT_EQ(util::utf32_to_u8(padded_utf32("", 4)), "");
}

TEST(StringUtils, Utf32ToU8KeepsInteriorNullCodepoint) {
    using namespace arcticdb;
    // A null codepoint before a non-null one is data, not padding: numpy preserves it through a round
    // trip and the engine's equality path matches on it. Truncating here silently drops the tail.
    const std::string with_interior_null{"a\0b", 3};
    ASSERT_EQ(util::utf32_to_u8(padded_utf32(with_interior_null, 8)), with_interior_null);
}

TEST(StringUtils, Utf32ToU8KeepsLeadingNullCodepoint) {
    using namespace arcticdb;
    // Worse than the interior case: truncating at the first null yields the empty string, so the value
    // loses every character it had.
    const std::string with_leading_null{"\0ab", 3};
    ASSERT_EQ(util::utf32_to_u8(padded_utf32(with_leading_null, 8)), with_leading_null);
}

TEST(StringUtils, Utf32ToU8KeepsCodepointWhoseHighBytesAreZero) {
    using namespace arcticdb;
    // U+00E9 is 'e9 00 00 00' little endian, so three of its four bytes are zero. Stripping must work
    // in whole codepoints, not bytes, or this vanishes.
    ASSERT_EQ(util::utf32_to_u8(padded_utf32("\xC3\xA9", 4)), "\xC3\xA9");
}
