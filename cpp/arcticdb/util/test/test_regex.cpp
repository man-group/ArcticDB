/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/regex_filter.hpp>

TEST(Regex, Simple) {
    using namespace arcticdb;

    const std::string match_text = "Hello World";
    const std::string no_match = "Bingybongy";
    util::RegexPatternUTF8 pattern{".*?o Wor.*"};
    util::RegexUTF8 regex(pattern);
    ASSERT_EQ(regex.match(match_text), true);
    ASSERT_EQ(regex.match(no_match), false);
}

TEST(Regex, Complex) {
    using namespace arcticdb;
    std::string match_text = "PANEURO_MACRO/maquity/bondlike/predictors/fidiv_3";
    std::string no_match = "Ubi solitudinem faciunt pacem appellant";
    util::RegexPatternUTF8 pattern("bondlike/predictors/(?P<pname>.*_\\d+|combined_signal)");
    util::RegexUTF8 regex(pattern);
    ASSERT_EQ(regex.match(match_text), true);
    ASSERT_EQ(regex.match(no_match), false);
}

TEST(Regex, Unicode) {
    using namespace arcticdb;

    const std::string match_text = "ɐɑɒɓɔɕɖɗɘəɚ";
    const std::string no_match = "֑ ֒";
    // In MSVC, the cpp file encoding will mess up the utf32 characters below, if not specified in \u....
    const std::u32string u32_match_text =
            U"\u0250\u0251\u0252\u0253\u0254\u0255\u0256\u0257\u0258\u0259\u025A"; // U"ɐɑɒɓɔɕɖɗɘəɚ"
    const std::u32string u32_no_match = U"\u0591\u0020\u0592";                     // U"֑ ֒"
    const std::string original_pattern = "[ɐɚ]";
    const std::u32string u32_original_pattern = U"[\u0250\u025A]"; // U"[ɐɚ]"
    const std::string no_match_pattern = "[ʧʨ]";

    util::RegexPatternUTF8 pattern{original_pattern};
    util::RegexPatternUTF8 non_match_pattern{no_match_pattern};
    util::RegexUTF8 regex(pattern);
    util::RegexUTF8 non_match_regex(non_match_pattern);
    ASSERT_EQ(regex.match(match_text), true);
    ASSERT_EQ(regex.match(no_match), false);
    ASSERT_EQ(non_match_regex.match(match_text), false);
    ASSERT_EQ(non_match_regex.match(no_match), false);

    util::RegexPatternUTF32 u32_pattern{u32_original_pattern};
    util::RegexUTF32 u32_regex(u32_pattern);
    ASSERT_EQ(u32_regex.match(u32_match_text), true);
    ASSERT_EQ(u32_regex.match(u32_no_match), false);

    util::RegexGeneric regex_generic{original_pattern};
    auto regex_utf8 = regex_generic.get_utf8_match_object();
    auto regex_utf32 = regex_generic.get_utf32_match_object();
    ASSERT_EQ(regex_utf32.match(u32_match_text), true);
    ASSERT_EQ(regex_utf32.match(u32_no_match), false);
    ASSERT_EQ(regex_utf8.match(match_text), true);
    ASSERT_EQ(regex_utf8.match(no_match), false);
}