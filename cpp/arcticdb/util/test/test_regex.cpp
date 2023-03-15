/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/regex_filter.hpp>

TEST(Regex, Simple) {
    using namespace arcticdb;

    const std::string match_text = "Hello World";
    const std::string no_match = "Bingybongy";
    util::RegexPattern pattern{".*?o Wor.*"};
    util::Regex regex(pattern);
    ASSERT_EQ(regex.match(match_text), true);
    ASSERT_EQ(regex.match(no_match), false);
}

TEST(Regex, Complex) {
    using namespace arcticdb;
    std::string match_text = "PANEURO_MACRO/maquity/bondlike/predictors/fidiv_3";
    std::string no_match = "Ubi solitudinem faciunt pacem appellant";
    util::RegexPattern pattern("bondlike/predictors/(?P<pname>.*_\\d+|combined_signal)");
    util::Regex regex(pattern);
    ASSERT_EQ(regex.match(match_text), true);
    ASSERT_EQ(regex.match(no_match), false);
}

