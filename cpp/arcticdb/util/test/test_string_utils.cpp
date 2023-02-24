/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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
