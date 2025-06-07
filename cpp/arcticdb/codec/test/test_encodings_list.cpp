#include <gtest/gtest.h>
#include <arcticdb/codec/scanner.hpp>

namespace arcticdb {
TEST(EncodingsListTest, SetAndCheck) {
    EncodingsList enc;
    enc.set(EncodingType::DELTA);
    EXPECT_TRUE(enc.is_set(EncodingType::DELTA));
    enc.unset(EncodingType::DELTA);
    EXPECT_FALSE(enc.is_set(EncodingType::DELTA));
}

TEST(EncodingsListTest, InitializerList) {
    EncodingsList enc {EncodingType::BITPACK, EncodingType::DELTA, EncodingType::FFOR};
    EXPECT_TRUE(enc.is_set(EncodingType::BITPACK));
    EXPECT_FALSE(enc.is_set(EncodingType::CONSTANT));
    EXPECT_TRUE(enc.is_set(EncodingType::DELTA));
    EXPECT_TRUE(enc.is_set(EncodingType::FFOR));
    EXPECT_FALSE(enc.is_set(EncodingType::FREQUENCY));
    EXPECT_FALSE(enc.is_set(EncodingType::ALP));
    EXPECT_FALSE(enc.is_set(EncodingType::RLE));
}

TEST(EncodingsListTest, PossibleEncodingsSequence) {
    auto enc = possible_encodings(DataType::UTF_DYNAMIC64);
    EXPECT_TRUE(enc.is_set(EncodingType::CONSTANT));
    EXPECT_TRUE(enc.is_set(EncodingType::DELTA));
    EXPECT_TRUE(enc.is_set(EncodingType::FREQUENCY));
    EXPECT_TRUE(enc.is_set(EncodingType::FFOR));
    EXPECT_FALSE(enc.is_set(EncodingType::BITPACK));
    EXPECT_FALSE(enc.is_set(EncodingType::ALP));
}

TEST(EncodingsListTest, PossibleEncodingsInteger) {
    auto enc = possible_encodings(DataType::UINT32);
    EXPECT_TRUE(enc.is_set(EncodingType::BITPACK));
    EXPECT_TRUE(enc.is_set(EncodingType::CONSTANT));
    EXPECT_TRUE(enc.is_set(EncodingType::DELTA));
    EXPECT_TRUE(enc.is_set(EncodingType::FFOR));
    EXPECT_TRUE(enc.is_set(EncodingType::FREQUENCY));
    EXPECT_FALSE(enc.is_set(EncodingType::ALP));
    EXPECT_FALSE(enc.is_set(EncodingType::RLE));
}

TEST(EncodingsListTest, PossibleEncodingsTime) {
    auto enc = possible_encodings(DataType::NANOSECONDS_UTC64);
    EXPECT_TRUE(enc.is_set(EncodingType::BITPACK));
    EXPECT_TRUE(enc.is_set(EncodingType::CONSTANT));
    EXPECT_TRUE(enc.is_set(EncodingType::DELTA));
    EXPECT_TRUE(enc.is_set(EncodingType::FFOR));
    EXPECT_TRUE(enc.is_set(EncodingType::FREQUENCY));
    EXPECT_FALSE(enc.is_set(EncodingType::ALP));
    EXPECT_FALSE(enc.is_set(EncodingType::RLE));
}

TEST(EncodingsListTest, PossibleEncodingsFloatingPoint) {
    auto enc = possible_encodings(DataType::FLOAT32);
    EXPECT_TRUE(enc.is_set(EncodingType::CONSTANT));
    EXPECT_TRUE(enc.is_set(EncodingType::ALP));
    EXPECT_FALSE(enc.is_set(EncodingType::BITPACK));
    EXPECT_FALSE(enc.is_set(EncodingType::DELTA));
    EXPECT_FALSE(enc.is_set(EncodingType::FFOR));
    EXPECT_FALSE(enc.is_set(EncodingType::FREQUENCY));
}

TEST(EncodingsListTest, PossibleEncodingsUnknown) {
    EXPECT_THROW(possible_encodings(DataType::UNKNOWN), std::runtime_error);
}
} //namespace arcticdb