#include <gtest/gtest.h>
#include "util/decimal.hpp"

TEST(DecimalConstructor, DefaultConstruct) {
    arcticdb::util::Decimal d;
    ASSERT_EQ(d.to_string(0), "0");
}

TEST(DecimalConstructor, FromDecimalString) {
    EXPECT_EQ(arcticdb::util::Decimal("1.23").to_string(0), "123");
    EXPECT_EQ(arcticdb::util::Decimal("0.00123").to_string(0), "123");
    EXPECT_EQ(arcticdb::util::Decimal("100.0023").to_string(0), "1000023");
    EXPECT_EQ(arcticdb::util::Decimal("123.40000").to_string(0), "1234");
    EXPECT_EQ(arcticdb::util::Decimal(".123").to_string(0), "123");
}

TEST(DecimalConstructor, ScientificNotationPositiveExponent) {
    EXPECT_EQ(arcticdb::util::Decimal("1E10").to_string(0), "10000000000");
    EXPECT_EQ(arcticdb::util::Decimal("12.3456E2").to_string(0), "123456");
    EXPECT_EQ(arcticdb::util::Decimal("12.3456E4").to_string(0), "123456");
    EXPECT_EQ(arcticdb::util::Decimal("12.3456E5").to_string(0), "1234560");
    EXPECT_EQ(arcticdb::util::Decimal("123.40000E2").to_string(0), "12340");
    EXPECT_EQ(arcticdb::util::Decimal("-123.456E2").to_string(0), "-123456");
}

TEST(DecimalConstructor, ScientificNotationNegativeExponent) {
    EXPECT_EQ(arcticdb::util::Decimal("12.345E-1").to_string(0), "12345");
    EXPECT_EQ(arcticdb::util::Decimal("12.345E-10").to_string(0), "12345");
}

TEST(DecimalConstructor, Zero) {
    EXPECT_EQ(arcticdb::util::Decimal("0").to_string(0), "0");
    EXPECT_EQ(arcticdb::util::Decimal("00000").to_string(0), "0");
}

TEST(DecimalConstructor, OneWordUnsigned) {
    arcticdb::util::Decimal d("1234");
    ASSERT_EQ(d.to_string(0), "1234");
}

TEST(DecimalConstructor, LargestOneWordUnsigned) {
    arcticdb::util::Decimal d("999999999999999999");
    ASSERT_EQ(d.to_string(0), "999999999999999999");
}

TEST(DecimalConstructor, SmallestTwoWordUnsigned) {
    ASSERT_EQ(arcticdb::util::Decimal("1000000000000000000").to_string(0), "1000000000000000000");
}

TEST(DecimalConstructor, LargestUnsigned) {
    arcticdb::util::Decimal d("99999999999999999999999999999999999999");
    ASSERT_EQ(d.to_string(0), "99999999999999999999999999999999999999");
}

TEST(Decimal, PositiveScale) {
    arcticdb::util::Decimal d("12345");
    ASSERT_EQ(d.to_string(2), "123.45");
    ASSERT_EQ(d.to_string(8), "0.00012345");
}

TEST(Decimal, NegativeScale) {
    arcticdb::util::Decimal d("12345");
    ASSERT_EQ(d.to_string(-2), "1234500");
}

TEST(Decimal, NegativeNumber) {
    arcticdb::util::Decimal d("-123");
    ASSERT_TRUE(d.is_negative());
    EXPECT_EQ(d.to_string(0), "-123");
    EXPECT_EQ(d.to_string(3), "-0.123");
    EXPECT_EQ(d.to_string(1), "-12.3");
    EXPECT_EQ(d.to_string(-3), "-123000");
}

TEST(Decimal, InvalidInput) {
    EXPECT_ANY_THROW(arcticdb::util::Decimal("123a3"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("0.123E1E2"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("123.123.3"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("E123"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("111111111111111111111111111111111111111"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("1E38"));
    EXPECT_ANY_THROW(arcticdb::util::Decimal("12.43E1a"));
}

TEST(Decimal, Compare) {
    EXPECT_EQ(arcticdb::util::Decimal("123"), arcticdb::util::Decimal("123"));
    EXPECT_EQ(arcticdb::util::Decimal("123456789101112131415"), arcticdb::util::Decimal("123456789101112131415"));

    EXPECT_EQ(arcticdb::util::Decimal("-123"), arcticdb::util::Decimal("-123"));
    EXPECT_EQ(arcticdb::util::Decimal("-123456789101112131415"), arcticdb::util::Decimal("-123456789101112131415"));

    EXPECT_FALSE(arcticdb::util::Decimal("123") == arcticdb::util::Decimal("1230"));
    EXPECT_FALSE(arcticdb::util::Decimal("123456789101112131415") == arcticdb::util::Decimal("12345678910111213141"));
}