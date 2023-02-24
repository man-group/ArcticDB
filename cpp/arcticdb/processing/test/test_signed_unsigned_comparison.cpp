/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/processing/signed_unsigned_comparison.hpp>
#include <limits>

TEST(CompareSignedUnsigned, LessThan) {
    using namespace arcticdb::comparison;

    auto uint64_max = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(less_than(uint64_max, int64_t{28}), false);
    ASSERT_EQ(less_than(uint64_t{30}, int64_t{28}), false);
    ASSERT_EQ(less_than(uint64_t{28}, int64_t{30}), true);
    ASSERT_EQ(less_than(uint64_t{28}, int64_t{-5}), false);
    ASSERT_EQ(less_than(uint64_t{3}, int64_t{3}), false);
    ASSERT_EQ(less_than(int64_t{28}, uint64_max), true);
    ASSERT_EQ(less_than(int64_t{28}, uint64_t{30}), true);
    ASSERT_EQ(less_than(int64_t{30}, uint64_t{28}), false);
    ASSERT_EQ(less_than(int64_t{-5}, uint64_t{28}), true);
    ASSERT_EQ(less_than(int64_t{3}, uint64_t{3}), false);
}

TEST(CompareSignedUnsigned, GreaterThan) {
    using namespace arcticdb::comparison;

    auto uint64_max = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(greater_than(uint64_max, int64_t{28}), true);
    ASSERT_EQ(greater_than(uint64_t{30}, int64_t{28}), true);
    ASSERT_EQ(greater_than(uint64_t{28}, int64_t{30}), false);
    ASSERT_EQ(greater_than(uint64_t{28}, int64_t{-5}), true);
    ASSERT_EQ(greater_than(uint64_t{3}, int64_t{3}), false);
    ASSERT_EQ(greater_than(int64_t{28}, uint64_max), false);
    ASSERT_EQ(greater_than(int64_t{28}, uint64_t{30}), false);
    ASSERT_EQ(greater_than(int64_t{30}, uint64_t{28}), true);
    ASSERT_EQ(greater_than(int64_t{-5}, uint64_t{28}), false);
    ASSERT_EQ(greater_than(int64_t{3}, uint64_t{3}), false);
}

TEST(CompareSignedUnsigned, LessthanEquals) {
    using namespace arcticdb::comparison;

    auto uint64_max = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(less_than_equals(uint64_max, int64_t{28}), false);
    ASSERT_EQ(less_than_equals(uint64_t{30}, int64_t{28}), false);
    ASSERT_EQ(less_than_equals(uint64_t{28}, int64_t{30}), true);
    ASSERT_EQ(less_than_equals(uint64_t{28}, int64_t{-5}), false);
    ASSERT_EQ(less_than_equals(uint64_t{3}, int64_t{3}), true);
    ASSERT_EQ(less_than_equals(int64_t{28}, uint64_max), true);
    ASSERT_EQ(less_than_equals(int64_t{28}, uint64_t{30}), true);
    ASSERT_EQ(less_than_equals(int64_t{30}, uint64_t{28}), false);
    ASSERT_EQ(less_than_equals(int64_t{-5}, uint64_t{28}), true);
    ASSERT_EQ(less_than_equals(int64_t{3}, uint64_t{3}), true);
}

TEST(CompareSignedUnsigned, GreaterThanEquals) {
    using namespace arcticdb::comparison;

    auto uint64_max = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(greater_than_equals(uint64_max, int64_t{28}), true);
    ASSERT_EQ(greater_than_equals(uint64_t{30}, int64_t{28}), true);
    ASSERT_EQ(greater_than_equals(uint64_t{28}, int64_t{30}), false);
    ASSERT_EQ(greater_than_equals(uint64_t{28}, int64_t{-5}), true);
    ASSERT_EQ(greater_than_equals(uint64_t{3}, int64_t{3}), true);
    ASSERT_EQ(greater_than_equals(int64_t{28}, uint64_max), false);
    ASSERT_EQ(greater_than_equals(int64_t{28}, uint64_t{30}), false);
    ASSERT_EQ(greater_than_equals(int64_t{30}, uint64_t{28}), true);
    ASSERT_EQ(greater_than_equals(int64_t{-5}, uint64_t{28}), false);
    ASSERT_EQ(greater_than_equals(int64_t{3}, uint64_t{3}), true);
}