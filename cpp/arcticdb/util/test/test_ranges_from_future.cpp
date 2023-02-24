/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <arcticdb/util/ranges_from_future.hpp>

#include <unordered_map>

using namespace arcticdb::utils;
using namespace testing;

TEST(RangesFromFuture, keys_and_values) {
    std::unordered_map<int, char> m;
    ASSERT_THAT(keys(m), IsEmpty());
    ASSERT_THAT(values(m), IsEmpty());
    m.emplace(1, 'a');
    ASSERT_THAT(keys(m), ElementsAre(1));
    ASSERT_THAT(values(m), ElementsAre('a'));
    m.emplace(2, 'b');
    ASSERT_THAT(keys(m), UnorderedElementsAre(1, 2));
    ASSERT_THAT(values(m), UnorderedElementsAre('a', 'b'));
    m.erase(1);
    ASSERT_THAT(keys(m), ElementsAre(2));
    ASSERT_THAT(values(m), ElementsAre('b'));
}