/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <limits>
#include <unordered_set>

#include <gtest/gtest.h>

#include <arcticdb/processing/operation_types.hpp>

TEST(SetMembership, uint64_isin_int64) {
    using namespace arcticdb;
    uint64_t u = std::numeric_limits<uint64_t>::max();
    std::unordered_set<int64_t> iset{-1};
    ASSERT_TRUE(iset.count(u) > 0);
    ASSERT_FALSE(IsInOperator{}(u, iset));
}

TEST(SetMembership, int64_isin_uint64) {
    using namespace arcticdb;
    int64_t i = -1;
    std::unordered_set<uint64_t> uset{std::numeric_limits<uint64_t>::max()};
    ASSERT_TRUE(uset.count(i) > 0);
    ASSERT_FALSE(IsInOperator{}(i, uset));
}

TEST(SetMembership, uint64_isnotin_int64) {
    using namespace arcticdb;
    uint64_t u = std::numeric_limits<uint64_t>::max();
    std::unordered_set<int64_t> iset{-1};
    ASSERT_FALSE(iset.count(u) == 0);
    ASSERT_TRUE(IsNotInOperator{}(u, iset));
}

TEST(SetMembership, int64_isnotin_uint64) {
    using namespace arcticdb;
    int64_t i = -1;
    std::unordered_set<uint64_t> uset{std::numeric_limits<uint64_t>::max()};
    ASSERT_FALSE(uset.count(i) == 0);
    ASSERT_TRUE(IsNotInOperator{}(i, uset));
}
