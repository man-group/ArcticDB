/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/configs_map.hpp>

using namespace arcticdb;

TEST(HashAccum, NotCommutative) {
    HashAccum h1;
    HashAccum h2;
    h1.reset();
    h2.reset();

    int val1 = 1;
    int val2 = 2;

    h1(&val1);
    h1(&val2);

    h2(&val2);
    h2(&val1);

    EXPECT_NE(h1.digest(), h2.digest());

}
