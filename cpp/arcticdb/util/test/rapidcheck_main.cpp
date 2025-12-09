/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/global_lifetimes.hpp>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    auto res = RUN_ALL_TESTS();
    arcticdb::shutdown_globals();
    return res;
}