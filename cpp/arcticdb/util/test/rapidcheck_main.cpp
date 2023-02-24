/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/global_lifetimes.hpp>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    auto res = RUN_ALL_TESTS();
    arcticdb::shutdown_globals();
    return res;
}