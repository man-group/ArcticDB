/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/global_lifetimes.hpp>
#include <Python.h>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    Py_Initialize();
    auto res = RUN_ALL_TESTS();
    arcticdb::shutdown_globals();
    Py_Finalize();
    return res;
}