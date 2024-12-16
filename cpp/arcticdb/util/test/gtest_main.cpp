/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/global_lifetimes.hpp>
#include <pybind11/pybind11.h>  // Must not directly include Python.h on Windows
#include <util/gil_safe_py_none.hpp>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    Py_Initialize();
    arcticdb::GilSafePyNone::instance(); // Ensure that the GIL is held when the static py::none gets allocated
    auto res = RUN_ALL_TESTS();
    arcticdb::shutdown_globals();
    Py_Finalize();
    return res;
}
