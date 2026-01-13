/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace arcticdb::column_store {

void register_column_store(py::module& m);

inline void register_bindings(py::module& m) {
    auto arcticdb_column_store = m.def_submodule("column_store", R"pydoc(
    In memory column store
    ----------------------
    In memory columnar representation of segments)pydoc");

    register_column_store(arcticdb_column_store);
}

} // namespace arcticdb::column_store
