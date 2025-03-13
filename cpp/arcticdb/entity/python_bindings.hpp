/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace arcticdb::entity {

void register_types_bindings(py::module &m);

inline void register_bindings(py::module &m) {

    auto arcticxx_types = m.def_submodule("types", R"pydoc(
    Fundamental types
    -----------------
    Contains definition of the types used to define the descriptors)pydoc");

    arcticdb::entity::register_types_bindings(arcticxx_types);
    }

} // namespace arcticdb::entity
