/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace arcticdb::async {

void register_bindings(py::module &m);

} // namespace arcticdb::async


