/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/storage/open_mode.hpp>

namespace arcticdb::storage::apy {

namespace py = pybind11;

void register_bindings(py::module &m);

} // namespace arcticdb


