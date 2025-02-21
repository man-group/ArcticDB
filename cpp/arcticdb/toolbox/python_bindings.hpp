/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>

namespace arcticdb::toolbox::apy {

namespace py = pybind11;

void register_bindings(py::module& m, py::exception<arcticdb::ArcticException>& base_exception);

} // namespace arcticdb::toolbox::apy
