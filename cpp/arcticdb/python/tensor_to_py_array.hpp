/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/native_tensor.hpp>

#include <pybind11/numpy.h>

namespace arcticdb::python_util {

namespace py = pybind11;

template<typename T>
py::array to_py_array(const entity::TypedTensor<T>& tensor) {
    return py::array({tensor.shape(), tensor.shape() + tensor.ndim()}, reinterpret_cast<const T*>(tensor.data()));
}

} // namespace arcticdb::python_utils
