/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <fmt/format.h>

#include <arcticdb/entity/types.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;

namespace arcticdb {

using namespace arcticdb::entity;

template<typename T>
void print_iterable(std::string_view key, const T& vals) {
    fmt::print("{}=[{}]\n", key, vals);
}

inline void print_buffer_info(py::buffer& buf) {
    auto info{buf.request()};
    py::dtype dtype(info);

    fmt::print("fmt={}, dtype(kind={}, size={})\n", info.format, dtype.kind(), dtype.itemsize());
    print_iterable("shape", info.shape);
    print_iterable("strides", info.strides);
}

inline std::string get_format_specifier(DataType dt) {
    return details::visit_type(dt, [&](auto DTT) {
        return py::format_descriptor<typename decltype(DTT)::raw_type>::format();
    });
}

// Python adaptor functions
inline DataType get_buffer_type(py::dtype& dtype) {
    // enumerated sizes go 1,2,4,8 so the offset in sizebits is the binary log of itemsize + 1
    return get_data_type(dtype.kind(), SizeBits(static_cast<uint8_t>(log2(dtype.itemsize()) + 1.5)));
};

inline TypeDescriptor get_type_descriptor(py::buffer_info& info) {
    py::dtype dtype(info);
    return TypeDescriptor(get_buffer_type(dtype), as_dim_checked(uint8_t(info.ndim)));
}

inline bool is_py_nan(const PyObject* obj) {
    if (PyFloat_Check(obj)) {
        const auto val = PyFloat_AsDouble(const_cast<PyObject*>(obj));
        return std::isnan(val);
    }
    return false;
}

inline bool is_py_none(const PyObject* obj) {
#if PY_MAJOR_VERSION > 3 || (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 10)
    return Py_IsNone(obj);
#else
    return obj == Py_None;
#endif
}

} // namespace arcticdb
