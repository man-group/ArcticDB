/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <pybind11/pybind11.h>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <string>

namespace arcticdb::convert {

namespace py = pybind11;
using namespace arcticdb::entity;

struct ARCTICDB_VISIBILITY_HIDDEN PyStringWrapper {
    char *buffer_;
    size_t length_;
    py::handle handle_;

    PyStringWrapper(char *buf, ssize_t len, py::handle handle) :
        buffer_(buf),
        length_(size_t(len)),
        handle_(handle) {}

    ~PyStringWrapper() {
        if (handle_)
            handle_.dec_ref();
    }
};

PyStringWrapper pystring_to_buffer(
    PyObject *obj,
    py::handle handle = py::handle());

PyStringWrapper py_unicode_to_buffer(
    PyObject *obj);

NativeTensor obj_to_tensor(PyObject *ptr);

pipelines::InputTensorFrame py_ndf_to_frame(
    const StreamId& stream_name,
    const py::tuple &item,
    const py::object &norm_meta,
    const py::object &user_meta);

} // namespace arcticdb::convert
