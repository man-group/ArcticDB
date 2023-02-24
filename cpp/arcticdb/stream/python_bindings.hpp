/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace arcticdb {
void register_types(py::module &m);

namespace stream {

void register_stream_bindings(py::module &m);

inline void register_bindings(py::module &m) {
    auto arcticxx_types = m.def_submodule("types", R"pydoc(
    Fundamental types
    -----------------
    Contains definition of the types used to define the descriptors)pydoc");

    arcticdb::register_types(arcticxx_types);

    auto arcticxx_stream = m.def_submodule("stream", R"pydoc(
    arcticdb Streams
    -----------------
    Contains the stream api classes used to write/read streams of values
    )pydoc");

    arcticdb::stream::register_stream_bindings(arcticxx_stream);
}

} // namespace arcticdb::stream
} // namespace arcticdb

