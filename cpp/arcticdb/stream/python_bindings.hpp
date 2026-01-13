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

namespace arcticdb {
void register_types(py::module& m);

namespace stream {

void register_stream_bindings(py::module& m);

inline void register_bindings(py::module& m) {
    auto arcticdb_ext_types = m.def_submodule("types", R"pydoc(
    Fundamental types
    -----------------
    Contains definition of the types used to define the descriptors)pydoc");

    arcticdb::register_types(arcticdb_ext_types);

    auto arcticdb_ext_stream = m.def_submodule("stream", R"pydoc(
    arcticdb Streams
    -----------------
    Contains the stream api classes used to write/read streams of values
    )pydoc");

    arcticdb::stream::register_stream_bindings(arcticdb_ext_stream);
}

} // namespace stream
} // namespace arcticdb
