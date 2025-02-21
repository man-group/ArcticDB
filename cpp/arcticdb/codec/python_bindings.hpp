/* Copyright 2023 Man Group Operations Limited
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
void register_codec(py::module& m);

namespace codec {
inline void register_bindings(py::module& m) {
    auto arcticdb_codec = m.def_submodule("codec", R"pydoc(
    Encoding / decoding of in memory segments for storage
    -----------------------------------------------------
    SegmentInMemory <=> Segment)pydoc");

    arcticdb::register_codec(arcticdb_codec);
}

} // namespace codec
} // namespace arcticdb
