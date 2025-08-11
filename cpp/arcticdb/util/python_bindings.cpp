/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <pybind11/functional.h>

#include <arcticdb/util/python_bindings.hpp>
#include <arcticdb/util/regex_filter.hpp>

namespace arcticdb::util {

void register_bindings(py::module &m) {
    auto tools = m.def_submodule("util", "Utility functions for ArcticDB");

    py::class_<RegexGeneric, std::shared_ptr<RegexGeneric>>(tools, "RegexGeneric")
        .def(py::init<const std::string&>(), py::arg("pattern"))
        .def("text", &RegexGeneric::text);
}
} // namespace arcticdb::util
