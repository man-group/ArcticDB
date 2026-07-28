/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/python_bindings.hpp>
#include <arcticdb/util/regex_filter.hpp>
#include <arcticdb/util/segment_residency_tracker.hpp>

namespace arcticdb::util {

void register_bindings(py::module& m) {
    auto tools = m.def_submodule("util", "Utility functions for ArcticDB");

    py::class_<RegexGeneric, std::shared_ptr<RegexGeneric>>(tools, "RegexGeneric")
            .def(py::init<const std::string&>(), py::arg("pattern"))
            .def("text", &RegexGeneric::text);

    tools.def(
            "set_segment_residency_tracking",
            [](bool enabled) { SegmentResidencyTracker::instance().set_enabled(enabled); },
            py::arg("enabled"),
            "Test-only. Enable counting of segments decoded from storage that are resident in memory."
    );
    tools.def("reset_segment_residency_tracking", []() { SegmentResidencyTracker::instance().reset(); });
    tools.def("segment_residency_high_water", []() { return SegmentResidencyTracker::instance().high_water(); });
    tools.def("segment_residency_live", []() { return SegmentResidencyTracker::instance().live(); });
}
} // namespace arcticdb::util
