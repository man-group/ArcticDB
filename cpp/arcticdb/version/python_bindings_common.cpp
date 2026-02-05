/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/python_bindings_common.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/numpy_buffer_holder.hpp>
#include <arcticdb/pipeline/pandas_output_frame.hpp>
#include <arcticdb/arrow/arrow_output_frame.hpp>

namespace py = pybind11;

namespace arcticdb::version_store {

void register_version_store_common_bindings(py::module& version, BindingScope scope) {
    bool local_bindings = (scope == BindingScope::LOCAL);
    py::class_<NumpyBufferHolder, std::shared_ptr<NumpyBufferHolder>>(
            version, "NumpyBufferHolder", py::module_local(local_bindings)
    );

    using PandasOutputFrame = arcticdb::pipelines::PandasOutputFrame;
    py::class_<PandasOutputFrame>(version, "PandasOutputFrame", py::module_local(local_bindings))
            .def("extract_numpy_arrays",
                 [](PandasOutputFrame& self) { return python_util::extract_numpy_arrays(self); });

    py::class_<ArrowOutputFrame>(version, "ArrowOutputFrame", py::module_local(local_bindings))
            .def("extract_record_batches", &ArrowOutputFrame::extract_record_batches)
            .def("create_iterator", &ArrowOutputFrame::create_iterator,
                 "Create an iterator for streaming record batches one at a time.")
            .def("num_blocks", &ArrowOutputFrame::num_blocks);
}

} // namespace arcticdb::version_store
