/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/python_output_frame.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/column_store/column_utils.hpp>

namespace arcticdb::pipelines {

PythonOutputFrame::PythonOutputFrame(const SegmentInMemory &frame, std::shared_ptr<BufferHolder> buffers) :
        module_data_(ModuleData::instance()),
        frame_(frame),
        names_(frame.fields().size() - frame.descriptor().index().field_count()),
        index_columns_(frame.descriptor().index().field_count()),
        buffers_(std::move(buffers)) {
    ARCTICDB_SAMPLE_DEFAULT(PythonOutputFrameCtor)
    const auto field_count = frame.descriptor().index().field_count();
    // works because we ensure that the index must be fetched
    for (std::size_t c = 0; c < field_count; ++c) {
        index_columns_[c] = frame.field(c).name();
    }
    for (std::size_t c = frame.descriptor().index().field_count(); c < static_cast<size_t>(frame.fields().size()); ++c) {
        names_[c - field_count] = frame.field(c).name();
    }
}

PythonOutputFrame::~PythonOutputFrame() {
    if(frame_.is_null())
        return;

    for (auto &column : frame_.columns()) {
        auto dt = column->type().data_type();
        if (is_dynamic_string_type(dt) && column->is_inflated()) {
            auto column_data = column->data();
            column_data.type().visit_tag([&](auto type_desc_tag) {
                using TDT = decltype(type_desc_tag);
                if constexpr (is_dynamic_string_type(TDT::DataTypeTag::data_type)) {
                    while (auto block = column_data.next<TDT>()) {
                        for (auto item : block.value()) {
                          util::check(reinterpret_cast<PyObject *>(item) != nullptr, "Can't delete null item");
                          Py_DECREF(reinterpret_cast<PyObject *>(item));
                        }
                    }
                }
            });
        }
    }
}

std::shared_ptr<FrameDataWrapper> PythonOutputFrame::arrays(py::object &ref) {
    if(auto cached = arrays_.lock())
        return cached;

    auto generated = arcticdb::detail::initialize_array(frame_, ref);
    arrays_  = generated;
    return generated;
}

py::array PythonOutputFrame::array_at(std::size_t col_pos, py::object &anchor) {
    return arcticdb::detail::array_at(frame_, col_pos, anchor);
}

}
