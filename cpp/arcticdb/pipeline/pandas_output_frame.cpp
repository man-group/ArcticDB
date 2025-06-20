/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/pandas_output_frame.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/column_store/column_utils.hpp>

namespace arcticdb::pipelines {

PandasOutputFrame::PandasOutputFrame(const SegmentInMemory &frame) :
        module_data_(ModuleData::instance()),
        frame_(frame),
        names_(frame.fields().size() - frame.descriptor().index().field_count()),
        index_columns_(frame.descriptor().index().field_count()) {
    ARCTICDB_SAMPLE_DEFAULT(PandasOutputFrameCtor)
    const auto field_count = frame.descriptor().index().field_count();
    // works because we ensure that the index must be fetched
    for (std::size_t c = 0; c < field_count; ++c) {
        index_columns_[c] = frame.field(c).name();
    }
    for (std::size_t c = frame.descriptor().index().field_count(); c < static_cast<size_t>(frame.fields().size()); ++c) {
        names_[c - field_count] = frame.field(c).name();
    }
}

PandasOutputFrame::~PandasOutputFrame() {
    if(frame_.is_null())
        return;

    for (auto &column : frame_.columns()) {
        auto dt = column->type().data_type();
        if (is_dynamic_string_type(dt) && column->is_inflated()) {
            auto column_data = column->data();
            column_data.type().visit_tag([&](auto type_desc_tag) {
                using TDT = decltype(type_desc_tag);
                constexpr auto td = TypeDescriptor(type_desc_tag);
                if constexpr (is_object_type(td)) {
                    if constexpr(is_array_type(td)) {
                        auto it = column_data.buffer().iterator(sizeof(PyObject*));
                        while(!it.finished()) {
                            if (reinterpret_cast<PyObject*>(it.value()) != nullptr) {
                                Py_DECREF(reinterpret_cast<PyObject*>(it.value()));
                            } else {
                                log::version().error("Unexpected nullptr to DecRef in PandasOutputFrame destructor");
                            }
                            it.next();
                        }
                    } else if constexpr (!is_arrow_output_only_type(td)) {
                        while (auto block = column_data.next<TDT>()) {
                            for (auto item : *block) {
                                if (reinterpret_cast<PyObject*>(item) != nullptr) {
                                    Py_DECREF(reinterpret_cast<PyObject*>(item));
                                } else {
                                    log::version().error("Unexpected nullptr to DecRef in PandasOutputFrame destructor");
                                }
                            }
                        }
                    } else {
                        log::version().error("Unexpected arrow output format seen in PandasOutputFrame");
                    }
                }
            });
        }
    }
}

std::shared_ptr<FrameDataWrapper> PandasOutputFrame::arrays(py::object &ref) {
    if(auto cached = arrays_.lock())
        return cached;

    auto generated = arcticdb::detail::initialize_array(frame_, OutputFormat::PANDAS, ref);
    arrays_  = generated;
    return generated;
}

py::array PandasOutputFrame::array_at(std::size_t col_pos, py::object &anchor) {
    return arcticdb::detail::array_at(frame_, col_pos, OutputFormat::PANDAS, anchor);
}

}
