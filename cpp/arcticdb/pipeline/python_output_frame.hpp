/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/pipeline/frame_data_wrapper.hpp>

namespace arcticdb::pipelines {

namespace py = pybind11;

struct ARCTICDB_VISIBILITY_HIDDEN PythonOutputFrame {

    PythonOutputFrame(const SegmentInMemory& frame);

    ~PythonOutputFrame();

    ARCTICDB_MOVE_ONLY_DEFAULT(PythonOutputFrame)

    std::shared_ptr<FrameDataWrapper> arrays(py::object &ref);

    std::vector<std::string> &names() { return names_; }

    std::vector<std::string> &index_columns() { return index_columns_; }

    SegmentInMemory frame() { return frame_; }

private:
    std::shared_ptr<FrameDataWrapper> initialize_array(py::object &ref);
    py::array array_at(std::size_t col_pos, py::object &anchor);

    std::shared_ptr<ModuleData> module_data_;
    SegmentInMemory frame_;
    std::vector<std::string> names_;
    std::vector<std::string> index_columns_;
    std::weak_ptr<FrameDataWrapper> arrays_;
};
}