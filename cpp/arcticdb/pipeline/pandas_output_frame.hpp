/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_data_wrapper.hpp>
#include <arcticdb/util/decode_path_data.hpp>

namespace arcticdb::pipelines {

namespace py = pybind11;

struct ARCTICDB_VISIBILITY_HIDDEN PandasOutputFrame {

    PandasOutputFrame(const SegmentInMemory& frame);

    ~PandasOutputFrame();

    ARCTICDB_MOVE_ONLY_DEFAULT(PandasOutputFrame)

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