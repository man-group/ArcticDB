/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/global_lifetimes.hpp>

namespace arcticdb::pipelines {

namespace py = pybind11;

struct ARCTICDB_VISIBILITY_HIDDEN PandasOutputFrame {

    PandasOutputFrame(const SegmentInMemory &frame) :
            module_data_(ModuleData::instance()),
            frame_(frame) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(PandasOutputFrame)

    SegmentInMemory frame() {
        util::check(frame_.has_value(), "PandasOutputFrame contains no frame");
        return *frame_;
    }

    SegmentInMemory release_frame() {
        // Only needed so that we're not relying on Python's garbage collection to destroy this object and thus free
        // the SegmentInMemory
        util::check(frame_.has_value(), "PandasOutputFrame contains no frame");
        auto res = *frame_;
        frame_.reset();
        return res;
    }

private:
    std::shared_ptr<ModuleData> module_data_;
    std::optional<SegmentInMemory> frame_;
};

}