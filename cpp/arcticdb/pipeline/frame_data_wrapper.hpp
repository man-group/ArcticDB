/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/util/constructors.hpp>
#include <pybind11/pybind11.h>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/global_lifetimes.hpp>
#include <arcticdb/util/preprocess.hpp>

#include <pybind11/numpy.h>
#include <vector>
#include <string>
#include <memory>

namespace arcticdb::pipelines {

namespace py = pybind11;

struct ARCTICDB_VISIBILITY_HIDDEN FrameDataWrapper{
    explicit FrameDataWrapper(size_t size) : data_(size) {}

    const std::vector<py::array>& data() const {
        return data_;
    }

    std::vector<py::array> data_;
};

} // arcticdb::pipelines
