/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <pybind11/numpy.h>
#include <concepts>

namespace py = pybind11;

namespace arcticdb::python_util {

/**
 * Python/NumPy adapter functions for core column_store types.
 * These functions provide the bridge between Python and C++ for column operations.
 *
 * These are free functions that accept pybind11 types, allowing the core Column and
 * SegmentInMemory classes to remain free of Python dependencies.
 */

// Column Python adapters
template<class T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
void column_set_array(Column& col, ssize_t row_offset, py::array_t<T>& val);

// SegmentInMemory Python adapters
template<class T>
requires std::integral<T> || std::floating_point<T>
void segment_set_array(SegmentInMemory& seg, position_t pos, py::array_t<T>& val);

} // namespace arcticdb::python_util
