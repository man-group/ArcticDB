/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace arcticdb::python_util {

/**
 * Python buffer protocol adapters for column_store types.
 * These functions create py::buffer_info objects for Python buffer protocol support.
 */

// StringPool buffer protocol adapter
py::buffer_info string_pool_as_buffer_info(const StringPool& pool);

// String array buffer protocol adapter
py::buffer_info from_string_array(const Column::StringArrayData& data);

} // namespace arcticdb::python_util
