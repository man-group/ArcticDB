/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/python_bindings.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/buffer_protocol_python_adapters.hpp>

namespace py = pybind11;

namespace arcticdb::column_store {

void register_column_store(py::module& m) {

    py::class_<Column>(m, "Column").def(py::init<>()).def_property_readonly("row_count", &Column::row_count);

    py::class_<ColumnData>(m, "ColumnData").def_property_readonly("type", &ColumnData::type);

    py::class_<StringPool>(m, "StringPool")
            .def(py::init())
            .def_property_readonly("nbytes", &StringPool::size)
            .def("as_buffer_info", &python_util::string_pool_as_buffer_info);
}

} // namespace arcticdb::column_store
