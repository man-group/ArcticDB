/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pybind11/stl.h>

#include <arcticdb/arrow/arrow_array.hpp>
#include <arcticdb/arrow/arrow_schema.hpp>
#include <arcticdb/arrow/arrow_buffer.hpp>

#include <arcticdb/arrow/arrow_utils.hpp> // TODO delete me

namespace py = pybind11;

namespace arcticdb::arrow {

void register_bindings(py::module& m) {
    auto arrow = m.def_submodule("arrow", R"pydoc(
    Encoding / decoding of arrow objects
    -----------------------------------------------------
    Arrow <=> ArcticDB)pydoc");

    py::class_<ArrayChildren>(arrow, "ArrayChildren")
        .def(py::init<std::shared_ptr<const Array>>())
        .def("__getitem__", &ArrayChildren::operator[])
        .def("__len__", &ArrayChildren::length);

    py::class_<ArrayViewChildren>(arrow, "ArrayViewChildren")
        .def("__len__", &ArrayViewChildren::length)
        .def("__getitem__", &ArrayViewChildren::operator[]);

    py::class_<Array, std::shared_ptr<Array>>(arrow, "Array")
        .def("addr", &Array::addr)
        .def("isValid", &Array::is_valid)
        .def("assertValid", &Array::assert_valid)
        .def("schema", &Array::schema, py::return_value_policy::reference)
        .def("length", &Array::length)
        .def("offset", &Array::offset)
        .def("nullCount", &Array::null_count)
        .def("buffers", &Array::buffers)
        .def("children", &Array::children)
        .def("dictionary", &Array::dictionary)
        .def("view", &Array::view);

    py::class_<ArrayViewBuffers>(arrow, "ArrayViewBuffers")
        .def(py::init<std::shared_ptr<ArrayView>>())
        .def("__len__", &ArrayViewBuffers::length)
        .def("__getitem__", &ArrayViewBuffers::operator[]);

    py::class_<ArrayView, std::shared_ptr<ArrayView>>(arrow, "ArrayView")
        .def("length", &ArrayView::length)
        .def("offset", &ArrayView::offset)
        .def("nullCount", &ArrayView::null_count)
        .def("children", &ArrayView::children)
        .def("buffers", &ArrayView::buffers)
        .def("dictionary", &ArrayView::dictionary)
        .def("schema", &ArrayView::schema);

    py::class_<SchemaView>(arrow, "SchemaView")
        .def(py::init<>())
        .def("schema_view", &SchemaView::schema_view, py::return_value_policy::reference)
        .def("type", &SchemaView::type)
        .def("storage_type", &SchemaView::storage_type)
        .def("fixed_size", &SchemaView::fixed_size)
        .def("decimal_bitwidth", &SchemaView::decimal_bitwidth)
        .def("decimal_precision", &SchemaView::decimal_precision)
        .def("decimal_scale", &SchemaView::decimal_scale)
        .def("time_unit", &SchemaView::time_unit)
        .def("timezone", &SchemaView::timezone)
        .def("union_type_ids", &SchemaView::union_type_ids)
        .def("extension_name", &SchemaView::extension_name);

    py::class_<SchemaChildren>(arrow, "SchemaChildren")
        .def(py::init<std::shared_ptr<const Schema>>())
        .def("__len__", &SchemaChildren::__len__)
        .def("__getitem__", &SchemaChildren::operator[], py::return_value_policy::reference);

    py::class_<SchemaMetadata>(arrow, "SchemaMetadata")
        .def(py::init<std::shared_ptr<Schema>, std::uintptr_t>())
        .def("size", &SchemaMetadata::size)
        .def("__iter__", [](SchemaMetadata& self) {
            self.initReader();
            return py::make_iterator(self.begin(), self.end());
        }, py::keep_alive<0, 1>());

    py::class_<Schema, std::shared_ptr<Schema>>(arrow, "Schema")
        .def(py::init<>())
        .def("addr", &Schema::addr)
        .def("__repr__", &Schema::__repr__)
        .def("format", &Schema::format)
        .def("name", &Schema::name)
        .def("flags", &Schema::flags)
        .def("metadata", &Schema::metadata)
        .def("children", &Schema::children)
        .def("dictionary", &Schema::dictionary)
        .def("view", &Schema::view);

    py::class_<BufferView>(arrow, "BufferView", py::buffer_protocol())
        .def_buffer([](arrow::BufferView &bv) -> py::buffer_info {
            return py::buffer_info(
                const_cast<uint8_t*>(bv.ptr()),
                bv.item_size(),
                bv.get_format(),
                1,
                { bv.shapes() },
                { bv.strides() }
            );
        });

    arrow.def("get_test_array", &get_test_array, "Get a random array");
}

} // namespace arcticdb::arrow
