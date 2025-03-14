/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/entity/python_bindings.hpp>

#include <pybind11/operators.h>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>

namespace py = pybind11;

namespace arcticdb::entity {

using namespace arcticdb::python_util;

std::vector<FieldWrapper> field_collection_to_ref_vector(const FieldCollection& fields){
    auto result = std::vector<FieldWrapper>{};
    result.reserve(fields.size());
    std::transform(fields.begin(), fields.end(), std::back_inserter(result), [](const Field& field){return FieldWrapper{field.type(), field.name()};});
    return result;
}

void register_types_bindings(py::module &m) {

    py::enum_<ValueType>(m, "ValueType")
#define VALUE_TYPE(__VT__) .value(#__VT__, ValueType::__VT__)
        VALUE_TYPE(UINT)
        VALUE_TYPE(INT)
        VALUE_TYPE(FLOAT)
        VALUE_TYPE(BOOL)
        VALUE_TYPE(NANOSECONDS_UTC)
        VALUE_TYPE(ASCII_FIXED)
        VALUE_TYPE(UTF8_FIXED)
        VALUE_TYPE(BYTES)
        VALUE_TYPE(UTF_DYNAMIC)
        VALUE_TYPE(EMPTY)
        VALUE_TYPE(BOOL_OBJECT)
#undef VALUE_TYPE
        ;

    py::enum_<DataType>(m, "DataType")
#define DATA_TYPE(__DT__) .value(#__DT__, DataType::__DT__)
        DATA_TYPE(UINT8)
        DATA_TYPE(UINT16)
        DATA_TYPE(UINT32)
        DATA_TYPE(UINT64)
        DATA_TYPE(INT8)
        DATA_TYPE(INT16)
        DATA_TYPE(INT32)
        DATA_TYPE(INT64)
        DATA_TYPE(FLOAT32)
        DATA_TYPE(FLOAT64)
        DATA_TYPE(BOOL8)
        DATA_TYPE(NANOSECONDS_UTC64)
        DATA_TYPE(ASCII_FIXED64)
        DATA_TYPE(ASCII_DYNAMIC64)
        DATA_TYPE(UTF_FIXED64)
        DATA_TYPE(UTF_DYNAMIC64)
#undef DATA_TYPE
        ;

    py::enum_<Dimension>(m, "Dimension")
        .value("Dim0", Dimension::Dim0)
        .value("Dim1", Dimension::Dim1)
        .value("Dim2", Dimension::Dim2);

    m.def("as_dim_checked", &as_dim_checked, "Turns a uint8_t into a Dimension enum object");

    python_util::add_repr(py::class_<TypeDescriptor>(m, "TypeDescriptor")
        .def(py::init<DataType, Dimension>())
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def("data_type", &TypeDescriptor::data_type)
        .def_property_readonly("value_type", [] (const TypeDescriptor& self) {
            return static_cast<int>(entity::value_proto_from_data_type(self.data_type()));
        })
        .def_property_readonly("dimension", [] (const TypeDescriptor& self) {
            return static_cast<int>(entity::type_descriptor_to_proto(self).dimension());
        }));

    python_util::add_repr(py::class_<FieldRef>(m, "FieldDescriptor")
        .def(py::init<TypeDescriptor, std::string_view>())
        .def_property_readonly("type", &FieldRef::type)
        .def_property_readonly("name", &FieldRef::name));

    python_util::add_repr(py::class_<FieldWrapper>(m, "FieldDescriptorWrapper")
        .def_property_readonly("type", &FieldWrapper::type)
        .def_property_readonly("name", &FieldWrapper::name));

    py::enum_<IndexDescriptorImpl::Type>(m, "IndexKind")
        .value("TIMESTAMP", IndexDescriptorImpl::Type::TIMESTAMP)
        .value("STRING", IndexDescriptorImpl::Type::STRING)
        .value("ROWCOUNT", IndexDescriptorImpl::Type::ROWCOUNT);

    python_util::add_repr(py::class_<IndexDescriptorImpl>(m, "IndexDescriptor")
        .def(py::init<std::size_t, IndexDescriptorImpl::Type>())
        .def("field_count", &IndexDescriptorImpl::field_count)
        .def("kind", &IndexDescriptorImpl::type));

    python_util::add_repr(py::class_<StreamDescriptor>(m, "StreamDescriptor")
        .def(py::init([](StreamId stream_id, IndexDescriptorImpl idx_desc, const std::vector<FieldRef>& fields) {
            auto index = stream::default_index_type_from_descriptor(idx_desc);
            return util::variant_match(index, [&stream_id, &fields] (auto idx_type){
                return StreamDescriptor{index_descriptor_from_range(stream_id, idx_type, fields_from_range(fields))};
            });
        }))
        .def("id", &StreamDescriptor::id)
        .def("fields", [](const StreamDescriptor& desc){
            return field_collection_to_ref_vector(desc.fields());
        })
        .def("sorted", &StreamDescriptor::sorted)
    );

    py::class_<TimeseriesDescriptor>(m, "TimeseriesDescriptor")
        .def_property_readonly("fields", [](const TimeseriesDescriptor& desc){
            return field_collection_to_ref_vector(desc.fields());
        }).def_property_readonly("normalization", [](const TimeseriesDescriptor& self) {
            return python_util::pb_to_python(self.normalization());
        }).def_property_readonly("sorted", [](const TimeseriesDescriptor& self) {
            return self.sorted();
        }).def_property_readonly("index", [](const TimeseriesDescriptor& self) {
            return self.index();
        }).def_property_readonly("total_rows", [](const TimeseriesDescriptor& self) {
            return self.total_rows();
        }).def_property_readonly("next_key", [](const TimeseriesDescriptor& self) -> std::optional<AtomKey> {
            if (self.proto().has_next_key()){
                return key_from_proto(self.proto().next_key());
            }
            return std::nullopt;
        });

    py::class_<PyTimestampRange>(m, "TimestampRange")
        .def(py::init<const py::object &, const py::object &>())
        .def("as_tuple", [](const PyTimestampRange &rg) {
            return static_cast<TimestampRange>(rg);
        })
        .def_property_readonly("start_nanos_utc", &PyTimestampRange::start_nanos_utc)
        .def_property_readonly("end_nanos_utc", &PyTimestampRange::end_nanos_utc);

    m.def("create_timestamp_index_stream_descriptor", [](StreamId tsid, const std::vector<FieldRef>& fields) {
        auto rg = folly::range(fields.begin(), fields.end());
        const auto index = stream::TimeseriesIndex::default_index();
        return index.create_stream_descriptor(tsid, fields_from_range(rg));
    });
}

} // namespace arcticdb::entity