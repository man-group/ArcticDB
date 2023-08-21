/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/python_bindings.hpp>

#include <pybind11/stl_bind.h>
#include <arcticdb/python/reader.hpp>

#include <arcticdb/storage/python_bindings.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace py = pybind11;

namespace arcticdb {
using namespace arcticdb::python_util;

void register_types(py::module &m) {
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
        //DATA_TYPE(UTF8_STRING)
        //     DATA_TYPE(BYTES)
        //   DATA_TYPE(PICKLE)
#undef DATA_TYPE
        ;

    py::enum_<Dimension>(m, "Dimension")
        .value("Dim0", Dimension::Dim0)
        .value("Dim1", Dimension::Dim1)
        .value("Dim2", Dimension::Dim2);

    m.def("as_dim_checked", &as_dim_checked, "Turns a uint8_t into a Dimension enum object");

    python_util::add_repr(py::class_<TypeDescriptor>(m, "TypeDescriptor")
                              .def(py::init<DataType, Dimension>())
                              .def("data_type", &TypeDescriptor::data_type)
                              .def("dimension", &TypeDescriptor::dimension)
    );
    //TODO re-add this constructor
    python_util::add_repr(py::class_<FieldRef>(m, "FieldDescriptor")
                              .def(py::init<TypeDescriptor, std::string_view>())
                              .def("type", &FieldRef::type)
                              .def("name", &FieldRef::name)
    );

    py::enum_<IndexDescriptor::Type>(m, "IndexKind")
        .value("TIMESTAMP", IndexDescriptor::TIMESTAMP)
        .value("STRING", IndexDescriptor::STRING)
        .value("ROWCOUNT", IndexDescriptor::ROWCOUNT);

    python_util::add_repr(py::class_<IndexDescriptor>(m, "IndexDescriptor")
                              .def(py::init<std::size_t, IndexDescriptor::Type>())
                              .def("field_count", &IndexDescriptor::field_count)
                              .def("kind", &IndexDescriptor::type)
    );

    //TODO re-add at the end
   python_util::add_repr(py::class_<StreamDescriptor>(m, "StreamDescriptor")
    .def(py::init([](StreamId stream_id, IndexDescriptor idx_desc, const std::vector<FieldRef>& fields) {
                                  auto index = default_index_type_from_descriptor(idx_desc.proto());
                                  return util::variant_match(index, [&stream_id, &fields] (auto idx_type){
                                      return StreamDescriptor{index_descriptor(stream_id, idx_type, fields_from_range(fields))};
                                  });
                              }))
                              .def("id", &StreamDescriptor::id)
                              //.def("fields", &StreamDescriptor::fields)
    );

    py::class_<PyTimestampRange>(m, "TimestampRange")
        .def(py::init<const py::object &, const py::object &>())
        .def("as_tuple", [](const PyTimestampRange &rg) {
            return static_cast<TimestampRange>(rg);
        })
        .def_property_readonly("start_nanos_utc", &PyTimestampRange::start_nanos_utc)
        .def_property_readonly("end_nanos_utc", &PyTimestampRange::end_nanos_utc);

    m.def("create_timestamp_index_stream_descriptor", [](StreamId tsid,
                                                         const std::vector<FieldRef>& fields) {
        auto rg = folly::range(fields.begin(), fields.end());
        const auto index = stream::TimeseriesIndex::default_index();
        return index.create_stream_descriptor(tsid, fields_from_range(rg));
    });
}
}

namespace arcticdb::stream {

struct SegmentHolder {
    SegmentInMemory segment;
};


void register_stream_bindings(py::module &m) {

    using Agg = FixedTimestampAggregator;
    using FixedTickRowBuilder = typename Agg::RowBuilderType;

    py::class_<SegmentInMemory>(m, "SegmentInMemory")
    .def(py::init<>())
    .def_property_readonly("row_count", &SegmentInMemory::row_count)
    .def_property_readonly("num_columns", &SegmentInMemory::num_columns)
    .def_property_readonly("string_pool_size", &SegmentInMemory::string_pool_size)
    .def("string_pool", &SegmentInMemory::string_pool, py::return_value_policy::reference)
    .def("column", &SegmentInMemory::column_ref, py::return_value_policy::reference)
    .def("empty", &SegmentInMemory::empty)
    .def("metadata",[](const SegmentInMemory & seg){
        if (!seg.metadata()) return py::bytes();
        return py::bytes(seg.metadata()->SerializeAsString());
        }, py::return_value_policy::copy);

    py::class_<SegmentHolder, std::shared_ptr<SegmentHolder>>(m, "SegmentHolder")
        .def(py::init())
        .def_readonly("segment", &SegmentHolder::segment);

    py::class_<Agg, std::shared_ptr<Agg>>(m, "FixedTimestampAggregator")
        .def(py::init([](std::shared_ptr<SegmentHolder> holder, const StreamDescriptor &desc) {
            return std::make_shared<Agg>(Agg::SchemaPolicy{desc, TimeseriesIndex::default_index()}, [hld = holder](SegmentInMemory &&segment) {
                hld->segment = std::move(segment);
            });
        }))
        .def_property_readonly("row_builder", &Agg::row_builder, py::return_value_policy::reference)
        .def_property_readonly("row_count", &Agg::row_count)
        .def("commit", &Agg::commit)
        .def("rollback_row", &Agg::rollback_row)
        .def("start_row", &Agg::start_row < timestamp > , py::return_value_policy::reference);

    py::class_<FixedTickRowBuilder>(m, "FixedTickRowBuilder")
        .def("start_row", [](FixedTickRowBuilder &b, entity::timestamp timestamp) {
            b.start_row(timestamp);
        })
        .def("end_row", &FixedTickRowBuilder::end_row)
        .def("rollback_row", &FixedTickRowBuilder::rollback_row)
        .def("__enter__", &FixedTickRowBuilder::self, py::return_value_policy::reference)
        .def("__exit__", [](FixedTickRowBuilder &b, py::object &type, py::object &, py::object &) {
            if (!type.is_none())
                b.rollback_row();
            else
                b.end_row();

        })
        .def("find_field", &FixedTickRowBuilder::find_field)

#if 0 // python code used to generate the per type method instantiations
        /*
        def gen_methods(name, cpp_type):
            return'\n'.join( """        .def("set_scalar", &FixedTickRowBuilder::set_scalar<{cpp_type}>,  \\\n
                R"pydoc(set_scalar value at position in the row builder \n
                  Convenience method that will go through the list of overloaded methods \n
                  until it finds one that is compatible. If you know the type beforehand, please use \n
                  the non-overloaded version)pydoc") \\\n
                .def("set_array", &FixedTickRowBuilder::set_array<{cpp_type}>, \\\n
                 R"pydoc(set_array value at position in the row builder \n
                  Convenience method that will go through the list of overloaded methods \n
                  until it finds one that is compatible. If you know the type beforehand, please use \n
                  the non-overloaded version)pydoc" ) \\\n
                .def("set_scalar_{name}", &FixedTickRowBuilder::set_scalar<{cpp_type}>) \n
                .def("set_array_{name}", &FixedTickRowBuilder::set_array<{cpp_type}>) \n""".format(name=name,cpp_type=cpp_type).split('\n')[::2])

        for t in ['uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64']:
            print(gen_methods(t,'std::%s_t' % t))

        for t in ['float', 'double', 'bool']:
            print(gen_methods(t,t))
        */
#endif
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::uint8_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::uint8_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_uint8", &FixedTickRowBuilder::set_scalar < std::uint8_t > )
        .def("set_array_uint8", &FixedTickRowBuilder::set_array < std::uint8_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::uint16_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::uint16_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_uint16", &FixedTickRowBuilder::set_scalar < std::uint16_t > )
        .def("set_array_uint16", &FixedTickRowBuilder::set_array < std::uint16_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::uint32_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::uint32_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_uint32", &FixedTickRowBuilder::set_scalar < std::uint32_t > )
        .def("set_array_uint32", &FixedTickRowBuilder::set_array < std::uint32_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::uint64_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::uint64_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_uint64", &FixedTickRowBuilder::set_scalar < std::uint64_t > )
        .def("set_array_uint64", &FixedTickRowBuilder::set_array < std::uint64_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::int8_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::int8_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_int8", &FixedTickRowBuilder::set_scalar < std::int8_t > )
        .def("set_array_int8", &FixedTickRowBuilder::set_array < std::int8_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::int16_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::int16_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_int16", &FixedTickRowBuilder::set_scalar < std::int16_t > )
        .def("set_array_int16", &FixedTickRowBuilder::set_array < std::int16_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::int32_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::int32_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_int32", &FixedTickRowBuilder::set_scalar < std::int32_t > )
        .def("set_array_int32", &FixedTickRowBuilder::set_array < std::int32_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < std::int64_t > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < std::int64_t > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_int64", &FixedTickRowBuilder::set_scalar < std::int64_t > )
        .def("set_array_int64", &FixedTickRowBuilder::set_array < std::int64_t > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < float > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < float > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_float", &FixedTickRowBuilder::set_scalar < float > )
        .def("set_array_float", &FixedTickRowBuilder::set_array < float > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < double > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < double > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_double", &FixedTickRowBuilder::set_scalar < double > )
        .def("set_array_double", &FixedTickRowBuilder::set_array < double > )
        .def("set_scalar", &FixedTickRowBuilder::set_scalar < bool > , \
        R"pydoc(set_scalar value at position in the row builder
          Convenience method that will go through the list of overloaded +methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_array", &FixedTickRowBuilder::set_array < bool > , \
         R"pydoc(set_array value at position in the row builder
          Convenience method that will go through the list of overloaded methods
          until it finds one that is compatible. If you know the type beforehand, please use
          the non-overloaded version)pydoc") \
.def("set_scalar_bool", &FixedTickRowBuilder::set_scalar < bool > )
        .def("set_array_bool", &FixedTickRowBuilder::set_array < bool > )

        .def("set_string", &FixedTickRowBuilder::set_string)
        .def("set_string_array", &FixedTickRowBuilder::set_string_array)
        .def("set_string_list", &FixedTickRowBuilder::set_string_list);


    py::class_<TickReader, std::shared_ptr<TickReader>>(m, "TickReader")
            .def(py::init())
            .def_property_readonly("row_count", &TickReader::row_count)
            .def("add_segment", &TickReader::add_segment)
            .def("at", &TickReader::at);
}

} // namespace arcticdb::stream


