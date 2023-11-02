#pragma once

#include <arcticdb/arrow/arrow_common.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/arrow/nanoarrow/nanoarrow.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/arrow/arrow_array.hpp> //TODO remove me
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/stream/index.hpp>


namespace arcticdb {


inline std::shared_ptr<Schema> arrow_schema_from_type(ArrowType arrow_type) {
    return std::make_shared<Schema>(arrow_type);
}

inline void initialize_arrow_array(Array& array, DataType data_type, int64_t length, size_t bytes) {
    const auto arrow_type = arcticdb_to_arrow_type(data_type);
    const auto base_type = base_arrow_type(arrow_type);
    array.set_schema(arrow_schema_from_type(base_type));
    array.initialize();
    array.ensure(length, bytes);
}

inline void arrow_schema_from_field(ArrowSchema* schema, const entity::Field& field) {
    const auto data_type = field.type().data_type();
    const auto arrow_type = arcticdb_to_arrow_type(data_type);
    if(is_time_type(data_type)) {
        ArrowSchemaInit(schema);
        ARCTICDB_ARROW_CHECK(ArrowSchemaSetTypeDateTime(schema, arrow_type, NANOARROW_TIME_UNIT_NANO, nullptr))
    } else {
        ARCTICDB_ARROW_CHECK(ArrowSchemaInitFromType(schema, arcticdb_to_arrow_type(field.type().data_type())))
    }
    std::string field_name{field.name()};
    ARCTICDB_ARROW_CHECK(ArrowSchemaSetName(schema, field_name.c_str()))
}

inline std::string string_from_view(nanoarrow::UniqueArrayView& input_view, entity::position_t pos) {
    ArrowStringView item;
    if (ArrowArrayViewIsNull(input_view.get(), pos)) {
        return std::string{};
    } else {
        item = ArrowArrayViewGetStringUnsafe(input_view.get(), pos);
        return  std::string(item.data, item.size_bytes);
    }
}

inline ArrowStringView arrow_string_view_from_char(const char* value) {
    return ArrowCharView(value);
}


inline void write_to_arrow_buffer(ArrowBuffer* buffer, const uint8_t* input, size_t bytes) {
    ArrowBufferAppendUnsafe(buffer, input, bytes);
}

inline void column_to_arrow_array(Array& array, const Column& column, const Field& /*field*/) {
  //  initialize_arrow_array(array, column.type().data_type(), column.row_count(), column.bytes());
  //  arrow_schema_from_field(array.schema().ptr(), field);
   // const auto data_type = field.type().data_type();
  //  const auto arrow_type = arcticdb_to_arrow_type(data_type);
    array.ensure(column.row_count(), column.bytes());
    auto* buffer = array.get_data_buffer(1);

    auto private_data = reinterpret_cast<ArrowArrayPrivateData*>(array.ptr()->private_data);
    util::check(private_data->layout.buffer_type[1] == NANOARROW_BUFFER_TYPE_DATA, "Wrong buffer type");
    auto column_data = column.data();
    column_data.type().visit_tag([&](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        while (auto block = column_data.next<TDT>()) {
            write_to_arrow_buffer(buffer, reinterpret_cast<const uint8_t*>(block->data()), block->nbytes());
        }
    });
}

void write_array_to_column(Column& output_column, const Array& array) {
    auto data = output_column.ptr();
    auto pos = 0ULL;
    auto view = array.view();
    for (int64_t i = 0; i < array.num_buffers(); i++) {
        auto* child1 = array.ptr();
        auto private_data = reinterpret_cast<ArrowArrayPrivateData*>(child1->private_data);
        util::check(private_data->layout.buffer_type[1] == NANOARROW_BUFFER_TYPE_DATA, "Wrong buffer type");

        if(view.layout().buffer_type[i] == NANOARROW_BUFFER_TYPE_DATA) {
            auto arrow_buffer = array.get_data_buffer(i);
            const auto bytes = arrow_buffer->size_bytes;
            memcpy(&data[pos], arrow_buffer->data, bytes);
            pos += bytes;
        }
    }
    output_column.force_set_row_data(array.length());
}

inline Column arrow_array_to_column(const Array& array, ArrowType arrow_type) {
    auto buffer_rows = static_cast<size_t>(array.length());
    Column output_column(make_scalar_type(arrow_to_arcticdb_type(arrow_type)), buffer_rows, true, false); //TODO handle sparse
    write_array_to_column(output_column, array);
    return output_column;
}

inline auto get_test_array() {
    const auto type = make_scalar_type(DataType::UINT64);
    Column column{type, 10, false, false};
    FieldWrapper field_wrapper{type, "numbers"};
    for(auto i = 0ULL; i < 10; ++i) {
        column.set_scalar<uint64_t>(i, i);
    }

    Array array;
    column_to_arrow_array(array, column, field_wrapper.field());
    return array;
}

inline std::shared_ptr<Schema> descriptor_to_arrow_schema(const StreamDescriptor& desc) {
    auto schema = std::make_shared<Schema>();
    ARCTICDB_ARROW_CHECK(ArrowSchemaInitFromType(schema->ptr(), NANOARROW_TYPE_STRUCT));
    ARCTICDB_ARROW_CHECK(ArrowSchemaAllocateChildren(schema->ptr(), desc.field_count()));
    const auto id = fmt::format("{}", desc.id());
    ARCTICDB_ARROW_CHECK(ArrowSchemaSetName(schema->ptr(), id.c_str()));

    auto children = schema->children();
    for(auto i = 0u; i < desc.field_count(); ++i) {
        arrow_schema_from_field(reinterpret_cast<ArrowSchema*>(children.child_addr(i)), desc.field(i));
    }
    return schema;
}

inline stream::IndexDescriptor arrow_index(const Schema& schema) {
    arrow::check<ErrorCode::E_ARROW_INVALID>(schema.num_children() > 0, "Empty arrow schema");
    auto first_child = schema.children()[0].view();
    if(first_child.type() == NANOARROW_TYPE_TIME64)
        return {1, IndexDescriptor::TIMESTAMP};
    else
        return {0, IndexDescriptor::ROWCOUNT};
}

inline StreamDescriptor arrow_schema_to_descriptor(const Schema& schema) {
    auto id = StringId(schema.name());
    auto index_desc = arrow_index(schema);
    //StreamDescriptor desc{, index_desc};
    auto index = stream::default_index_type_from_descriptor(index_desc.proto());
    auto desc = util::variant_match(index, [&schema] (const auto& idx) { return idx.create_stream_descriptor(StreamId{schema.name()}, {}); });
    auto children = schema.children();
    for(auto i = index_desc.field_count(); i < static_cast<size_t>(schema.num_children()); ++i) {
        const auto child = schema.children()[i];
        const auto arctic_type = arrow_to_arcticdb_type(child.view().type());
        desc.add_field(scalar_field(arctic_type, child.name()));
    }
    return desc;
}

inline void initialize_record_batch(ArrowArray* array, const StreamDescriptor& /*desc*/, const Schema& schema) {
    ArrowError error;
    ARCTICDB_ARROW_CHECK(ArrowArrayInitFromSchema(array, schema.ptr(), &error));

}

inline std::shared_ptr<Array> segment_to_record_batch(const SegmentInMemory& segment) {
    const auto& descriptor = segment.descriptor();
    auto schema = descriptor_to_arrow_schema(descriptor);

    auto record_batch = std::make_shared<Array>(schema);
    initialize_record_batch(record_batch->ptr(), descriptor, *schema);
    auto array_children = record_batch->children();

    for(auto i = 0ULL; i < descriptor.field_count(); ++i) {
        auto child_array = array_children[i];
        column_to_arrow_array(*child_array, segment.column(i), descriptor.field(i));
    }
    auto* child1 = array_children[0]->ptr();
    auto private_data = reinterpret_cast<ArrowArrayPrivateData*>(child1->private_data);
    util::check(private_data->layout.buffer_type[1] == NANOARROW_BUFFER_TYPE_DATA, "Wrong buffer type");
    return record_batch;
}

inline SegmentInMemory record_batch_to_segment(const Array& record_batch) {
    auto desc = arrow_schema_to_descriptor(record_batch.schema());
    auto array_children = record_batch.children();
    auto* child1 = array_children[0]->ptr();
    auto private_data = reinterpret_cast<ArrowArrayPrivateData*>(child1->private_data);
    util::check(private_data->layout.buffer_type[1] == NANOARROW_BUFFER_TYPE_DATA, "Wrong buffer type");
    const auto num_rows = record_batch.num_children() > 0 ? array_children[0]->length() : 0;
    SegmentInMemory segment(desc, num_rows, true, false);

    for(auto i = 0LL; i < record_batch.num_children(); ++i) {
        auto child_array = array_children[i];
        write_array_to_column(segment.column(i), *child_array);
    }
    segment.set_row_id(segment.column(0).row_count() - 1);
    return segment;
}

} //namespace arcticdb

namespace fmt {
template<>
struct formatter<ArrowType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(ArrowType arrow_type, FormatContext &ctx) const {
        return format_to(ctx.out(), "{:c}", arrow_type);
    }
};

} //namespace fmt