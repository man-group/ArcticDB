/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>

#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <pipeline/read_frame.hpp>

TEST(Append, Simple) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_append"};
    auto wrapper = get_test_simple_frame(stream_id, 10, 0);
    auto& frame = wrapper.frame_;
    auto desc = frame.desc.clone();
    append_incomplete(store, stream_id, std::move(frame));
    pipelines::FilterRange range;
    auto pipeline_context = std::make_shared<PipelineContext>(desc);
    pipeline_context->selected_columns_ = util::BitSet(2);
    pipeline_context->selected_columns_ ->flip();
    pipeline_context->fetch_index_ = util::BitSet(2);
    pipeline_context->fetch_index_.flip();
    async::TaskScheduler scheduler{5};

    pipeline_context->slice_and_keys_ = arcticdb::stream::get_incomplete(store, stream_id, range, 0, false, false);
    generate_filtered_field_descriptors(pipeline_context, {});

    SegmentInMemory allocated_frame = allocate_frame(pipeline_context);
    ASSERT_EQ(allocated_frame.row_count(), size_t(frame.num_rows));
}

TEST(Append, MergeDescriptorsPromote) {
    using namespace arcticdb;

    StreamId id{"test_desc"};
    IndexDescriptor idx{1u, IndexDescriptor::TIMESTAMP};

    std::vector<FieldDescriptor> fields {
        FieldDescriptor{scalar_field_proto(DataType::MICROS_UTC64, "time")},
        FieldDescriptor{scalar_field_proto(DataType::INT8, "int8")},
        FieldDescriptor{scalar_field_proto(DataType::INT16, "int16")},
        FieldDescriptor{scalar_field_proto(DataType::UINT8, "uint8")},
        FieldDescriptor{scalar_field_proto(DataType::UINT16, "uint16")}
    };

    StreamDescriptor original{
        id, idx, fields_proto_from_range(fields)
    };

    std::vector<std::vector<FieldDescriptor>> new_fields {{
        FieldDescriptor{scalar_field_proto(DataType::MICROS_UTC64, "time")},
        FieldDescriptor{scalar_field_proto(DataType::INT16, "int8")},
        FieldDescriptor{scalar_field_proto(DataType::INT32, "int16")},
        FieldDescriptor{scalar_field_proto(DataType::UINT16, "uint8")},
        FieldDescriptor{scalar_field_proto(DataType::UINT32, "uint16")}
    }};

    auto new_desc = stream::merge_descriptors(original, { fields_proto_from_range(new_fields[0]) }, std::vector<std::string>{});
    auto new_proto = fields_proto_from_range(new_fields[0]);
    google::protobuf::util::MessageDifferencer diff;
    auto result = std::equal(std::begin(new_desc.fields()), std::end(new_desc.fields()), std::begin(new_proto), std::end(new_proto), [&diff]
        (const auto& left, const auto& right) {
        return diff.Compare(left, right);
    });
    ASSERT_EQ(result, true);
}


TEST(Append, MergeDescriptorsNoPromote) {
    using namespace arcticdb;

    StreamId id{"test_desc"};
    IndexDescriptor idx{1u, IndexDescriptor::TIMESTAMP};

    std::vector<FieldDescriptor> fields {
        FieldDescriptor{scalar_field_proto(DataType::MICROS_UTC64, "time")},
        FieldDescriptor{scalar_field_proto(DataType::INT8, "int8")},
        FieldDescriptor{scalar_field_proto(DataType::INT16, "int16")},
        FieldDescriptor{scalar_field_proto(DataType::UINT8, "uint8")},
        FieldDescriptor{scalar_field_proto(DataType::UINT16, "uint16")}
    };

    StreamDescriptor original{
        id, idx, fields_proto_from_range(fields)
    };

    std::vector<std::vector<FieldDescriptor>> new_fields {{
      FieldDescriptor{scalar_field_proto(DataType::MICROS_UTC64, "time")},
      FieldDescriptor{scalar_field_proto(DataType::INT8, "int8")},
      FieldDescriptor{scalar_field_proto(DataType::INT16, "int16")},
      FieldDescriptor{scalar_field_proto(DataType::UINT8, "uint8")},
      FieldDescriptor{scalar_field_proto(DataType::UINT16, "uint16")}
    }};

    auto new_desc = stream::merge_descriptors(original, { fields_proto_from_range(new_fields[0]) }, std::vector<std::string>{});
    google::protobuf::util::MessageDifferencer diff;
    auto result = std::equal(std::begin(new_desc.fields()), std::end(new_desc.fields()), std::begin(original), std::end(original), [&diff]
    (const auto& left, const auto& right) {
        return diff.Compare(left, right);
    });
    ASSERT_EQ(result, true);
}


