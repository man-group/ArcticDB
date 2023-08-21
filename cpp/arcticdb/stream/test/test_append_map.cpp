/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/pipeline/read_frame.hpp>

TEST(Append, Simple) {
    using namespace arcticdb;
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

    pipeline_context->slice_and_keys_ = arcticdb::get_incomplete(store, stream_id, range, 0, false, false);
    generate_filtered_field_descriptors(pipeline_context, {});

    SegmentInMemory allocated_frame = allocate_frame(pipeline_context);
    ASSERT_EQ(allocated_frame.row_count(), size_t(frame.num_rows));
}

TEST(Append, MergeDescriptorsPromote) {
    using namespace arcticdb;

    StreamId id{"test_desc"};
    IndexDescriptor idx{1u, IndexDescriptor::TIMESTAMP};

    std::vector<FieldRef> fields {
        scalar_field(DataType::NANOSECONDS_UTC64, "time"),
        scalar_field(DataType::INT8, "int8"),
        scalar_field(DataType::INT16, "int16"),
        scalar_field(DataType::UINT8, "uint8"),
        scalar_field(DataType::UINT16, "uint16")
    };

    StreamDescriptor original{
        id, idx, std::make_shared<FieldCollection>(fields_from_range(fields))
    };

    auto get_new_fields = [] () {
        std::vector<std::vector<FieldRef>> new_fields {{
            scalar_field(DataType::NANOSECONDS_UTC64, "time"),
            scalar_field(DataType::INT16, "int8"),
            scalar_field(DataType::INT32, "int16"),
            scalar_field(DataType::UINT16, "uint8"),
            scalar_field(DataType::UINT32, "uint16")
        }};
        return new_fields;
    };

    std::vector<std::shared_ptr<FieldCollection>> new_desc_fields;
    new_desc_fields.emplace_back(std::make_shared<FieldCollection>(fields_from_range(get_new_fields()[0])));
    auto new_desc = merge_descriptors(original, std::move(new_desc_fields), std::vector<std::string>{});
    new_desc_fields.emplace_back(std::make_shared<FieldCollection>(fields_from_range(get_new_fields()[0])));

    auto result = std::equal(std::begin(new_desc.fields()), std::end(new_desc.fields()), std::begin(*new_desc_fields[0]), std::end(*new_desc_fields[0]), []
        (const auto& left, const auto& right) {
        return left == right;
    });
    ASSERT_EQ(result, true);
}

TEST(Append, MergeDescriptorsNoPromote) {
    using namespace arcticdb;

    StreamId id{"test_desc"};
    IndexDescriptor idx{1u, IndexDescriptor::TIMESTAMP};

    std::vector<FieldRef> fields {
        scalar_field(DataType::NANOSECONDS_UTC64, "time"),
        scalar_field(DataType::INT8, "int8"),
        scalar_field(DataType::INT16, "int16"),
        scalar_field(DataType::UINT8, "uint8"),
        scalar_field(DataType::UINT16, "uint16")
    };

    StreamDescriptor original{
        id, idx, std::make_shared<FieldCollection>(fields_from_range(fields))
    };

    std::vector<std::vector<FieldRef>> new_fields {{
      scalar_field(DataType::NANOSECONDS_UTC64, "time"),
      scalar_field(DataType::INT8, "int8"),
      scalar_field(DataType::INT16, "int16"),
      scalar_field(DataType::UINT8, "uint8"),
      scalar_field(DataType::UINT16, "uint16")
    }};

    std::vector<std::shared_ptr<FieldCollection>> new_desc_fields;
    new_desc_fields.emplace_back(std::make_shared<FieldCollection>(fields_from_range(new_fields[0])));
    auto new_desc = merge_descriptors(original, std::move(new_desc_fields), std::vector<std::string>{});
    auto result = std::equal(std::begin(new_desc.fields()), std::end(new_desc.fields()), std::begin(original), std::end(original), []
    (const auto& left, const auto& right) {
        return left == right;
    });
    ASSERT_EQ(result, true);
}


