/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <sparrow/record_batch.hpp>

#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/arrow/test/arrow_test_utils.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_handlers.hpp>
#include <arcticdb/util/allocator.hpp>

using namespace arcticdb;

SegmentInMemory get_detachable_segment(
        std::span<const FieldRef> fields, size_t num_rows, size_t chunk_size, const ReadOptions& read_options
) {
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("symbol", fields);
    auto& fields_with_index = desc.fields();
    auto num_columns = fields_with_index.size();
    auto handler = ArrowStringHandler();
    auto segment = SegmentInMemory();
    for (auto& field : fields_with_index) {
        size_t extra_bytes_per_block = 0;
        if (is_sequence_type(field.type().data_type())) {
            auto [type, extra_bytes] = handler.output_type_and_extra_bytes(field.type(), field.name(), read_options);
            field.type_ = type;
            extra_bytes_per_block = extra_bytes;
        }
        segment.add_column(
                field,
                std::make_shared<Column>(
                        field.type(), 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, extra_bytes_per_block
                )
        );
    }

    for (auto i = 0u; i < num_columns; ++i) {
        auto& column = segment.column(i);
        details::visit_scalar(column.type(), [&column, &num_rows, &chunk_size](auto&& impl) {
            using TagType = std::decay_t<decltype(impl)>;
            using RawType = typename TagType::DataTypeTag::raw_type;
            allocate_and_fill_chunked_column<RawType>(column, num_rows, chunk_size);
        });
    }

    return segment;
}

// Populates a column with strings as if using OutputFormat::ARROW. Assumes column is already allocated.
void fill_chunked_string_column(
        Column& column, std::string_view col_name, size_t num_rows, size_t chunk_size,
        std::shared_ptr<StringPool> string_pool, const std::vector<std::string>& values, const ReadOptions& read_options
) {
    auto num_chunks = num_rows / chunk_size + (num_rows % chunk_size != 0);

    std::vector<position_t> string_pool_offsets;
    for (auto& str : values) {
        string_pool_offsets.push_back(string_pool->get(str).offset());
    }

    // Use arrow string handler to populate the column in arrow format chunk by chunk
    auto handler = ArrowStringHandler();
    auto source_type_desc = TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::Dim0};
    auto dest_type_desc = handler.output_type_and_extra_bytes(source_type_desc, col_name, read_options).first;
    for (auto chunk = 0u; chunk < num_chunks; ++chunk) {
        auto row_count = std::min(chunk_size, num_rows - chunk * chunk_size);
        // To use the `handler.convert_type` we prepare the source data for each chunk in `source_column`.
        // We fill the column with pointers to the string pool.
        auto source_column = Column(source_type_desc, row_count, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
        for (auto row_in_chunk = 0u; row_in_chunk < row_count; ++row_in_chunk) {
            auto global_row = chunk * chunk_size + row_in_chunk;
            source_column.push_back(string_pool_offsets[global_row]);
        }

        auto dest_size = data_type_size(dest_type_desc);
        auto handler_data = std::any{};
        auto field_wrapper = FieldWrapper(dest_type_desc, col_name);
        auto column_mapping = ColumnMapping(
                source_type_desc,
                dest_type_desc,
                field_wrapper.field(),
                dest_size,
                row_count,
                chunk * chunk_size,
                dest_size * chunk * chunk_size,
                dest_size * row_count,
                0
        );

        handler.convert_type(
                source_column, column, column_mapping, DecodePathData{}, handler_data, string_pool, read_options
        );
    }
}

TEST(ArrowRead, ZeroCopy) {
    size_t num_rows{10};
    uint8_t* data_ptr = std::allocator<uint8_t>().allocate(sizeof(uint64_t) * num_rows);
    auto typed_ptr = reinterpret_cast<uint64_t*>(data_ptr);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        typed_ptr[idx] = idx;
    }
    sparrow::u8_buffer<uint64_t> u8_buffer(typed_ptr, num_rows, get_detachable_allocator());
    sparrow::primitive_array<uint64_t> primitive_array(std::move(u8_buffer), num_rows);
    sparrow::array array{std::move(primitive_array)};
    auto arrow_structures = sparrow::get_arrow_structures(array);
    auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_structures.first, *arrow_structures.second);
    const auto* roundtripped_ptr = reinterpret_cast<uint64_t*>(arrow_array_buffers.at(1).data<uint8_t>());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        ASSERT_EQ(typed_ptr[idx], idx);
        ASSERT_EQ(roundtripped_ptr[idx], idx);
    }
    ASSERT_EQ(roundtripped_ptr, typed_ptr);
}

TEST(ArrowRead, ColumnBasic) {
    const size_t num_rows = 100;
    const size_t chunk_size = 5;
    const size_t num_chunks = num_rows / chunk_size;
    auto column = Column(
            TypeDescriptor{DataType::FLOAT32, Dimension::Dim0}, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED
    );
    allocate_and_fill_chunked_column<float>(column, num_rows, chunk_size);
    auto arrow_arrays = arrow_arrays_from_column(column, "col");
    EXPECT_EQ(arrow_arrays.size(), num_chunks);
    for (const auto& arr : arrow_arrays) {
        EXPECT_EQ(arr.name(), "col");
    }
    for (auto row = 0u; row < num_rows; ++row) {
        auto chunk = row / chunk_size;
        auto pos = row % chunk_size;
        EXPECT_EQ(std::get<sparrow::nullable<const float&>>(arrow_arrays[chunk][pos]).get(), static_cast<float>(row));
    }
}

class ArrowStringColumnRead : public testing::TestWithParam<ArrowOutputStringFormat> {
  public:
    ArrowOutputStringFormat output_string_format() { return GetParam(); }
};

TEST_P(ArrowStringColumnRead, Basic) {
    const size_t num_rows = 100;
    const size_t chunk_size = 5;
    const size_t num_chunks = num_rows / chunk_size;
    const std::vector<std::string> strings = {"test", "strings", "available", "!"};

    std::vector<std::string> column_values;
    column_values.reserve(num_rows);
    for (auto i = 0u; i < num_rows; ++i) {
        column_values.push_back(strings[i % strings.size()]);
    }

    // Populate string pool and column using arrow string handler
    auto pool = std::make_shared<StringPool>();
    auto handler = ArrowStringHandler();
    auto column_name = "col";
    auto read_options = ReadOptions();
    read_options.set_output_format(OutputFormat::ARROW);
    read_options.set_arrow_output_default_string_format(output_string_format());
    auto [type_desc, extra_bytes] = handler.output_type_and_extra_bytes(
            TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::Dim0}, column_name, read_options
    );
    auto column = Column(type_desc, 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, extra_bytes);
    allocate_chunked_column(column, num_rows, chunk_size);
    fill_chunked_string_column(column, column_name, num_rows, chunk_size, pool, column_values, read_options);

    // Verify applying the string handler sets the correct external buffers
    for (auto chunk = 0u; chunk < num_chunks; ++chunk) {
        auto dest_size = data_type_size(type_desc);
        if (output_string_format() == ArrowOutputStringFormat::CATEGORICAL) {
            EXPECT_EQ(dest_size, 4); // We should be using 32-bit integer keys for categorical arrow

            // We should have attached offsets and string buffers to the corresponding offsets.
            auto offset = chunk * chunk_size * dest_size;
            EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::OFFSET));
            EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::STRING));
            auto& column_buffer = column.buffer();
            auto& offset_buffer = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
            auto& string_buffer = column.get_extra_buffer(offset, ExtraBufferType::STRING);

            for (auto row_in_chunk = 0u; row_in_chunk < chunk_size; ++row_in_chunk) {
                auto global_row = chunk * chunk_size + row_in_chunk;
                auto id = column_buffer.cast<int32_t>(global_row);
                auto offset_begin = offset_buffer.cast<int64_t>(id);
                auto str_size = offset_buffer.cast<int64_t>(id + 1) - offset_begin;
                auto str_in_column = std::string_view(
                        reinterpret_cast<char*>(string_buffer.bytes_at(offset_begin, str_size)), str_size
                );
                EXPECT_EQ(str_in_column, column_values[global_row]);
            }
        } else {
            // We should be using 64-bit offsets for LARGE_STRING and 32-bit offsets for SMALL_STRING
            details::visit_scalar(column.type(), [&](auto&& impl) {
                using TagType = std::decay_t<decltype(impl)>;
                if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
                    using RawType = typename TagType::DataTypeTag::raw_type;
                    auto expected_dest_size = output_string_format() == ArrowOutputStringFormat::LARGE_STRING ? 8 : 4;
                    EXPECT_EQ(dest_size, expected_dest_size);
                    EXPECT_EQ(sizeof(RawType), expected_dest_size);

                    // We should have attached only a string buffer to the corresponding offset because we're storing
                    // the offsets in the column buffer.
                    auto offset = chunk * chunk_size * dest_size;
                    EXPECT_FALSE(column.has_extra_buffer(offset, ExtraBufferType::OFFSET));
                    EXPECT_TRUE(column.has_extra_buffer(offset, ExtraBufferType::STRING));
                    auto& column_buffer = column.buffer();
                    auto column_chunk_ptr = reinterpret_cast<RawType*>(
                            column_buffer.bytes_at(chunk * chunk_size * dest_size, chunk_size * dest_size)
                    );
                    auto& string_buffer = column.get_extra_buffer(offset, ExtraBufferType::STRING);

                    for (auto row_in_chunk = 0u; row_in_chunk < chunk_size; ++row_in_chunk) {
                        auto global_row = chunk * chunk_size + row_in_chunk;
                        auto offset_begin = column_chunk_ptr[row_in_chunk];
                        auto str_size = column_chunk_ptr[row_in_chunk + 1] - offset_begin;
                        auto str_in_column = std::string_view(
                                reinterpret_cast<char*>(string_buffer.bytes_at(offset_begin, str_size)), str_size
                        );
                        EXPECT_EQ(str_in_column, column_values[global_row]);
                    }
                } else {
                    util::raise_rte("Unexpected non-string type");
                }
            });
        }
    }

    // Convert to arrow arrays
    auto arrow_arrays = arrow_arrays_from_column(column, column_name);

    // Verify the dict arrays
    EXPECT_EQ(arrow_arrays.size(), num_chunks);
    for (const auto& arr : arrow_arrays) {
        EXPECT_EQ(arr.name(), column_name);
    }
    for (auto row = 0u; row < num_rows; ++row) {
        auto chunk = row / chunk_size;
        auto pos = row % chunk_size;
        auto value = arrow_arrays[chunk][pos];
        EXPECT_TRUE(value.has_value());
        EXPECT_EQ(std::get<sparrow::nullable<std::string_view>>(value).get(), column_values[row]);
    }
}

INSTANTIATE_TEST_SUITE_P(
        AllStringFormats, ArrowStringColumnRead,
        testing::Values(
                ArrowOutputStringFormat::CATEGORICAL, ArrowOutputStringFormat::LARGE_STRING,
                ArrowOutputStringFormat::SMALL_STRING
        )
);

TEST(ArrowRead, ConvertSegmentBasic) {
    const auto num_rows = 100u;
    const auto chunk_size = 10u;
    const auto num_chunks = num_rows / chunk_size;
    const auto fields = std::array{
            scalar_field(DataType::UINT8, "smallints"),
            scalar_field(DataType::INT64, "bigints"),
            scalar_field(DataType::FLOAT64, "floats"),
    };
    auto segment = get_detachable_segment(fields, num_rows, chunk_size, ReadOptions{});
    // Verify the index column has the expected number of chunks
    EXPECT_EQ(segment.column(0).num_blocks(), num_chunks);

    auto arrow_data = segment_to_arrow_data(segment);
    // We expect to see num_chunks record batches
    EXPECT_EQ(arrow_data->size(), num_chunks);
    for (const auto& record_batch : *arrow_data) {
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        EXPECT_EQ(names[1], "smallints");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::UINT8);
        EXPECT_EQ(names[2], "bigints");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::INT64);
        EXPECT_EQ(names[3], "floats");
        EXPECT_EQ(columns[3].data_type(), sparrow::data_type::DOUBLE);
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), chunk_size);
        }
    }
}

void assert_arrow_string_array_as_expected(const sparrow::array& arr, const std::span<std::string>& expected) {
    EXPECT_EQ(arr.size(), expected.size());
    for (auto i = 0u; i < arr.size(); i++) {
        const auto value = std::get<sparrow::nullable<std::string_view>>(arr[i]);
        EXPECT_TRUE(value.has_value());
        EXPECT_EQ(value.get(), std::string_view(expected[i]));
    }
}

TEST(ArrowRead, ConvertSegmentMultipleStringColumns) {
    const auto num_rows = 100u;
    const auto chunk_size = 19u;
    const auto num_chunks = num_rows / chunk_size + (num_rows % chunk_size != 0);
    const auto fields = std::array{
            scalar_field(DataType::FLOAT64, "floats"),
            scalar_field(DataType::UTF_DYNAMIC64, "str_1"),
            scalar_field(DataType::UTF_DYNAMIC64, "str_2"),
    };
    // We populate string columns so they have 30 different and 70 common strings.
    const auto str_id_offset = 30u;
    std::vector<std::vector<std::string>> string_values(2);
    for (auto row = 0u; row < num_rows; ++row) {
        string_values[0].emplace_back(fmt::format("string_{}", row));
        string_values[1].emplace_back(fmt::format("string_{}", str_id_offset + row));
    }
    auto read_options = ReadOptions();
    read_options.set_output_format(OutputFormat::ARROW);
    read_options.set_arrow_output_default_string_format(ArrowOutputStringFormat::CATEGORICAL);
    auto per_column_string_format =
            std::unordered_map<std::string, ArrowOutputStringFormat>{{"str_2", ArrowOutputStringFormat::LARGE_STRING}};
    read_options.set_arrow_output_per_column_string_format(per_column_string_format);
    auto segment = get_detachable_segment(fields, num_rows, chunk_size, read_options);
    auto string_pool = std::make_shared<StringPool>();
    fill_chunked_string_column(
            segment.column(2), "str_1", num_rows, chunk_size, string_pool, string_values[0], read_options
    );
    fill_chunked_string_column(
            segment.column(3), "str_2", num_rows, chunk_size, string_pool, string_values[1], read_options
    );
    segment.set_string_pool(string_pool);

    // Convert to arrow
    auto arrow_data = segment_to_arrow_data(segment);
    EXPECT_EQ(arrow_data->size(), num_chunks);
    for (auto i = 0u; i < num_chunks; ++i) {
        auto row_count = std::min(chunk_size, num_rows - i * chunk_size);
        const auto& record_batch = (*arrow_data)[i];
        auto names = record_batch.names();
        auto columns = record_batch.columns();
        // Each record batch should have all columns for the row range (including the index)
        EXPECT_EQ(names.size(), fields.size() + 1);
        EXPECT_EQ(columns.size(), fields.size() + 1);
        EXPECT_EQ(names[0], "time");
        EXPECT_EQ(columns[0].data_type(), sparrow::data_type::TIMESTAMP_NANOSECONDS);
        EXPECT_EQ(names[1], "floats");
        EXPECT_EQ(columns[1].data_type(), sparrow::data_type::DOUBLE);
        EXPECT_EQ(names[2], "str_1");
        EXPECT_EQ(columns[2].data_type(), sparrow::data_type::INT32); // The dict array keys are INT32s
        assert_arrow_string_array_as_expected(
                columns[2], std::span(string_values[0]).subspan(i * chunk_size, row_count)
        );
        EXPECT_EQ(names[3], "str_2");
        EXPECT_EQ(columns[3].data_type(), sparrow::data_type::LARGE_STRING); // The offset values are INT64s
        assert_arrow_string_array_as_expected(
                columns[3], std::span(string_values[1]).subspan(i * chunk_size, row_count)
        );
        for (const auto& col : columns) {
            EXPECT_EQ(col.size(), row_count);
        }
    }
}