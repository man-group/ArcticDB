/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/arrow/array_from_block.hpp>


std::vector<sparrow::array> make_array_list(const std::size_t data_size)
{
    std::uint16_t* data = new std::uint16_t[data_size];
    for (std::size_t i = 0; i < data_size; ++i)
    {
        data[i] = static_cast<std::uint16_t>(i);
    }
    sparrow::u8_buffer<std::uint16_t> buffer0(data, data_size);

    sparrow::primitive_array<std::uint16_t> pr0(std::move(buffer0), "column0");
    sparrow::primitive_array<std::int32_t> pr1(
        std::ranges::iota_view{std::int32_t(4), 4 + std::int32_t(data_size)},
        "column1"
    );
    sparrow::primitive_array<std::int32_t> pr2(
        std::ranges::iota_view{std::int32_t(2), 2 + std::int32_t(data_size)},
        "column2"
    );

    std::vector<sparrow::array> arr_list = {sparrow::array(std::move(pr0)), sparrow::array(std::move(pr1)), sparrow::array(std::move(pr2))};
    return arr_list;
}

std::vector<std::string> make_name_list()
{
    std::vector<std::string> name_list = {"first", "second", "third"};
    return name_list;
}

sparrow::record_batch make_record_batch(const std::size_t data_size)
{
    return sparrow::record_batch(make_name_list(), make_array_list(data_size));
}

TEST(Arrow, SparrowBasic) {
    auto record_batch = make_record_batch(20);
}

TEST(Arrow, ConvertColumn) {
    using namespace arcticdb;
    constexpr size_t data_size = 20;
    Column column(make_scalar_type(DataType::UINT64), data_size, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    auto ptr = reinterpret_cast<uint64_t*>(column.buffer().data());
    for(auto i = 0UL; i < data_size; ++i) {
        *ptr++ = i;
    }

    auto data = const_cast<uint64_t*>(column.data().next<ScalarTagType<DataTypeTag<DataType::UINT64>>>()->release());
    sparrow::u8_buffer<std::uint64_t> buffer0(data, data_size);
    sparrow::primitive_array<std::uint64_t> pr0(std::move(buffer0), "column0");
    std::vector<sparrow::array> arr_list = {sparrow::array(std::move(pr0))};
    std::vector<std::string> names{"column0"};
    auto record_batch = sparrow::record_batch(std::move(names), std::move(arr_list));
}

TEST(Arrow, ArrayFromColumn) {
    using namespace arcticdb;
    constexpr size_t data_size = 20;
    Column column(make_scalar_type(DataType::UINT64), data_size, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    auto ptr = reinterpret_cast<uint64_t*>(column.buffer().data());
    for(auto i = 0UL; i < data_size; ++i) {
        *ptr++ = i;
    }
    std::vector<sparrow::array> vec;
    auto column_data = column.data();
    std::string_view name{"column0"};
    column.type().visit_tag([&vec, &column_data, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        while (auto block = column_data.next<TagType>()) {
            vec.emplace_back(arrow_array_from_block<TagType>(*block, name, std::nullopt));
        }
    });
}

TEST(Arrow, RecordBatchFromColumn) {
    using namespace arcticdb;
    constexpr size_t data_size = 20;
    Column column(make_scalar_type(DataType::UINT64), data_size, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    auto ptr = reinterpret_cast<uint64_t*>(column.buffer().data());
    for(auto i = 0UL; i < data_size; ++i) {
        *ptr++ = i;
    }
    std::vector<sparrow::array> vec;
    auto column_data = column.data();
    std::string_view name{"column0"};
    column.type().visit_tag([&vec, &column_data, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        while (auto block = column_data.next<TagType>()) {
            vec.emplace_back(arrow_array_from_block<TagType>(*block, name, std::nullopt));
        }
    });

    auto record_batch = sparrow::record_batch{};
    record_batch.add_column(std::string{"name"}, std::move(vec[0]));
}

sparrow::array add_column(std::string_view name) {

    using namespace arcticdb;
    constexpr size_t data_size = 20;
    Column column(make_scalar_type(DataType::UINT64), data_size, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    auto ptr = reinterpret_cast<uint64_t*>(column.buffer().data());
    for(auto i = 0UL; i < data_size; ++i) {
        *ptr++ = i;
    }

    auto column_data = column.data();
    return column.type().visit_tag([&column_data, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        auto block = column_data.next<TagType>();
        return arrow_array_from_block<TagType>(*block, name, std::nullopt);
    });
}

std::shared_ptr<std::vector<sparrow::record_batch>> vector_of_record_batch() {
    std::vector<sparrow::array> arrays;
    arrays.reserve(3);
    for(auto i = 0UL; i < 3; ++i) {
        arrays.emplace_back(add_column(fmt::format("column_{}", i)));
    }

    auto output = std::make_shared<std::vector<sparrow::record_batch>>();
    for(auto i = 0; i < 3; ++i)
        output->emplace_back(sparrow::record_batch{});

    for(auto i = 0UL; i < 3; ++i) {
       (*output)[0].add_column(fmt::format("column_{}", i), std::move(arrays[0]));
    }

    return output;
}

TEST(Arrow, ReturnVectorOfRecordBatch) {
    auto ptr_to_vector = vector_of_record_batch();
}

TEST(Arrow, ConvertSegment) {
    using namespace arcticdb;
    auto desc = stream_descriptor(StreamId{"id"}, stream::RowCountIndex{}, {
        scalar_field(DataType::UINT8, "uint8"),
        scalar_field(DataType::UINT32, "uint32")});

    SegmentInMemory segment(desc, 20, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, OutputFormat::ARROW, DataTypeMode::INTERNAL);

    auto col1_ptr = segment.column(0).buffer().data();
    auto col2_ptr = reinterpret_cast<uint32_t*>(segment.column(1).buffer().data());
    for(auto j = 0; j < 20; ++j ) {
        *col1_ptr++ = j;
        *col2_ptr++ = j * 2;
    }

    auto vec = segment_to_arrow_data(segment);
    ASSERT_EQ(vec->size(), 1);
}

using keys_type = uint32_t;
using layout_type = sparrow::dictionary_encoded_array<keys_type>;

sparrow::nullable<std::string_view> get_dict_value(layout_type::const_reference r) {
    return std::get<sparrow::nullable<std::string_view>>(r);
}

TEST(StringDictionary, CreateFromStringPool) {
    std::vector<uint32_t> offsets = {0, 1, 0, 2, 1};
    std::vector<std::string_view> unique_strings = {"hello", "world", "test"};

    sparrow::string_array dict_values(unique_strings);
    sparrow::array dict_array(std::move(dict_values));

    std::vector<size_t> null_positions;
    sparrow::dictionary_encoded_array<uint32_t> dict_encoded(
        sparrow::dictionary_encoded_array<uint32_t>::keys_buffer_type(offsets),
        std::move(dict_array),
        std::move(null_positions)
    );

    ASSERT_EQ(dict_encoded.size(), offsets.size());
    ASSERT_EQ(get_dict_value(dict_encoded[0]).value(), "hello");
    ASSERT_EQ(get_dict_value(dict_encoded[1]).value(), "world");
    ASSERT_EQ(get_dict_value(dict_encoded[2]).value(), "hello");
    ASSERT_EQ(get_dict_value(dict_encoded[3]).value(), "test");
    ASSERT_EQ(get_dict_value(dict_encoded[4]).value(), "world");
}

TEST(StringDictionary, CreateFromStringPoolZeroCopy) {
    using namespace arcticdb;
    StringPool pool;
    std::vector<uint32_t> string_offsets;  // indices into the pool
    std::vector<position_t> unique_positions;  // positions of unique strings
    ankerl::unordered_dense::map<std::string_view, uint32_t> unique_map;

    for (const auto& str : {"hello", "world", "hello", "test", "world"}) {
        auto offset_str = pool.get(str);
        auto pos = offset_str.offset();

        auto [it, inserted] = unique_map.try_emplace(
            pool.get_const_view(pos),
            static_cast<uint32_t>(unique_positions.size())
        );

        string_offsets.push_back(it->second);
        if (inserted) {
            unique_positions.push_back(pos);
        }
    }

    size_t total_size = 0;
    for (auto pos : unique_positions) {
        total_size += pool.get_const_view(pos).size();
    }

    sparrow::u8_buffer<char> data_buffer(total_size);
    sparrow::u8_buffer<int32_t> offset_buffer(unique_positions.size() + 1, 0);

    size_t current_pos = 0;
    offset_buffer[0] = 0;
    for (size_t i = 0; i < unique_positions.size(); ++i) {
        auto sv = pool.get_const_view(unique_positions[i]);
        std::memcpy(data_buffer.data() + current_pos, sv.data(), sv.size());
        current_pos += sv.size();
        offset_buffer[i + 1] = current_pos;
    }

    sparrow::string_array dict_values(
        std::move(data_buffer),
        std::move(offset_buffer)
    );
    sparrow::array dict_array(std::move(dict_values));

    sparrow::dictionary_encoded_array<uint32_t> dict_encoded(
        sparrow::dictionary_encoded_array<uint32_t>::keys_buffer_type(string_offsets),
        std::move(dict_array),
        std::vector<size_t>{}
    );

    ASSERT_EQ(dict_encoded.size(), 5);
    ASSERT_EQ(get_dict_value(dict_encoded[0]).value(), "hello");
    ASSERT_EQ(get_dict_value(dict_encoded[1]).value(), "world");
    ASSERT_EQ(get_dict_value(dict_encoded[2]).value(), "hello");
    ASSERT_EQ(get_dict_value(dict_encoded[3]).value(), "test");
    ASSERT_EQ(get_dict_value(dict_encoded[4]).value(), "world");
}




