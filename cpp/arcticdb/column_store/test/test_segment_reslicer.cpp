/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/column_store/segment_reslicer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/string_utils.hpp>

using namespace arcticdb;

template<typename type>
class SegmentReslicerDenseNumericStaticSchemaFixture : public testing::Test {};

using test_types =
        ::testing::Types<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;

TYPED_TEST_SUITE(SegmentReslicerDenseNumericStaticSchemaFixture, test_types);

TYPED_TEST(SegmentReslicerDenseNumericStaticSchemaFixture, CombineIntoOne) {
    using RawType = TypeParam;
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<RawType>());
    std::vector<std::shared_ptr<Column>> input_columns;
    if constexpr (std::is_same_v<RawType, bool>) {
        std::vector<std::vector<uint8_t>> input_data{{0, 0, 1, 1}, {1, 0, 1}, {1, 0, 0, 0, 1}};
        for (const auto& data : input_data) {
            Column col{type_descriptor, data.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
            memcpy(col.ptr(), data.data(), data.size() * sizeof(uint8_t));
            input_columns.emplace_back(std::make_shared<Column>(std::move(col)));
        }
    } else {
        std::vector<std::vector<RawType>> input_data{{1, 2, 3, 4}, {11, 12, 13}, {101, 102, 103, 104, 105}};
        for (const auto& data : input_data) {
            Column col{type_descriptor, data.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
            memcpy(col.ptr(), data.data(), data.size() * sizeof(RawType));
            input_columns.emplace_back(std::make_shared<Column>(std::move(col)));
        }
    }
    uint64_t total_rows{12};
    SegmentReslicer reslicer{total_rows};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, total_rows};
    auto res = reslicer.reslice_dense_numeric_static_schema_columns(std::move(input_columns), slicing_info);
    ASSERT_EQ(res.size(), 1);
    ASSERT_TRUE(res.front().has_value());
    auto& col = *res.front();
    ASSERT_EQ(col.row_count(), total_rows);
    // This happens for the whole segment later in the real flow
    col.set_row_data(total_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    if constexpr (std::is_same_v<RawType, bool>) {
        std::vector<bool> expected_values{false, false, true, true, true, false, true, true, false, false, false, true};
        for (size_t idx = 0; idx < total_rows; ++idx) {
            auto opt_val = col.scalar_at<RawType>(idx);
            ASSERT_TRUE(opt_val.has_value());
            ASSERT_EQ(*opt_val, expected_values.at(idx));
        }
    } else {
        std::vector<RawType> expected_values{1, 2, 3, 4, 11, 12, 13, 101, 102, 103, 104, 105};
        for (size_t idx = 0; idx < total_rows; ++idx) {
            auto opt_val = col.scalar_at<RawType>(idx);
            ASSERT_TRUE(opt_val.has_value());
            ASSERT_EQ(*opt_val, expected_values.at(idx));
        }
    }
    ASSERT_EQ(col.buffer().bytes(), total_rows * sizeof(RawType));
    ASSERT_EQ(col.num_blocks(), 1);
    ASSERT_EQ(col.buffer().blocks().front()->capacity(), total_rows * sizeof(RawType));
}

TYPED_TEST(SegmentReslicerDenseNumericStaticSchemaFixture, SplitInTwo) {
    using RawType = TypeParam;
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<RawType>());
    size_t total_rows{7};
    Column input_col{type_descriptor, total_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
    if constexpr (std::is_same_v<RawType, bool>) {
        std::vector<uint8_t> input_data{0, 0, 1, 0, 1, 1, 1};
        memcpy(input_col.ptr(), input_data.data(), input_data.size() * sizeof(RawType));
    } else {
        std::vector<RawType> input_data{11, 12, 13, 14, 15, 16, 17};
        memcpy(input_col.ptr(), input_data.data(), input_data.size() * sizeof(RawType));
    }
    std::vector<std::shared_ptr<Column>> input_columns;
    input_columns.emplace_back(std::make_shared<Column>(std::move(input_col)));
    uint64_t max_rows_per_slice{4};
    uint64_t rows_in_first_slice{total_rows - max_rows_per_slice};
    SegmentReslicer reslicer{max_rows_per_slice};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, max_rows_per_slice};
    auto res = reslicer.reslice_dense_numeric_static_schema_columns(std::move(input_columns), slicing_info);
    ASSERT_EQ(res.size(), 2);
    ASSERT_TRUE(res.front().has_value());
    ASSERT_TRUE(res.back().has_value());
    auto& col_0 = *res.front();
    auto& col_1 = *res.back();
    ASSERT_EQ(col_0.row_count(), rows_in_first_slice);
    ASSERT_EQ(col_1.row_count(), max_rows_per_slice);
    // This happens for the whole segment later in the real flow
    col_0.set_row_data(rows_in_first_slice - 1);
    col_1.set_row_data(max_rows_per_slice - 1);
    ASSERT_FALSE(col_0.is_sparse());
    ASSERT_FALSE(col_1.is_sparse());
    std::vector<RawType> expected_values = []() {
        if constexpr (std::is_same_v<RawType, bool>) {
            return std::vector<RawType>{false, false, true, false, true, true, true};
        } else {
            return std::vector<RawType>{11, 12, 13, 14, 15, 16, 17};
        }
    }();
    for (size_t idx = 0; idx < total_rows; ++idx) {
        auto opt_val = idx < rows_in_first_slice ? col_0.scalar_at<RawType>(idx)
                                                 : col_1.scalar_at<RawType>(idx - rows_in_first_slice);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, expected_values.at(idx));
    }
    ASSERT_EQ(col_0.buffer().bytes(), rows_in_first_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().bytes(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_0.num_blocks(), 1);
    ASSERT_EQ(col_1.num_blocks(), 1);
    ASSERT_EQ(col_0.buffer().blocks().front()->capacity(), rows_in_first_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(RawType));
}

TYPED_TEST(SegmentReslicerDenseNumericStaticSchemaFixture, CombineThreeIntoTwo) {
    using RawType = TypeParam;
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<RawType>());
    uint64_t total_rows{30};

    std::vector<std::shared_ptr<Column>> input_columns;
    if constexpr (std::is_same_v<RawType, bool>) {
        std::vector<std::vector<uint8_t>> input_data{
                {true, false, true, false, true, false, true, false, true, false},
                {true, true, true, true, true, true, true, true, true, true},
                {false, false, false, false, false, false, false, false, false, false}
        };
        for (const auto& data : input_data) {
            Column col{type_descriptor, data.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
            memcpy(col.ptr(), data.data(), data.size() * sizeof(RawType));
            input_columns.emplace_back(std::make_shared<Column>(std::move(col)));
        }
    } else {
        std::vector<std::vector<RawType>> input_data{
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                {10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
                {100, 101, 102, 103, 104, 105, 106, 107, 108, 109}
        };
        for (const auto& data : input_data) {
            Column col{type_descriptor, data.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED};
            memcpy(col.ptr(), data.data(), data.size() * sizeof(RawType));
            input_columns.emplace_back(std::make_shared<Column>(std::move(col)));
        }
    }
    SegmentReslicer reslicer{total_rows};
    uint64_t max_rows_per_slice{15};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, max_rows_per_slice};
    auto res = reslicer.reslice_dense_numeric_static_schema_columns(std::move(input_columns), slicing_info);
    ASSERT_EQ(res.size(), 2);
    ASSERT_TRUE(res.front().has_value());
    ASSERT_TRUE(res.back().has_value());
    auto& col_0 = *res.front();
    auto& col_1 = *res.back();
    ASSERT_EQ(col_0.row_count(), max_rows_per_slice);
    ASSERT_EQ(col_1.row_count(), max_rows_per_slice);
    // This happens for the whole segment later in the real flow
    col_0.set_row_data(max_rows_per_slice - 1);
    col_1.set_row_data(max_rows_per_slice - 1);
    ASSERT_FALSE(col_0.is_sparse());
    ASSERT_FALSE(col_1.is_sparse());
    std::vector<RawType> expected_values = []() {
        if constexpr (std::is_same_v<RawType, bool>) {
            return std::vector<RawType>{true,  false, true,  false, true,  false, true,  false, true,  false,
                                        true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
                                        false, false, false, false, false, false, false, false, false, false};
        } else {
            return std::vector<RawType>{0,  1,  2,  3,  4,  5,   6,   7,   8,   9,   10,  11,  12,  13,  14,
                                        15, 16, 17, 18, 19, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109};
        }
    }();
    for (size_t idx = 0; idx < expected_values.size(); ++idx) {
        auto opt_val = idx < max_rows_per_slice ? col_0.scalar_at<RawType>(idx)
                                                : col_1.scalar_at<RawType>(idx - max_rows_per_slice);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, expected_values.at(idx));
    }
    ASSERT_EQ(col_0.buffer().bytes(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().bytes(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_0.num_blocks(), 1);
    ASSERT_EQ(col_1.num_blocks(), 1);
    ASSERT_EQ(col_0.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(RawType));
}

TEST(SegmentReslicerDenseNumericStaticSchema, MultiBlockColumns) {
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<int64_t>());
    std::vector<int64_t> input_data;
    uint64_t total_rows{3000};
    input_data.resize(total_rows);
    std::iota(input_data.begin(), input_data.end(), 42);
    std::vector<std::shared_ptr<Column>> input_columns;
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    // 3968 bytes == 496 int64s per block, so 3 blocks per input column here
    for (size_t idx = 0; idx < input_data.size() / 3; ++idx) {
        input_columns.at(0)->push_back<int64_t>(input_data.at(idx));
        input_columns.at(1)->push_back<int64_t>(input_data.at(idx + total_rows / 3));
        input_columns.at(2)->push_back<int64_t>(input_data.at(idx + 2 * total_rows / 3));
    }
    ASSERT_TRUE(input_columns.at(0)->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(0)->num_blocks(), 3);
    ASSERT_TRUE(input_columns.at(1)->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(1)->num_blocks(), 3);
    ASSERT_TRUE(input_columns.at(2)->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(2)->num_blocks(), 3);

    SegmentReslicer reslicer{total_rows};
    uint64_t max_rows_per_slice{total_rows / 2};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, max_rows_per_slice};
    auto res = reslicer.reslice_dense_numeric_static_schema_columns(std::move(input_columns), slicing_info);
    ASSERT_EQ(res.size(), 2);
    ASSERT_TRUE(res.front().has_value());
    ASSERT_TRUE(res.back().has_value());
    auto& col_0 = *res.front();
    auto& col_1 = *res.back();
    ASSERT_EQ(col_0.row_count(), max_rows_per_slice);
    ASSERT_EQ(col_1.row_count(), max_rows_per_slice);
    // This happens for the whole segment later in the real flow
    col_0.set_row_data(max_rows_per_slice - 1);
    col_1.set_row_data(max_rows_per_slice - 1);
    ASSERT_FALSE(col_0.is_sparse());
    ASSERT_FALSE(col_1.is_sparse());
    for (size_t idx = 0; idx < input_data.size(); ++idx) {
        auto opt_val = idx < max_rows_per_slice ? col_0.scalar_at<int64_t>(idx)
                                                : col_1.scalar_at<int64_t>(idx - max_rows_per_slice);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, input_data.at(idx));
    }
    ASSERT_EQ(col_0.buffer().bytes(), max_rows_per_slice * sizeof(int64_t));
    ASSERT_EQ(col_1.buffer().bytes(), max_rows_per_slice * sizeof(int64_t));
    ASSERT_EQ(col_0.num_blocks(), 1);
    ASSERT_EQ(col_1.num_blocks(), 1);
    ASSERT_EQ(col_0.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(int64_t));
    ASSERT_EQ(col_1.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(int64_t));
}

// Test strings separately as they are quite different
class SegmentReslicerDenseStringStaticSchema : public ::testing::Test {
  protected:
    ColumnWithStrings column_with_strings(const std::vector<std::string>& strings, DataType data_type) {
        Column col{make_scalar_type(data_type), Sparsity::NOT_PERMITTED};
        auto string_pool = std::make_shared<StringPool>();
        details::visit_type(data_type, [&](auto tag) {
            using type_info = ScalarTypeInfo<decltype(tag)>;
            // True by construction
            if constexpr (is_sequence_type(type_info::data_type)) {
                if constexpr (is_dynamic_string_type(type_info::data_type)) {
                    for (const auto& str : strings) {
                        col.push_back(string_pool->get(str).offset());
                    }
                } else { // Fixed-width
                    // Needed to pad with null bytes to the width of the widest string, as this is how numpy fixed-width
                    // strings work
                    auto width =
                            std::ranges::max_element(strings, [](const std::string& left, const std::string& right) {
                                return left.size() < right.size();
                            })->size();
                    if constexpr (is_utf_type(type_info::data_type)) {
                        for (const auto& str : strings) {
                            auto str32 = util::utf8_to_u32(str);
                            str32.resize(width, '\0\0\0\0');
                            // The string pool uses string views even to represent UTF32
                            std::string_view strv{reinterpret_cast<const char*>(str32.data()), 4 * str32.size()};
                            col.push_back(string_pool->get(strv).offset());
                        }
                    } else { // ASCII
                        for (const auto& str : strings) {
                            auto str_copy = str;
                            str_copy.resize(width, '\0');
                            col.push_back(string_pool->get(str_copy).offset());
                        }
                    }
                }
            }
        });
        return {std::move(col), string_pool, "dummy"};
    };

    TypeDescriptor ascii_fixed_td{make_scalar_type(DataType::ASCII_FIXED64)};
    TypeDescriptor ascii_dynamic_td{make_scalar_type(DataType::ASCII_DYNAMIC64)};
    TypeDescriptor utf32_td{make_scalar_type(DataType::UTF_FIXED64)};
    TypeDescriptor utf8_td{make_scalar_type(DataType::UTF_DYNAMIC64)};
};

TEST_F(SegmentReslicerDenseStringStaticSchema, CombineIntoOne) {
    using RawType = StringPool::offset_t;
    // Combine columns of each of the 4 supported string types into a single column
    std::vector<ColumnWithStrings> input_columns;
    // "hello" appears in all columns
    // A string representation of the column type appears only in the column
    // Others appear in 2 or 3 of the columns
    input_columns.emplace_back(column_with_strings({"hello", "ascii fixed", "fixed"}, ascii_fixed_td.data_type()));
    input_columns.emplace_back(column_with_strings({"ascii dynamic", "dynamic", "hello"}, ascii_dynamic_td.data_type())
    );
    input_columns.emplace_back(column_with_strings({"fixed", "hello", "utf32"}, utf32_td.data_type()));
    input_columns.emplace_back(column_with_strings({"dynamic", "utf8", "hello"}, utf8_td.data_type()));
    uint64_t total_rows{12};
    SegmentReslicer reslicer{total_rows};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, total_rows};
    std::vector<StringPool> string_pools(1);
    auto res = reslicer.reslice_dense_string_columns(std::move(input_columns), slicing_info, string_pools);
    ASSERT_EQ(res.size(), 1);
    ASSERT_TRUE(res.front().has_value());
    auto& col = *res.front();
    ASSERT_EQ(col.type(), utf8_td);
    const auto& string_pool = string_pools.front();
    ASSERT_EQ(col.row_count(), total_rows);
    // This happens for the whole segment later in the real flow
    col.set_row_data(total_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    std::vector<std::string> expected_values{
            "hello",
            "ascii fixed",
            "fixed",
            "ascii dynamic",
            "dynamic",
            "hello",
            "fixed",
            "hello",
            "utf32",
            "dynamic",
            "utf8",
            "hello"
    };
    // Strings should be deduplicated in the pool regardless of source column type
    // The pool is populated in the order that strings are encountered. The offsets start at 0, and then are equal to
    // the previous offset + the length of the previous string in the pool + 4 for the string length
    std::unordered_map<std::string, RawType> expected_offsets{
            {"hello", 0},
            {"ascii fixed", 9},
            {"fixed", 24},
            {"ascii dynamic", 33},
            {"dynamic", 50},
            {"utf32", 61},
            {"utf8", 70}
    };
    for (size_t idx = 0; idx < total_rows; ++idx) {
        const auto& expected_string = expected_values.at(idx);
        auto opt_val = col.scalar_at<RawType>(idx);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, expected_offsets.at(expected_string));
        ASSERT_EQ(string_pool.get_const_view(*opt_val), expected_string);
    }
    ASSERT_EQ(col.buffer().bytes(), total_rows * sizeof(RawType));
    ASSERT_EQ(col.num_blocks(), 1);
    ASSERT_EQ(col.buffer().blocks().front()->capacity(), total_rows * sizeof(RawType));
}

class SegmentReslicerDenseStringStaticSchemaSplit : public SegmentReslicerDenseStringStaticSchema,
                                                    public ::testing::WithParamInterface<DataType> {};

TEST_P(SegmentReslicerDenseStringStaticSchemaSplit, SplitInTwoTest) {
    using RawType = StringPool::offset_t;
    std::vector<ColumnWithStrings> input_columns;
    const std::vector<std::string> input_data{"hello", "gutentag", "hello", "bonjour", "bonjour", "hello", "nihao"};
    input_columns.emplace_back(column_with_strings(input_data, GetParam()));
    size_t total_rows{input_data.size()};
    uint64_t max_rows_per_slice{4};
    uint64_t rows_in_first_slice{total_rows - max_rows_per_slice};
    SegmentReslicer reslicer{max_rows_per_slice};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, max_rows_per_slice};
    std::vector<StringPool> string_pools(2);
    auto res = reslicer.reslice_dense_string_columns(std::move(input_columns), slicing_info, string_pools);
    ASSERT_EQ(res.size(), 2);
    ASSERT_TRUE(res.front().has_value());
    ASSERT_TRUE(res.back().has_value());
    auto& col_0 = *res.front();
    auto& col_1 = *res.back();
    ASSERT_EQ(col_0.type(), utf8_td);
    ASSERT_EQ(col_1.type(), utf8_td);
    const auto& string_pool_0 = string_pools.front();
    const auto& string_pool_1 = string_pools.back();
    ASSERT_EQ(col_0.row_count(), rows_in_first_slice);
    ASSERT_EQ(col_1.row_count(), max_rows_per_slice);
    // This happens for the whole segment later in the real flow
    col_0.set_row_data(rows_in_first_slice - 1);
    col_1.set_row_data(max_rows_per_slice - 1);
    ASSERT_FALSE(col_0.is_sparse());
    ASSERT_FALSE(col_1.is_sparse());
    std::unordered_map<std::string, RawType> expected_offsets_0{{"hello", 0}, {"gutentag", 9}};
    std::unordered_map<std::string, RawType> expected_offsets_1{{"bonjour", 0}, {"hello", 11}, {"nihao", 20}};
    for (size_t idx = 0; idx < total_rows; ++idx) {
        const auto& expected_string = input_data.at(idx);
        const bool in_col_0 = idx < rows_in_first_slice;
        auto opt_val = in_col_0 ? col_0.scalar_at<RawType>(idx) : col_1.scalar_at<RawType>(idx - rows_in_first_slice);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, in_col_0 ? expected_offsets_0.at(expected_string) : expected_offsets_1.at(expected_string));
        ASSERT_EQ(
                in_col_0 ? string_pool_0.get_const_view(*opt_val) : string_pool_1.get_const_view(*opt_val),
                expected_string
        );
    }
    ASSERT_EQ(col_0.buffer().bytes(), rows_in_first_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().bytes(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_0.num_blocks(), 1);
    ASSERT_EQ(col_1.num_blocks(), 1);
    ASSERT_EQ(col_0.buffer().blocks().front()->capacity(), rows_in_first_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(RawType));
}

INSTANTIATE_TEST_SUITE_P(
        SplitInTwo, SegmentReslicerDenseStringStaticSchemaSplit,
        ::testing::Values(
                DataType::ASCII_FIXED64, DataType::ASCII_DYNAMIC64, DataType::UTF_FIXED64, DataType::UTF_DYNAMIC64
        )
);
