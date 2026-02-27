/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/column_store/segment_reslicer.hpp>

using namespace arcticdb;

template<typename type>
class SegmentReslicerDenseNumericStaticSchemaFixture : public testing::Test {};

using test_types =
        ::testing::Types<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;
// using test_types = ::testing::Types<bool>;

TYPED_TEST_SUITE(SegmentReslicerDenseNumericStaticSchemaFixture, test_types);

TYPED_TEST(SegmentReslicerDenseNumericStaticSchemaFixture, CombineIntoOne) {
    using RawType = TypeParam;
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<RawType>());
    std::vector<std::optional<std::shared_ptr<Column>>> input_columns;
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
    std::vector<std::optional<std::shared_ptr<Column>>> input_columns;
    input_columns.emplace_back(std::make_shared<Column>(std::move(input_col)));
    uint64_t max_rows_per_slice{4};
    uint64_t rows_in_second_slice{total_rows - max_rows_per_slice};
    SegmentReslicer reslicer{max_rows_per_slice};
    SegmentReslicer::SlicingInfo slicing_info{total_rows, max_rows_per_slice};
    auto res = reslicer.reslice_dense_numeric_static_schema_columns(std::move(input_columns), slicing_info);
    ASSERT_EQ(res.size(), 2);
    ASSERT_TRUE(res.front().has_value());
    ASSERT_TRUE(res.back().has_value());
    auto& col_0 = *res.front();
    auto& col_1 = *res.back();
    ASSERT_EQ(col_0.row_count(), max_rows_per_slice);
    ASSERT_EQ(col_1.row_count(), rows_in_second_slice);
    // This happens for the whole segment later in the real flow
    col_0.set_row_data(max_rows_per_slice - 1);
    col_1.set_row_data(rows_in_second_slice - 1);
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
        auto opt_val = idx < max_rows_per_slice ? col_0.scalar_at<RawType>(idx)
                                                : col_1.scalar_at<RawType>(idx - max_rows_per_slice);
        ASSERT_TRUE(opt_val.has_value());
        ASSERT_EQ(*opt_val, expected_values.at(idx));
    }
    ASSERT_EQ(col_0.buffer().bytes(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().bytes(), rows_in_second_slice * sizeof(RawType));
    ASSERT_EQ(col_0.num_blocks(), 1);
    ASSERT_EQ(col_1.num_blocks(), 1);
    ASSERT_EQ(col_0.buffer().blocks().front()->capacity(), max_rows_per_slice * sizeof(RawType));
    ASSERT_EQ(col_1.buffer().blocks().front()->capacity(), rows_in_second_slice * sizeof(RawType));
}

TYPED_TEST(SegmentReslicerDenseNumericStaticSchemaFixture, CombineThreeIntoTwo) {
    using RawType = TypeParam;
    auto type_descriptor = make_scalar_type(data_type_from_raw_type<RawType>());
    uint64_t total_rows{30};

    std::vector<std::optional<std::shared_ptr<Column>>> input_columns;
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
    std::vector<std::optional<std::shared_ptr<Column>>> input_columns;
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    input_columns.emplace_back(std::make_shared<Column>(type_descriptor, Sparsity::NOT_PERMITTED));
    // 3968 bytes == 496 int64s per block, so 3 blocks per input column here
    for (size_t idx = 0; idx < input_data.size() / 3; ++idx) {
        input_columns.at(0).value()->push_back<int64_t>(input_data.at(idx));
        input_columns.at(1).value()->push_back<int64_t>(input_data.at(idx + total_rows / 3));
        input_columns.at(2).value()->push_back<int64_t>(input_data.at(idx + 2 * total_rows / 3));
    }
    ASSERT_TRUE(input_columns.at(0).value()->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(0).value()->num_blocks(), 3);
    ASSERT_TRUE(input_columns.at(1).value()->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(1).value()->num_blocks(), 3);
    ASSERT_TRUE(input_columns.at(2).value()->buffer().is_regular_sized());
    ASSERT_EQ(input_columns.at(2).value()->num_blocks(), 3);

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