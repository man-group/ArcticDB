/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_data_random_accessor.hpp>

using namespace arcticdb;

class ColumnDataRandomAccessorTest : public testing::Test {
  protected:
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    void SetUp() override {
        input_data.resize(n);
        std::iota(input_data.begin(), input_data.end(), 42);
    }
    // 3968 bytes == 496 int64s per block, so 3 blocks here
    size_t n{1000};
    std::vector<int64_t> input_data;
    TypeDescriptor type_descriptor{static_cast<TypeDescriptor>(TDT{})};
};

// Note that dense and sparse single block accessors are tested in rapidcheck_column_data_random_accessor.cpp
TEST_F(ColumnDataRandomAccessorTest, DenseRegularBlocks) {
    Column column(type_descriptor, Sparsity::NOT_PERMITTED);
    for (auto& val : input_data) {
        column.push_back<int64_t>(val);
    }

    ASSERT_TRUE(column.buffer().is_regular_sized());
    ASSERT_EQ(column.num_blocks(), 3);

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    for (size_t idx = 0; idx < n; ++idx) {
        ASSERT_EQ(accessor.at(idx), input_data[idx]);
    }
}

TEST_F(ColumnDataRandomAccessorTest, SparseRegularBlocks) {
    Column column(type_descriptor, Sparsity::PERMITTED);
    // Set every third value
    for (size_t idx = 0; idx < n; ++idx) {
        column.set_scalar<int64_t>(3 * idx, input_data[idx]);
    }

    ASSERT_TRUE(column.is_sparse());
    ASSERT_TRUE(column.buffer().is_regular_sized());
    ASSERT_EQ(column.num_blocks(), 3);

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    for (size_t idx = 0; idx < n; ++idx) {
        ASSERT_EQ(accessor.at(3 * idx), input_data[idx]);
    }
}

TEST_F(ColumnDataRandomAccessorTest, DenseIrregularBlocks) {
    // Presize to 500 values forces irregular blocks
    Column column(type_descriptor, n / 2, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    ASSERT_EQ(column.num_blocks(), 1);
    memcpy(column.ptr(), input_data.data(), (n / 2) * sizeof(int64_t));
    column.set_row_data((n / 2) - 1);
    for (size_t idx = n / 2; idx < n; ++idx) {
        column.push_back<int64_t>(input_data[idx]);
    }

    ASSERT_FALSE(column.buffer().is_regular_sized());
    ASSERT_EQ(column.num_blocks(), 3);

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    for (size_t idx = 0; idx < n; ++idx) {
        ASSERT_EQ(accessor.at(idx), input_data[idx]);
    }
}

TEST_F(ColumnDataRandomAccessorTest, SparseIrregularBlocks) {
    // Presize to 500 values forces irregular blocks
    Column column(type_descriptor, n / 2, AllocationType::PRESIZED, Sparsity::PERMITTED);
    ASSERT_EQ(column.num_blocks(), 1);
    memcpy(column.ptr(), input_data.data(), (n / 2) * sizeof(int64_t));
    column.set_row_data((n / 2) - 1);
    // Set every third value
    for (size_t idx = n / 2; idx < n; ++idx) {
        column.set_scalar<int64_t>(3 * idx, input_data[idx]);
    }

    ASSERT_TRUE(column.is_sparse());
    ASSERT_FALSE(column.buffer().is_regular_sized());
    ASSERT_EQ(column.num_blocks(), 3);

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    for (size_t idx = 0; idx < n; ++idx) {
        if (idx < n / 2) {
            ASSERT_EQ(accessor.at(idx), input_data[idx]);
        } else {
            ASSERT_EQ(accessor.at(3 * idx), input_data[idx]);
        }
    }
}