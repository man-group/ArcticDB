/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <limits>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/test/test_utils.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

TEST(Column, Empty) {
    using namespace arcticdb;

    Column c(TypeDescriptor(DataType::UINT16, Dimension(2)), 0, false, false);
    ASSERT_EQ(c.row_count(), 0);
}

template<typename TDT>
void test_column_type(size_t num_values = 20, size_t num_tests = 50) {
    using namespace arcticdb;
    using TypeDescriptorTag = TDT;
    using DTT = typename TypeDescriptorTag::DataTypeTag;
    using raw_type = typename DTT::raw_type;
    const Dimension dimensions = TDT::DimensionTag::value;

    TypeDescriptorTag typeDescriptorTag;
    Column column{TypeDescriptor(typeDescriptorTag), 0, false, false};
    ASSERT_EQ(TypeDescriptor(typeDescriptorTag), column.type());
    for (size_t i = 0; i < num_tests; ++i) {
        raw_type start = std::numeric_limits<raw_type>::min() + raw_type(i);
        TestValue<decltype(typeDescriptorTag)> testValue{start, num_values};

        if constexpr (dimensions == Dimension::Dim0) {
            column.set_scalar(i, testValue.get_scalar());
        } else {
            auto t = testValue.get_tensor();
            column.set_array(i, t);
        }

        ASSERT_EQ(column.row_count(), i + 1);
    }

    for (size_t j = 0; j < num_tests; ++j) {
        raw_type start = std::numeric_limits<raw_type>::min() + raw_type(j);
        position_t index = j;
        ASSERT_EQ(*column.ptr_cast<raw_type>(index, sizeof(raw_type)), start);

        if constexpr (dimensions == Dimension::Dim0) {
            ASSERT_EQ(column.search_unsorted(start).value(), index);
            auto s = column.scalar_at<raw_type>(j);
            ASSERT_FALSE(s == std::nullopt);
            ASSERT_EQ(s.value(), start);
        } else {
            TestValue<decltype(typeDescriptorTag)> testValue{start, num_values};
            auto v = column.tensor_at<raw_type>(j);
            ASSERT_FALSE(v == std::nullopt);
            auto t = v.value();
            ASSERT_TRUE(testValue.check_tensor(t));
        }
//TODO fix visitation with proper tensor
//        raw_type val = 0;
//        ASSERT_NO_THROW(column.visit(index, [&](auto &&x) { assign(*x.data(), val); }));
//        ASSERT_EQ(val, start);
    }
}

TEST(Column, ScalarTypes) {
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT32>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT8>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT16>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT32>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT32>, DimensionTag<Dimension::Dim0>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension::Dim0>>>();
}

TEST(Column, TensorTypes) {
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT32>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT8>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT16>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT32>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT32>, DimensionTag<Dimension ::Dim1>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension ::Dim1>>>();

    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT32>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT8>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT16>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT32>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT32>, DimensionTag<Dimension ::Dim2>>>();
    test_column_type<TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension ::Dim2>>>();
}

TEST(Column, IterateData) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(auto i= 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i);
    }

    std::vector<uint16_t> output;

    auto column_data = column.data();
    while (auto block = column_data.next<TDT>()) {
        for (const auto& item : *block)
            output.emplace_back(item);
    }

    ASSERT_EQ(output.size(), 10u);
    for(auto i= 0; i < 10; ++i) {
        ASSERT_EQ(output[i], i);
    }
}

TEST(Column, ChangeType) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(auto i= 0; i < 10; ++i) {
        column.set_scalar<int64_t>(i, i);
    }

    column.change_type(DataType::FLOAT64);
    auto expected = TypeDescriptor{DataType::FLOAT64, Dimension::Dim0};

    ASSERT_EQ(column.row_count(), 10u);
    ASSERT_EQ(column.type(), expected);
    for(auto i= 0; i < 10; ++i) {
        ASSERT_EQ(column.scalar_at<double>(i), i);
    }
}

std::unique_ptr<Column> get_sparse_column(size_t offset = 0, size_t start = 0, size_t num_rows = 10) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_unique<Column>(static_cast<TypeDescriptor>(TDT{}), 0, false, true);
    for(auto i = start; i < start + num_rows; i += 2) {
        column->set_scalar<int64_t>(i, i + offset);
    }
    return column;
}

std::unique_ptr<Column> get_dense_column(size_t offset = 0, size_t start = 0, size_t num_rows = 10) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_unique<Column>(static_cast<TypeDescriptor>(TDT{}), 0, false, true);
    for(auto i = start; i < start + num_rows; ++i) {
        column->set_scalar<int64_t>(i, i + offset);
    }
    return column;
}

TEST(Column, Dense) {
    auto column = get_dense_column();

    ASSERT_EQ(column->row_count(), 10u);
    for(auto i= 0; i < 10; ++i) {
        check_value(column->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, Sparse) {
    auto column = get_sparse_column();

    ASSERT_EQ(column->row_count(), 5u);
    for(auto i= 0; i < 10; i += 2) {
        check_value(column->scalar_at<int64_t>(i), i);
        check_value(column->scalar_at<int64_t>(i + 1), std::nullopt);
    }
}

TEST(Column, SparseChangeType) {
    auto column = get_sparse_column();

    column->change_type(DataType::FLOAT64);
    auto expected = TypeDescriptor{DataType::FLOAT64, Dimension::Dim0};

    ASSERT_EQ(column->row_count(), 5u);
    ASSERT_EQ(column->type(), expected);
    for(auto i= 0; i < 10; i += 2) {
        check_value(column->scalar_at<double>(i), i);
        check_value(column->scalar_at<double>(i + 1), std::nullopt);
    }
}

TEST(Column, AppendDenseToDense) {
    auto col1 = get_dense_column();
    auto col2 = get_dense_column(10);

    col1->append(*col2, col1->row_count());

    ASSERT_EQ(col1->row_count(),20u);
    for(auto i= 0; i < 20; ++i) {
        check_value(col1->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, AppendSparseToDense) {
    auto dense_column = get_dense_column();
    auto sparse_column = get_sparse_column(10);

    dense_column->append(*sparse_column, dense_column->row_count());

    ASSERT_EQ(dense_column->row_count(),15u);
    for(auto i= 0; i < 10; ++i) {
        check_value(dense_column->scalar_at<int64_t>(i), i);
    }

    for(auto j= 10; j < 20; j += 2) {
        check_value(dense_column->scalar_at<int64_t>(j), j);
        check_value(dense_column->scalar_at<int64_t>(j + 1), std::nullopt);
    }
}

TEST(Column, AppendDenseToSparse) {
    auto sparse_column = get_sparse_column();
    auto dense_column = get_dense_column(10);

    sparse_column->append(*dense_column, 10);

    ASSERT_EQ(sparse_column->row_count(),15u);
    for(auto i= 0; i < 10; i += 2) {
        check_value(sparse_column->scalar_at<uint64_t>(i), i);
        check_value(sparse_column->scalar_at<uint64_t>(i + 1), std::nullopt);
    }

    for(auto i= 10; i < 20; ++i) {
        check_value(sparse_column->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, AppendSparseToSparse) {
    auto col1 = get_sparse_column();
    auto col2 = get_sparse_column(10);

    col1->append(*col2, 10);

    ASSERT_EQ(col1->row_count(),10u);
    for(auto i= 0; i < 20; i += 2) {
        check_value(col1->scalar_at<uint64_t>(i), i);
        check_value(col1->scalar_at<uint64_t>(i + 1), std::nullopt);
    }
}

TEST(ColumnData, Iterator) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(auto i= 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i);
    }

    auto count = 0;
    for(auto val = column.begin<TDT>(); val != column.end<TDT>(); ++val) {
        ASSERT_EQ(*val, count++);
    }
}

TEST(ColumnData, LowerBound) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(auto i= 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i * 2);
    }

    auto it = std::lower_bound(column.begin<TDT>(), column.end<TDT>(), 5);
    ASSERT_EQ(*it, 6);
    ASSERT_EQ(std::distance(column.begin<TDT>(), it), 3);
}