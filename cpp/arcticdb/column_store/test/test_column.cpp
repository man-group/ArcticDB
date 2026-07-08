/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <vector>

#include <arcticdb/util/test/test_utils.hpp>

#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

TEST(Column, Empty) {
    using namespace arcticdb;

    Column c(TypeDescriptor(DataType::UINT16, Dimension(2)), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
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
    Column column{TypeDescriptor(typeDescriptorTag), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED};
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
        // TODO fix visitation with proper tensor
        //         raw_type val = 0;
        //         ASSERT_NO_THROW(column.visit(index, [&](auto &&x) { assign(*x.data(), val); }));
        //         ASSERT_EQ(val, start);
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
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (auto i = 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i);
    }

    std::vector<uint16_t> output;

    auto column_data = column.data();
    while (auto block = column_data.next<TDT>()) {
        for (const auto& item : *block)
            output.emplace_back(item);
    }

    ASSERT_EQ(output.size(), 10u);
    for (auto i = 0; i < 10; ++i) {
        ASSERT_EQ(output[i], i);
    }
}

TEST(Column, ChangeType) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (auto i = 0; i < 10; ++i) {
        column.set_scalar<int64_t>(i, i);
    }

    column.change_type(DataType::FLOAT64);
    auto expected = TypeDescriptor{DataType::FLOAT64, Dimension::Dim0};

    ASSERT_EQ(column.row_count(), 10u);
    ASSERT_EQ(column.type(), expected);
    for (auto i = 0; i < 10; ++i) {
        ASSERT_EQ(column.scalar_at<double>(i), i);
    }
}

std::unique_ptr<Column> get_sparse_column(size_t offset = 0, size_t start = 0, size_t num_rows = 10) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_unique<Column>(
            static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    for (auto i = start; i < start + num_rows; i += 2) {
        column->set_scalar<int64_t>(i, i + offset);
    }
    return column;
}

std::unique_ptr<Column> get_dense_column(size_t offset = 0, size_t start = 0, size_t num_rows = 10) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_unique<Column>(
            static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    for (auto i = start; i < start + num_rows; ++i) {
        column->set_scalar<int64_t>(i, i + offset);
    }
    return column;
}

TEST(Column, Dense) {
    auto column = get_dense_column();

    ASSERT_EQ(column->row_count(), 10u);
    for (auto i = 0; i < 10; ++i) {
        check_value(column->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, Sparse) {
    auto column = get_sparse_column();

    ASSERT_EQ(column->row_count(), 5u);
    for (auto i = 0; i < 10; i += 2) {
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
    for (auto i = 0; i < 10; i += 2) {
        check_value(column->scalar_at<double>(i), i);
        check_value(column->scalar_at<double>(i + 1), std::nullopt);
    }
}

TEST(Column, AppendDenseToDense) {
    auto col1 = get_dense_column();
    auto col2 = get_dense_column(10);

    col1->append(*col2, col1->row_count());

    ASSERT_EQ(col1->row_count(), 20u);
    for (auto i = 0; i < 20; ++i) {
        check_value(col1->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, AppendSparseToDense) {
    auto dense_column = get_dense_column();
    auto sparse_column = get_sparse_column(10);

    dense_column->append(*sparse_column, dense_column->row_count());

    ASSERT_EQ(dense_column->row_count(), 15u);
    for (auto i = 0; i < 10; ++i) {
        check_value(dense_column->scalar_at<int64_t>(i), i);
    }

    for (auto j = 10; j < 20; j += 2) {
        check_value(dense_column->scalar_at<int64_t>(j), j);
        check_value(dense_column->scalar_at<int64_t>(j + 1), std::nullopt);
    }
}

TEST(Column, AppendDenseToSparse) {
    auto sparse_column = get_sparse_column();
    auto dense_column = get_dense_column(10);

    sparse_column->append(*dense_column, 10);

    ASSERT_EQ(sparse_column->row_count(), 15u);
    for (auto i = 0; i < 10; i += 2) {
        check_value(sparse_column->scalar_at<uint64_t>(i), i);
        check_value(sparse_column->scalar_at<uint64_t>(i + 1), std::nullopt);
    }

    for (auto i = 10; i < 20; ++i) {
        check_value(sparse_column->scalar_at<int64_t>(i), i);
    }
}

TEST(Column, AppendSparseToSparse) {
    auto col1 = get_sparse_column();
    auto col2 = get_sparse_column(10);

    col1->append(*col2, 10);

    ASSERT_EQ(col1->row_count(), 10u);
    for (auto i = 0; i < 20; i += 2) {
        check_value(col1->scalar_at<uint64_t>(i), i);
        check_value(col1->scalar_at<uint64_t>(i + 1), std::nullopt);
    }
}

TEST(ColumnData, Iterator) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (auto i = 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i);
    }

    auto count = 0;
    for (auto val = column.begin<TDT>(); val != column.end<TDT>(); ++val) {
        ASSERT_EQ(*val, count++);
    }
}

TEST(ColumnData, IteratorSkipsTrailingEmptyBlock) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

    Column col(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::array<int64_t, 3> data{10, 20, 30};
    col.set_dense_block(0, data.data(), data.size());
    col.set_dense_block(static_cast<ssize_t>(data.size()), static_cast<int64_t*>(nullptr), 0);
    ASSERT_EQ(col.buffer().num_blocks(), 2u);
    ASSERT_EQ(col.buffer().blocks()[0]->logical_size(), data.size() * sizeof(int64_t));
    ASSERT_EQ(col.buffer().blocks()[1]->logical_size(), 0u);

    auto column_data = col.data();
    std::vector<int64_t> visited;
    for (auto it = column_data.cbegin<TDT, IteratorType::REGULAR, IteratorDensity::SPARSE>(),
              end = column_data.cend<TDT, IteratorType::REGULAR, IteratorDensity::SPARSE>();
         it != end;
         ++it) {
        visited.push_back(*it);
    }
    EXPECT_EQ(visited, (std::vector<int64_t>{10, 20, 30}));
}

TEST(ColumnData, IteratorOnAllEmptyColumn) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

    // A single zero-size external block. begin must compare equal to end.
    Column empty_col(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    empty_col.set_dense_block(0, static_cast<int64_t*>(nullptr), 0);
    ASSERT_EQ(empty_col.buffer().num_blocks(), 1u);
    ASSERT_EQ(empty_col.buffer().blocks()[0]->logical_size(), 0u);
    auto empty_data = empty_col.data();
    auto begin = empty_data.cbegin<TDT, IteratorType::REGULAR, IteratorDensity::SPARSE>();
    auto end = empty_data.cend<TDT, IteratorType::REGULAR, IteratorDensity::SPARSE>();
    EXPECT_EQ(begin, end);
}

// We should be able to differentiate iterators to same shared memory in different blocks.
TEST(ColumnData, IteratorEqualityAcrossSharedExternalMemory) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;
    Column col(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);

    std::array<int64_t, 3> shared{100, 200, 300};
    col.set_dense_block(0, shared.data(), shared.size());
    col.set_dense_block(static_cast<ssize_t>(shared.size()), shared.data(), shared.size());

    auto column_data = col.data();
    auto it_in_block0 = column_data.citerator_at<TDT>(1);                 // (block 0, offset 1)
    auto it_in_block1 = column_data.citerator_at<TDT>(shared.size() + 1); // (block 1, offset 1)

    EXPECT_NE(it_in_block0, it_in_block1);
    // Dereferences agree because both iterators see the same underlying memory.
    EXPECT_EQ(*it_in_block0, *it_in_block1);

    // And a full iteration
    std::vector<int64_t> visited;
    for (auto it = column_data.cbegin<TDT, IteratorType::REGULAR, IteratorDensity::DENSE>(),
              end = column_data.cend<TDT, IteratorType::REGULAR, IteratorDensity::DENSE>();
         it != end;
         ++it) {
        visited.push_back(*it);
    }
    EXPECT_EQ(visited, (std::vector<int64_t>{100, 200, 300, 100, 200, 300}));
}

TEST(ColumnData, LowerBound) {
    using namespace arcticdb;

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (auto i = 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i * 2);
    }

    auto it = std::lower_bound(column.begin<TDT>(), column.end<TDT>(), 5);
    ASSERT_EQ(*it, 6);
    ASSERT_EQ(std::distance(column.begin<TDT>(), it), 3);
}

FieldStatsImpl generate_stats_from_column(const Column& column) {
    return details::visit_scalar(column.type(), [&column](auto tdt) {
        using TagType = std::decay_t<decltype(tdt)>;
        return generate_column_statistics<TagType>(column.data());
    });
}

TEST(ColumnStats, IntegerColumn) {
    Column int_col(make_scalar_type(DataType::INT64));
    int_col.set_scalar<int64_t>(0, 10);
    int_col.set_scalar<int64_t>(1, 5);
    int_col.set_scalar<int64_t>(2, 20);
    int_col.set_scalar<int64_t>(3, 5);
    int_col.set_scalar<int64_t>(4, 15);

    FieldStatsImpl stats = generate_stats_from_column(int_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_EQ(stats.get_min<int64_t>(), 5);
    EXPECT_EQ(stats.get_max<int64_t>(), 20);
    EXPECT_EQ(stats.unique_count_, 4);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(ColumnStats, FloatColumn) {
    Column float_col(make_scalar_type(DataType::FLOAT32));
    float_col.set_scalar<float>(0, 10.5f);
    float_col.set_scalar<float>(1, 5.5f);
    float_col.set_scalar<float>(2, 20.5f);
    float_col.set_scalar<float>(3, 5.5f);
    float_col.set_scalar<float>(4, 15.5f);

    FieldStatsImpl stats = generate_stats_from_column(float_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_FLOAT_EQ(stats.get_min<float>(), 5.5f);
    EXPECT_FLOAT_EQ(stats.get_max<float>(), 20.5f);
    EXPECT_EQ(stats.unique_count_, 4);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(ColumnStats, EmptyColumn) {
    Column empty_col(make_scalar_type(DataType::FLOAT32));
    FieldStatsImpl stats = generate_stats_from_column(empty_col);

    EXPECT_FALSE(stats.has_min());
    EXPECT_FALSE(stats.has_max());
    EXPECT_FALSE(stats.has_unique());
    EXPECT_EQ(stats.unique_count_, 0);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(ColumnStats, SingleValueColumn) {
    Column single_col(make_scalar_type(DataType::INT32));
    single_col.set_scalar<int32_t>(0, 42);

    FieldStatsImpl stats = generate_stats_from_column(single_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_EQ(stats.get_min<int32_t>(), 42);
    EXPECT_EQ(stats.get_max<int32_t>(), 42);
    EXPECT_EQ(stats.unique_count_, 1);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(ColumnStats, NegativeNumbers) {
    Column neg_col(make_scalar_type(DataType::INT64));
    neg_col.set_scalar<int64_t>(0, -10);
    neg_col.set_scalar<int64_t>(1, -5);
    neg_col.set_scalar<int64_t>(2, -20);
    neg_col.set_scalar<int64_t>(3, -15);

    FieldStatsImpl stats = generate_stats_from_column(neg_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_EQ(stats.get_min<int64_t>(), -20);
    EXPECT_EQ(stats.get_max<int64_t>(), -5);
    EXPECT_EQ(stats.unique_count_, 4);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(ColumnStats, DoubleColumn) {
    Column double_col(make_scalar_type(DataType::FLOAT64));
    double_col.set_scalar<double>(0, 10.5);
    double_col.set_scalar<double>(1, 5.5);
    double_col.set_scalar<double>(2, 20.5);
    double_col.set_scalar<double>(3, 5.5);
    double_col.set_scalar<double>(4, 15.5);

    FieldStatsImpl stats = generate_stats_from_column(double_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_DOUBLE_EQ(stats.get_min<double>(), 5.5);
    EXPECT_DOUBLE_EQ(stats.get_max<double>(), 20.5);
    EXPECT_EQ(stats.unique_count_, 4);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}

// ─── Sorted-column search tests ──────────────────────────────────────────────────────────────────

namespace {
using namespace arcticdb;
using SearchTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

Column make_single_block(const std::vector<int64_t>& values) {
    return make_single_block_column<int64_t>(values, DataType::INT64);
}

Column make_regular_blocks(const std::vector<int64_t>& values) {
    return make_regular_blocks_column<int64_t>(values, DataType::INT64);
}

Column make_irregular_blocks(const std::vector<int64_t>& values, const std::vector<size_t>& block_sizes) {
    return make_irregular_blocks_column<int64_t>(values, block_sizes, DataType::INT64);
}

Column make_irregular_blocks(const std::vector<int64_t>& values) {
    return make_irregular_blocks_column<int64_t>(values, DataType::INT64);
}

// Cross-checks our search functions against std::lower_bound / upper_bound on the reference vector.
// from/to (when set) restrict the column-side search via citerator_at; otherwise cbegin/cend are used.
void check_search_on_column(
        const std::vector<int64_t>& values, const Column& col, const std::vector<int64_t>& probes,
        std::string_view label, std::optional<size_t> from = std::nullopt, std::optional<size_t> to = std::nullopt
) {
    auto column_data = col.data();
    const ssize_t total = static_cast<ssize_t>(values.size());
    auto begin = from.has_value() ? column_data.citerator_at<SearchTDT, IteratorType::ENUMERATED>(*from)
                                  : column_data.cbegin<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto end = to.has_value() ? column_data.citerator_at<SearchTDT, IteratorType::ENUMERATED>(*to)
                              : column_data.cend<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto res_idx = [&](auto& it) { return it.current_block_data() != nullptr ? it->idx() : total; };
    const auto std_begin = values.begin() + from.value_or(0);
    const auto std_end = values.begin() + to.value_or(values.size());
    for (auto v : probes) {
        auto std_lb = std::lower_bound(std_begin, std_end, v);
        auto std_ub = std::upper_bound(std_begin, std_end, v);
        auto lb_it = lower_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, v);
        auto ub_it = upper_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, v);
        auto ex_lb_it =
                exponential_lower_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, v);
        auto ex_ub_it =
                exponential_upper_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, v);
        ASSERT_EQ(res_idx(lb_it), std_lb - values.begin()) << label << " lower_bound(" << v << ")";
        ASSERT_EQ(res_idx(ub_it), std_ub - values.begin()) << label << " upper_bound(" << v << ")";
        ASSERT_EQ(res_idx(ex_lb_it), std_lb - values.begin()) << label << " exponential_lower_bound(" << v << ")";
        ASSERT_EQ(res_idx(ex_ub_it), std_ub - values.begin()) << label << " exponential_upper_bound(" << v << ")";
    }
}

void check_search(
        const std::vector<int64_t>& values, const std::vector<int64_t>& probes,
        std::optional<size_t> from = std::nullopt, std::optional<size_t> to = std::nullopt
) {
    check_search_on_column(values, make_single_block(values), probes, "single", from, to);
    check_search_on_column(values, make_regular_blocks(values), probes, "regular", from, to);
    check_search_on_column(values, make_irregular_blocks(values), probes, "irregular", from, to);
    check_search_on_column(
            values,
            make_irregular_blocks(values, std::vector<size_t>(values.size(), 1)),
            probes,
            "all-1-blocks",
            from,
            to
    );
}

} // namespace

TEST(ColumnSearch, BasicRegular) {
    std::vector<int64_t> values{0, 5, 10, 15, 20, 20, 25, 25, 30, 35};
    Column col = make_regular_blocks(values);
    auto column_data = col.data();
    auto begin = column_data.cbegin<SearchTDT, IteratorType::REGULAR, IteratorDensity::DENSE>();
    auto end = column_data.cend<SearchTDT, IteratorType::REGULAR, IteratorDensity::DENSE>();
    // 20 is duplicated at indices 4 and 5; lower_bound must land on the first (4) and upper_bound past
    // the last (6).
    auto lb = lower_bound<SearchTDT, IteratorType::REGULAR, IteratorDensity::DENSE>(begin, end, int64_t{20});
    ASSERT_EQ(*lb, 20);
    ASSERT_EQ(std::distance(begin, lb), 4);
    auto ub = upper_bound<SearchTDT, IteratorType::REGULAR, IteratorDensity::DENSE>(begin, end, int64_t{20});
    ASSERT_EQ(*ub, 25);
    ASSERT_EQ(std::distance(begin, ub), 6);
}

TEST(ColumnSearch, BasicEnumerated) {
    std::vector<int64_t> values{0, 5, 10, 15, 20, 20, 25, 25, 30, 35};
    Column col = make_regular_blocks(values);
    auto column_data = col.data();
    auto begin = column_data.cbegin<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto end = column_data.cend<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>();
    auto lb = lower_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, int64_t{20});
    ASSERT_EQ(lb->idx(), 4);
    ASSERT_EQ(lb->value(), 20);
    auto ub = upper_bound<SearchTDT, IteratorType::ENUMERATED, IteratorDensity::DENSE>(begin, end, int64_t{20});
    ASSERT_EQ(ub->idx(), 6);
    ASSERT_EQ(ub->value(), 25);
}

TEST(ColumnSearch, BasicCoverage) {
    // Small column; mix of unique and duplicated values to make lower_bound / upper_bound diverge.
    std::vector<int64_t> values{0, 1, 2, 2, 2, 5, 7, 7, 9, 10};
    std::vector<int64_t> probes{-1, 0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11};
    check_search(values, probes);
}

TEST(ColumnSearch, LargeArray) {
    // Large array test ensures jumping across regular sized blocks works as well
    std::vector<int64_t> values(10000);
    std::iota(values.begin(), values.end(), 0);
    std::vector<int64_t> probes{-1, 0, 1, 511, 512, 1023, 1024, 2500, 5000, 7500, 8191, 8192, 9999, 10000};
    check_search(values, probes);
}

TEST(ColumnSearch, SingleElement) {
    std::vector<int64_t> values{42};
    std::vector<int64_t> probes{41, 42, 43};
    check_search(values, probes);
}

TEST(ColumnSearch, AllEqual) {
    std::vector<int64_t> values(64, 5);
    std::vector<int64_t> probes{4, 5, 6};
    check_search(values, probes);
}

TEST(ColumnSearch, SubRange) {
    std::vector<int64_t> values(500);
    std::iota(values.begin(), values.end(), 0);
    std::vector<int64_t> probes{-1, 0, 99, 100, 200, 399, 400, 500};
    check_search(values, probes, 100, 400);
}

TEST(ColumnSearch, EmptySubRange) {
    std::vector<int64_t> values{1, 2, 3, 4, 5};
    std::vector<int64_t> probes{0, 3, 6};
    check_search(values, probes, 2, 2);
}

TEST(ColumnSearch, SubRangeAtRunOfEquals) {
    std::vector<int64_t> values{0, 1, 1, 1, 1, 2, 3, 3, 3, 3, 4};
    std::vector<int64_t> probes{0, 1, 2, 3, 4};
    check_search(values, probes, 2, 8);
    check_search(values, probes, 3, 7);
}

// Covers boundary condition where galloping to the position 0 + 2**2 = 4 is the precisely the last element in the block
TEST(ColumnSearch, GallopProbeAtFirstBlockEnd) {
    std::vector<int64_t> values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    std::vector<int64_t> probes{0, 4, 5, 6, 10, 16};
    check_search_on_column(values, make_irregular_blocks(values, std::vector<size_t>{5, 5, 5}), probes, "fixed-5");
    check_search_on_column(values, make_irregular_blocks(values, std::vector<size_t>{5, 5, 5}), probes, "fixed-5", 2);
}

TEST(ColumnStats, MultipleBlocks) {
    Column single_col(make_scalar_type(DataType::UINT64));

    for (auto i = 0UL; i < 1'000'000UL; ++i)
        single_col.set_scalar<int64_t>(i, i);

    FieldStatsImpl stats = generate_stats_from_column(single_col);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());

    EXPECT_EQ(single_col.buffer().num_blocks(), 2017);

    EXPECT_EQ(stats.get_min<uint64_t>(), 0);
    EXPECT_EQ(stats.get_max<int32_t>(), 999'999);
    EXPECT_EQ(stats.unique_count_, 1'000'000);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
}
