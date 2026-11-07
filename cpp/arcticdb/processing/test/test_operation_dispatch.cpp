/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/operation_dispatch_unary.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/test/generators.hpp>

TEST(OperationDispatch, unary_operator) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto empty_column = ColumnWithStrings(std::make_unique<Column>(generate_empty_column()), "empty_col");

    // int col
    auto variant_data = visit_unary_operator(int_column, NegOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data));
    auto results_column = std::get<ColumnWithStrings>(variant_data).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(-idx, results_column->scalar_at<int64_t>(idx));
    }
    // empty col
    EXPECT_THROW(visit_unary_operator(empty_column, NegOperator{}), SchemaException);
}

TEST(OperationDispatch, binary_operator) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto empty_column = ColumnWithStrings(std::make_unique<Column>(generate_empty_column()), "empty_col");
    auto value = std::make_shared<Value>(static_cast<int64_t>(50), DataType::INT64);

    // int col + int col
    auto variant_data_0 = visit_binary_operator(int_column, int_column, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data_0));
    auto results_column_0 = std::get<ColumnWithStrings>(variant_data_0).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(idx + idx, results_column_0->scalar_at<int64_t>(idx));
    }
    // int col + val
    auto variant_data_1 = visit_binary_operator(int_column, value, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data_1));
    auto results_column_1 = std::get<ColumnWithStrings>(variant_data_1).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(idx + 50, results_column_1->scalar_at<int64_t>(idx));
    }
    // val + int col
    auto variant_data_2 = visit_binary_operator(value, int_column, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data_2));
    auto results_column_2 = std::get<ColumnWithStrings>(variant_data_2).column_;
    ASSERT_TRUE(*results_column_1 == *results_column_2);
    // val + val
    auto variant_data_3 = visit_binary_operator(value, value, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Value>>(variant_data_3));
    auto results_value = std::get<std::shared_ptr<Value>>(variant_data_3)->get<int64_t>();
    ASSERT_EQ(results_value, 100);
    // int col + empty col
    EXPECT_THROW(visit_binary_operator(int_column, empty_column, PlusOperator{}), SchemaException);
    // empty col + int col
    EXPECT_THROW(visit_binary_operator(empty_column, int_column, PlusOperator{}), SchemaException);
    // empty col + empty col
    EXPECT_THROW(visit_binary_operator(empty_column, empty_column, PlusOperator{}), SchemaException);
    // empty col + val
    EXPECT_THROW(visit_binary_operator(empty_column, value, PlusOperator{}), SchemaException);
    // val + empty col
    EXPECT_THROW(visit_binary_operator(value, empty_column, PlusOperator{}), SchemaException);
}

TEST(OperationDispatch, binary_comparator) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto empty_column = ColumnWithStrings(std::make_unique<Column>(generate_empty_column()), "empty_col");
    auto value = std::make_shared<Value>(static_cast<int64_t>(50), DataType::INT64);

    // int col < int col
    ASSERT_TRUE(std::holds_alternative<EmptyResult>(visit_binary_comparator(int_column, int_column, LessThanOperator{}))
    );
    // int col < val
    auto variant_data_0 = visit_binary_comparator(int_column, value, LessThanOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(variant_data_0));
    auto results_bitset_0 = std::get<util::BitSet>(variant_data_0);
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(idx < 50, results_bitset_0.get_bit(idx));
    }
    // val < int col
    auto variant_data_1 = visit_binary_comparator(value, int_column, LessThanOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(variant_data_1));
    auto results_bitset_1 = std::get<util::BitSet>(variant_data_1);
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(50 < idx, results_bitset_1.get_bit(idx));
    }
    // val < val not supported, should be handled at expression evaluation time
    // int col < empty col
    ASSERT_TRUE(
            std::holds_alternative<EmptyResult>(visit_binary_comparator(int_column, empty_column, LessThanOperator{}))
    );
    // empty col < int col
    ASSERT_TRUE(
            std::holds_alternative<EmptyResult>(visit_binary_comparator(empty_column, int_column, LessThanOperator{}))
    );
    // empty col < empty col
    ASSERT_TRUE(
            std::holds_alternative<EmptyResult>(visit_binary_comparator(empty_column, empty_column, LessThanOperator{}))
    );
    // empty col < val
    ASSERT_TRUE(std::holds_alternative<EmptyResult>(visit_binary_comparator(empty_column, value, LessThanOperator{})));
    // val < empty col
    ASSERT_TRUE(std::holds_alternative<EmptyResult>(visit_binary_comparator(value, empty_column, LessThanOperator{})));
}

TEST(OperationDispatch, binary_membership) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto empty_column = ColumnWithStrings(std::make_unique<Column>(generate_empty_column()), "empty_col");
    std::unordered_set<int64_t> raw_set{0, 23, 82, static_cast<int64_t>(num_rows) - 1, 1000000};
    auto value_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<int64_t>>(raw_set));

    // int col isin set
    auto variant_data_0 = visit_binary_membership(int_column, value_set, IsInOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(variant_data_0));
    auto results_bitset_0 = std::get<util::BitSet>(variant_data_0);
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(raw_set.count(static_cast<int64_t>(idx)) > 0, results_bitset_0.get_bit(idx));
    }
    // int col isnotin set
    auto variant_data_1 = visit_binary_membership(int_column, value_set, IsNotInOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(variant_data_0));
    auto results_bitset_1 = std::get<util::BitSet>(variant_data_1);
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(raw_set.count(static_cast<int64_t>(idx)) == 0, results_bitset_1.get_bit(idx));
    }
    // empty col isin set
    ASSERT_TRUE(std::holds_alternative<EmptyResult>(visit_binary_membership(empty_column, value_set, IsInOperator{})));
    // empty col isnotin set
    ASSERT_TRUE(std::holds_alternative<FullResult>(visit_binary_membership(empty_column, value_set, IsNotInOperator{}))
    );
}
namespace {
template<typename T>
arcticdb::ColumnWithStrings float_column(std::initializer_list<T> values) {
    using namespace arcticdb;
    auto column = std::make_unique<Column>(
            make_scalar_type(data_type_from_raw_type<T>()), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED
    );
    ssize_t row = 0;
    for (auto value : values) {
        column->template set_scalar<T>(row++, value);
    }
    return {std::move(column), "float_col"};
}

std::vector<bool> to_bools(const arcticdb::VariantData& data, size_t num_rows) {
    using namespace arcticdb;
    std::vector<bool> res(num_rows, false);
    if (std::holds_alternative<FullResult>(data)) {
        res.assign(num_rows, true);
    } else if (std::holds_alternative<util::BitSet>(data)) {
        const auto& bitset = std::get<util::BitSet>(data);
        for (size_t idx = 0; idx < num_rows; ++idx) {
            res[idx] = bitset.get_bit(idx);
        }
    }
    return res;
}
} // namespace

TEST(OperationDispatch, python_float_literal_compared_at_float64_precision_against_float64_column) {
    using namespace arcticdb;
    auto column = float_column<double>({0.05, 0.07, 0.04, 0.05});
    auto literal = std::make_shared<Value>(construct_python_float_value(0.05));
    ASSERT_EQ(
            to_bools(visit_binary_comparator(column, literal, EqualsOperator{}), 4),
            (std::vector{true, false, false, true})
    );
    ASSERT_EQ(
            to_bools(visit_binary_comparator(literal, column, LessThanEqualsOperator{}), 4),
            (std::vector{true, true, false, true})
    );
}

TEST(OperationDispatch, python_float_literal_compared_at_float32_precision_against_float32_column) {
    using namespace arcticdb;
    auto column = float_column<float>({0.05f, 0.07f, 0.04f, 0.05f});
    auto literal = std::make_shared<Value>(construct_python_float_value(0.05));
    ASSERT_EQ(
            to_bools(visit_binary_comparator(column, literal, EqualsOperator{}), 4),
            (std::vector{true, false, false, true})
    );
    ASSERT_EQ(
            to_bools(visit_binary_comparator(column, literal, LessThanEqualsOperator{}), 4),
            (std::vector{true, false, true, true})
    );
}

TEST(OperationDispatch, python_float_literal_beyond_float32_range_narrows_to_infinity) {
    using namespace arcticdb;
    auto column = float_column<float>({std::numeric_limits<float>::max(), std::numeric_limits<float>::infinity()});
    auto literal = std::make_shared<Value>(construct_python_float_value(1e300));
    ASSERT_EQ(to_bools(visit_binary_comparator(column, literal, LessThanOperator{}), 2), (std::vector{true, false}));
    ASSERT_EQ(to_bools(visit_binary_comparator(column, literal, EqualsOperator{}), 2), (std::vector{false, true}));
}

TEST(OperationDispatch, numpy_float64_literal_widens_float32_column) {
    using namespace arcticdb;
    auto column = float_column<float>({0.05f, 0.07f});
    auto literal = std::make_shared<Value>(construct_value<double>(0.05));
    ASSERT_EQ(to_bools(visit_binary_comparator(column, literal, EqualsOperator{}), 2), (std::vector{false, false}));
}

TEST(OperationDispatch, python_float_literal_arithmetic_takes_column_float_precision) {
    using namespace arcticdb;
    auto literal = std::make_shared<Value>(construct_python_float_value(0.05));

    auto float64_result = visit_binary_operator(float_column<double>({2.0}), literal, TimesOperator{});
    const auto& float64_column = *std::get<ColumnWithStrings>(float64_result).column_;
    ASSERT_EQ(float64_column.type().data_type(), DataType::FLOAT64);
    ASSERT_EQ(float64_column.scalar_at<double>(0), 2.0 * 0.05);

    auto float32_result = visit_binary_operator(literal, float_column<float>({2.0f}), TimesOperator{});
    const auto& float32_column = *std::get<ColumnWithStrings>(float32_result).column_;
    ASSERT_EQ(float32_column.type().data_type(), DataType::FLOAT32);
    ASSERT_EQ(float32_column.scalar_at<float>(0), 0.05f * 2.0f);
}

TEST(OperationDispatch, python_float_literal_ternary_takes_column_float_precision) {
    using namespace arcticdb;
    util::BitSet condition(2);
    condition.set_bit(0);
    auto literal = std::make_shared<Value>(construct_python_float_value(0.05));

    auto float64_result =
            dispatch_ternary(condition, float_column<double>({1.0, 2.0}), literal, OperationType::TERNARY);
    const auto& float64_column = *std::get<ColumnWithStrings>(float64_result).column_;
    ASSERT_EQ(float64_column.type().data_type(), DataType::FLOAT64);
    ASSERT_EQ(float64_column.scalar_at<double>(1), 0.05);

    auto float32_result =
            dispatch_ternary(condition, literal, float_column<float>({1.0f, 2.0f}), OperationType::TERNARY);
    const auto& float32_column = *std::get<ColumnWithStrings>(float32_result).column_;
    ASSERT_EQ(float32_column.type().data_type(), DataType::FLOAT32);
    ASSERT_EQ(float32_column.scalar_at<float>(0), 0.05f);
}
