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

    // int col % val
    auto modulo_value = std::make_shared<Value>(static_cast<int64_t>(7), DataType::INT64);
    auto variant_data = visit_binary_operator(int_column, modulo_value, ModOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data));
    auto results_column = std::get<ColumnWithStrings>(variant_data).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(static_cast<int64_t>(idx) % 7, results_column->scalar_at<int64_t>(idx));
    }

    auto one = std::make_shared<Value>(static_cast<int64_t>(1), DataType::INT64);
    auto nonzero_column_variant = visit_binary_operator(int_column, one, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(nonzero_column_variant));
    auto nonzero_column = std::get<ColumnWithStrings>(nonzero_column_variant);

    // int col % int col (with non-zero divisor column)
    auto variant_data_col_col = visit_binary_operator(int_column, nonzero_column, ModOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data_col_col));
    auto results_column_col_col = std::get<ColumnWithStrings>(variant_data_col_col).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(static_cast<int64_t>(idx), results_column_col_col->scalar_at<int64_t>(idx));
    }

    // val % int col.
    auto variant_data_val_col = visit_binary_operator(modulo_value, nonzero_column, ModOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data_val_col));
    auto results_column_val_col = std::get<ColumnWithStrings>(variant_data_val_col).column_;
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(
                static_cast<int64_t>(7) % static_cast<int64_t>(idx + 1),
                results_column_val_col->scalar_at<int64_t>(idx)
        );
    }

    // Match Python/Pandas behavior for negative floating-point values.
    auto minus_three = std::make_shared<Value>(-3.0, DataType::FLOAT64);
    auto plus_two = std::make_shared<Value>(2.0, DataType::FLOAT64);
    auto variant_data_float = visit_binary_operator(minus_three, plus_two, ModOperator{});
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Value>>(variant_data_float));
    ASSERT_DOUBLE_EQ(std::get<std::shared_ptr<Value>>(variant_data_float)->get<double>(), 1.0);

    auto minus_two = std::make_shared<Value>(-2.0, DataType::FLOAT64);
    auto variant_data_float_neg_divisor = visit_binary_operator(minus_three, minus_two, ModOperator{});
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Value>>(variant_data_float_neg_divisor));
    ASSERT_DOUBLE_EQ(std::get<std::shared_ptr<Value>>(variant_data_float_neg_divisor)->get<double>(), -1.0);

    // Integral modulo by zero should fail with a user input error instead of UB.
    auto zero = std::make_shared<Value>(static_cast<int64_t>(0), DataType::INT64);
    EXPECT_THROW(visit_binary_operator(int_column, zero, ModOperator{}), UserInputException);
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
