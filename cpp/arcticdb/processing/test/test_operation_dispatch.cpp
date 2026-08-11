/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/processing/operation_dispatch_unary.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>
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

TEST(OperationDispatch, unary_operator_datetime) {
    using namespace arcticdb;
    auto datetime_column = ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(100)), "datetime_col");
    auto datetime_value = std::make_shared<Value>(construct_timestamp_value(50));

    EXPECT_THROW(visit_unary_operator(datetime_column, NegOperator{}), UserInputException);
    EXPECT_THROW(visit_unary_operator(datetime_column, AbsOperator{}), UserInputException);
    EXPECT_THROW(visit_unary_operator(datetime_value, NegOperator{}), UserInputException);
}

TEST(OperationDispatch, binary_operator_datetime) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    auto int_value = std::make_shared<Value>(static_cast<int64_t>(50), DataType::INT64);
    auto float_value = std::make_shared<Value>(static_cast<double>(1.5), DataType::FLOAT64);
    auto datetime_value = std::make_shared<Value>(construct_timestamp_value(50));

    // Multiplying or dividing a timestamp is meaningless whichever side the numeric operand is on
    EXPECT_THROW(visit_binary_operator(datetime_column, int_column, TimesOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(int_column, datetime_column, TimesOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, int_value, DivideOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_value, int_value, TimesOperator{}), UserInputException);

    // A float cannot be a nanosecond offset
    EXPECT_THROW(visit_binary_operator(datetime_column, float_value, PlusOperator{}), UserInputException);

    // Adding or subtracting an integer is a nanosecond offset, and yields a timestamp
    for (auto&& variant_data :
         {visit_binary_operator(datetime_column, int_column, PlusOperator{}),
          visit_binary_operator(datetime_column, int_value, PlusOperator{}),
          visit_binary_operator(datetime_column, int_value, MinusOperator{})}) {
        ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(variant_data));
        ASSERT_EQ(DataType::NANOSECONDS_UTC64, std::get<ColumnWithStrings>(variant_data).column_->type().data_type());
    }

    // Subtracting one timestamp from another gives a duration, which has no type of its own
    auto difference = visit_binary_operator(datetime_column, datetime_column, MinusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(difference));
    ASSERT_EQ(DataType::INT64, std::get<ColumnWithStrings>(difference).column_->type().data_type());

    // Adding, multiplying, dividing or raising one timestamp by another is meaningless; subtraction is the only
    // arithmetic that stays legal between two timestamps
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_column, PlusOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_column, TimesOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_column, DivideOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_column, PowOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_value, datetime_value, PlusOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_value, datetime_value, TimesOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_value, datetime_value, DivideOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_value, datetime_value, PowOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_value, PlusOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_value, TimesOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_value, DivideOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(datetime_column, datetime_value, PowOperator{}), UserInputException);

    // Two literal Values also do offset arithmetic when one is a timestamp and the other an integer
    auto value_plus_value = visit_binary_operator(datetime_value, int_value, PlusOperator{});
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Value>>(value_plus_value));
    auto plus_value = std::get<std::shared_ptr<Value>>(value_plus_value);
    ASSERT_EQ(DataType::NANOSECONDS_UTC64, plus_value->data_type());
    ASSERT_EQ(100, plus_value->get<timestamp>());

    auto value_minus_value = visit_binary_operator(datetime_value, int_value, MinusOperator{});
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Value>>(value_minus_value));
    auto minus_value = std::get<std::shared_ptr<Value>>(value_minus_value);
    ASSERT_EQ(DataType::NANOSECONDS_UTC64, minus_value->data_type());
    ASSERT_EQ(0, minus_value->get<timestamp>());

    // A timestamp Value minus an integer Column is still an offset even though the Value is the left operand
    auto value_minus_column = visit_binary_operator(datetime_value, int_column, MinusOperator{});
    ASSERT_TRUE(std::holds_alternative<ColumnWithStrings>(value_minus_column));
    auto minus_column = std::get<ColumnWithStrings>(value_minus_column).column_;
    ASSERT_EQ(DataType::NANOSECONDS_UTC64, minus_column->type().data_type());
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(50 - static_cast<timestamp>(idx), minus_column->scalar_at<timestamp>(idx));
    }

    // The reverse direction is not an offset: an integer minus a timestamp is not a timestamp, whether the
    // timestamp is a Value or a Column
    EXPECT_THROW(visit_binary_operator(int_value, datetime_column, MinusOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_operator(int_column, datetime_column, MinusOperator{}), UserInputException);
}

namespace {
using namespace arcticdb;

template<typename Func>
std::string read_time_arithmetic_error(const ColumnWithStrings& left, const ColumnWithStrings& right, Func&& func) {
    try {
        visit_binary_operator(left, right, std::forward<Func>(func));
    } catch (const UserInputException& e) {
        return e.what();
    }
    return {};
}

std::string schema_time_arithmetic_error(OperationType op) {
    ankerl::unordered_dense::map<std::string, DataType> column_types{
            {"dt1", DataType::NANOSECONDS_UTC64}, {"dt2", DataType::NANOSECONDS_UTC64}
    };
    try {
        node(col("dt1"), col("dt2"), op)->compute(column_types);
    } catch (const UserInputException& e) {
        return e.what();
    }
    return {};
}
} // namespace

TEST(OperationDispatch, binary_operator_two_timestamps_names_python_operator) {
    using namespace arcticdb;
    auto left = ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(100)), "dt1");
    auto right = ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(100)), "dt2");

    // The message names the operator as the user typed it in Python, and does so identically whether the error comes
    // from the schema-time type check in expression_node.cpp or the read-time dispatch in operation_dispatch_binary.hpp
    EXPECT_THAT(read_time_arithmetic_error(left, right, PlusOperator{}), testing::HasSubstr("using the + operator"));
    EXPECT_THAT(schema_time_arithmetic_error(OperationType::ADD), testing::HasSubstr("using the + operator"));
    EXPECT_THAT(read_time_arithmetic_error(left, right, TimesOperator{}), testing::HasSubstr("using the * operator"));
    EXPECT_THAT(schema_time_arithmetic_error(OperationType::MUL), testing::HasSubstr("using the * operator"));
    EXPECT_THAT(read_time_arithmetic_error(left, right, DivideOperator{}), testing::HasSubstr("using the / operator"));
    EXPECT_THAT(schema_time_arithmetic_error(OperationType::DIV), testing::HasSubstr("using the / operator"));
    EXPECT_THAT(read_time_arithmetic_error(left, right, PowOperator{}), testing::HasSubstr("using the ** operator"));
    EXPECT_THAT(schema_time_arithmetic_error(OperationType::POW), testing::HasSubstr("using the ** operator"));
}

TEST(OperationDispatch, binary_comparator_datetime) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    auto int_value = std::make_shared<Value>(static_cast<int64_t>(50), DataType::INT64);
    auto datetime_value = std::make_shared<Value>(construct_timestamp_value(50));

    EXPECT_THROW(visit_binary_comparator(datetime_column, int_column, LessThanOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_comparator(int_column, datetime_column, LessThanOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_comparator(datetime_column, int_value, LessThanOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_comparator(int_column, datetime_value, LessThanOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_comparator(datetime_value, int_column, LessThanOperator{}), UserInputException);

    // Timestamp against timestamp must keep working
    auto variant_data = visit_binary_comparator(datetime_column, datetime_value, LessThanOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(variant_data));
    auto results_bitset = std::get<util::BitSet>(variant_data);
    for (size_t idx = 0; idx < num_rows; idx++) {
        ASSERT_EQ(idx < 50, results_bitset.get_bit(idx));
    }
    // Column against itself is false for every row, which the dispatch layer collapses to EmptyResult. An all-true
    // result is deliberately not collapsed to FullResult, see transform_to_placeholder.
    ASSERT_TRUE(std::holds_alternative<EmptyResult>(
            visit_binary_comparator(datetime_column, datetime_column, LessThanOperator{})
    ));
    auto all_true = visit_binary_comparator(datetime_column, datetime_column, LessThanEqualsOperator{});
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(all_true));
    ASSERT_EQ(num_rows, std::get<util::BitSet>(all_true).count());
}

TEST(OperationDispatch, binary_membership_datetime) {
    using namespace arcticdb;
    size_t num_rows = 100;
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    std::unordered_set<int64_t> raw_set{0, 23, 82};
    auto int_set = std::make_shared<ValueSet>(std::make_shared<std::unordered_set<int64_t>>(raw_set));

    EXPECT_THROW(visit_binary_membership(datetime_column, int_set, IsInOperator{}), UserInputException);
    EXPECT_THROW(visit_binary_membership(datetime_column, int_set, IsNotInOperator{}), UserInputException);
    // The int column against the same set is fine, so it is the type mixing that is rejected
    ASSERT_TRUE(std::holds_alternative<util::BitSet>(visit_binary_membership(int_column, int_set, IsInOperator{})));
}

namespace {
using namespace arcticdb;

template<typename Func, DataType left_dt, DataType right_dt>
constexpr bool offset_arithmetic_for = time_arithmetic<Func, left_dt, right_dt> == TimeArithmeticKind::OFFSET;

template<typename Func, typename LeftRaw, typename RightRaw>
constexpr bool promotes_to_integral =
        std::is_integral_v<typename binary_operation_promoted_type<LeftRaw, RightRaw, Func>::type>;

constexpr auto TS = DataType::NANOSECONDS_UTC64;

// Adding or subtracting an integer offset is permitted at every integer width and both signednesses. Each permitted
// combination must promote to an integral type: the output is tagged NANOSECONDS_UTC64, so a floating point promotion
// would write doubles into an int64 column.
static_assert(offset_arithmetic_for<PlusOperator, TS, DataType::INT8>);
static_assert(offset_arithmetic_for<PlusOperator, TS, DataType::UINT8>);
static_assert(offset_arithmetic_for<PlusOperator, TS, DataType::INT64>);
static_assert(offset_arithmetic_for<PlusOperator, TS, DataType::UINT64>);
static_assert(promotes_to_integral<PlusOperator, timestamp, uint64_t>);
static_assert(promotes_to_integral<MinusOperator, timestamp, uint64_t>);
static_assert(promotes_to_integral<PlusOperator, timestamp, int8_t>);

// The integer may be on either side of an addition, since addition is commutative
static_assert(offset_arithmetic_for<PlusOperator, DataType::INT64, TS>);

// Subtraction needs the timestamp on the left: an integer minus a timestamp is not a timestamp
static_assert(offset_arithmetic_for<MinusOperator, TS, DataType::INT64>);
static_assert(!offset_arithmetic_for<MinusOperator, DataType::INT64, TS>);

// A float cannot be a nanosecond offset, and scaling a timestamp is meaningless whatever the other operand
static_assert(!offset_arithmetic_for<PlusOperator, TS, DataType::FLOAT64>);
static_assert(!offset_arithmetic_for<TimesOperator, TS, DataType::INT64>);
static_assert(!offset_arithmetic_for<DivideOperator, TS, DataType::INT64>);
static_assert(!offset_arithmetic_for<PowOperator, TS, DataType::INT64>);

// Subtraction is the only arithmetic that stays legal between two timestamps, and even then the result is a
// duration rather than an offset
static_assert(!offset_arithmetic_for<MinusOperator, TS, TS>);
static_assert(time_arithmetic<MinusOperator, TS, TS> == TimeArithmeticKind::DURATION);
static_assert(time_arithmetic<PlusOperator, TS, TS> == TimeArithmeticKind::INVALID);
static_assert(time_arithmetic<TimesOperator, TS, TS> == TimeArithmeticKind::INVALID);
static_assert(time_arithmetic<DivideOperator, TS, TS> == TimeArithmeticKind::INVALID);
static_assert(time_arithmetic<PowOperator, TS, TS> == TimeArithmeticKind::INVALID);

// Two plain numerics are unaffected
static_assert(!offset_arithmetic_for<PlusOperator, DataType::INT64, DataType::INT32>);
static_assert(time_arithmetic<PlusOperator, DataType::INT64, DataType::INT32> == TimeArithmeticKind::NONE);

} // namespace
