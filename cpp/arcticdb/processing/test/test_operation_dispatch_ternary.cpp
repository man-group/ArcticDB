/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/util/test/generators.hpp>

namespace {

using namespace arcticdb;

constexpr size_t num_rows = 100;

util::BitSet alternating_condition() {
    util::BitSet condition(num_rows);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        condition.set(idx, idx % 2 == 0);
    }
    return condition;
}

DataType output_data_type(const VariantData& variant_data) {
    return std::get<ColumnWithStrings>(variant_data).column_->type().data_type();
}

} // namespace

TEST(OperationDispatchTernary, DatetimeNumericMismatchColumnColumn) {
    auto condition = alternating_condition();
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");

    EXPECT_THROW(ternary_operator(condition, datetime_column, int_column), UserInputException);
    EXPECT_THROW(ternary_operator(condition, int_column, datetime_column), UserInputException);
}

TEST(OperationDispatchTernary, DatetimeNumericMismatchColumnValue) {
    auto condition = alternating_condition();
    auto int_column = ColumnWithStrings(std::make_unique<Column>(generate_int_column(num_rows)), "int_col");
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    auto int_value = Value{static_cast<int64_t>(5), DataType::INT64};
    auto datetime_value = construct_timestamp_value(5);

    EXPECT_THROW(ternary_operator(condition, datetime_column, int_value), UserInputException);
    EXPECT_THROW((ternary_operator<true>(condition, datetime_column, int_value)), UserInputException);
    EXPECT_THROW(ternary_operator(condition, int_column, datetime_value), UserInputException);
    EXPECT_THROW((ternary_operator<true>(condition, int_column, datetime_value)), UserInputException);
}

TEST(OperationDispatchTernary, DatetimeNumericMismatchValueValue) {
    auto condition = alternating_condition();
    auto int_value = Value{static_cast<int64_t>(5), DataType::INT64};
    auto datetime_value = construct_timestamp_value(5);

    EXPECT_THROW(ternary_operator(condition, datetime_value, int_value), UserInputException);
    EXPECT_THROW(ternary_operator(condition, int_value, datetime_value), UserInputException);
}

// timestamp is int64_t, so without an explicit branch the output would be tagged INT64
TEST(OperationDispatchTernary, DatetimeOutputTypeIsTimestamp) {
    auto condition = alternating_condition();
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    auto other_datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "other_datetime_col");
    auto datetime_value = construct_timestamp_value(5);

    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64,
            output_data_type(ternary_operator(condition, datetime_column, other_datetime_column))
    );
    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64, output_data_type(ternary_operator(condition, datetime_column, datetime_value))
    );
    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64,
            output_data_type((ternary_operator<true>(condition, datetime_column, datetime_value)))
    );
    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64, output_data_type(ternary_operator(condition, datetime_value, datetime_value))
    );
    // Only one real operand, so there is nothing to mismatch against and the column type is reused
    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64, output_data_type(ternary_operator(condition, datetime_column, EmptyResult{}))
    );
    ASSERT_EQ(
            DataType::NANOSECONDS_UTC64, output_data_type(ternary_operator(condition, datetime_value, EmptyResult{}))
    );
}

TEST(OperationDispatchTernary, DatetimeColumnColumnSelectsCorrectValues) {
    auto condition = alternating_condition();
    auto datetime_column =
            ColumnWithStrings(std::make_unique<Column>(generate_datetime_column(num_rows)), "datetime_col");
    auto datetime_value = construct_timestamp_value(-1);

    auto variant_data = ternary_operator(condition, datetime_column, datetime_value);
    auto output = std::get<ColumnWithStrings>(variant_data).column_;
    for (size_t idx = 0; idx < num_rows; ++idx) {
        const auto expected = idx % 2 == 0 ? static_cast<timestamp>(idx) : timestamp{-1};
        ASSERT_EQ(expected, output->scalar_at<timestamp>(idx));
    }
}
