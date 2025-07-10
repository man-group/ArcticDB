#include <gtest/gtest.h>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/util/test/test_utils.hpp>

using namespace arcticdb;

class UnsortedAggregationDataTypeParametrizationFixture :public ::testing::TestWithParam<DataType> {};

TEST_P(UnsortedAggregationDataTypeParametrizationFixture, Sum) {
    SumAggregatorData aggregator_data;
    const DataType dt = GetParam();
    aggregator_data.add_data_type(dt);
    if (is_signed_type(dt)) {
        constexpr DataType expected_data_type = combine_data_type(ValueType::INT, SizeBits::S64);
        ASSERT_EQ(aggregator_data.get_output_data_type(), expected_data_type);
        ASSERT_EQ(aggregator_data.get_default_value(), std::optional{Value(int64_t{0}, expected_data_type)});
    } else if (is_unsigned_type(dt) || is_bool_type(dt)) {
        constexpr DataType expected_data_type = combine_data_type(ValueType::UINT, SizeBits::S64);
        ASSERT_EQ(aggregator_data.get_output_data_type(), expected_data_type);
        ASSERT_EQ(aggregator_data.get_default_value(), std::optional{Value(uint64_t{0}, expected_data_type)});
    } else if (is_floating_point_type(dt)) {
        constexpr DataType expected_data_type = combine_data_type(ValueType::FLOAT, SizeBits::S64);
        ASSERT_EQ(aggregator_data.get_output_data_type(), expected_data_type);
        ASSERT_EQ(aggregator_data.get_default_value(), std::optional{Value(double{0}, expected_data_type)});
    } else if (is_sequence_type(dt) || is_time_type(dt) || is_bool_object_type(dt)) {
        ASSERT_THROW(aggregator_data.get_output_data_type(), SchemaException);
    } else {
        ASSERT_TRUE(is_empty_type(dt));
        constexpr DataType expected_data_type = combine_data_type(ValueType::FLOAT, SizeBits::S64);
        ASSERT_EQ(aggregator_data.get_output_data_type(), expected_data_type);
        ASSERT_EQ(aggregator_data.get_default_value(), std::optional{Value(double{0}, expected_data_type)});
    }
}

INSTANTIATE_TEST_SUITE_P(Sum, UnsortedAggregationDataTypeParametrizationFixture, ::testing::ValuesIn(all_data_types()));