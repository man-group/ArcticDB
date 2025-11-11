#include <gtest/gtest.h>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/util/test/test_utils.hpp>

using namespace arcticdb;

class UnsortedAggregationDataTypeParametrizationFixture : public ::testing::TestWithParam<DataType> {};

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

TEST_P(UnsortedAggregationDataTypeParametrizationFixture, Mean) {
    MeanAggregatorData aggregator_data;
    const DataType dt = GetParam();
    if (!(is_numeric_type(dt) || is_bool_type(dt) || is_empty_type(dt))) {
        ASSERT_THROW(aggregator_data.add_data_type(dt), SchemaException);
    } else {
        aggregator_data.add_data_type(dt);
        if (is_time_type(dt)) {
            ASSERT_EQ(aggregator_data.get_output_data_type(), dt);
        } else {
            ASSERT_EQ(aggregator_data.get_output_data_type(), DataType::FLOAT64);
        }
    }
}
INSTANTIATE_TEST_SUITE_P(
        AllTypes, UnsortedAggregationDataTypeParametrizationFixture, ::testing::ValuesIn(all_data_types())
);

class AggregationResult : public ::testing::TestWithParam<DataType> {
  public:
    template<typename InputTypeTag>
    requires util::instantiation_of<InputTypeTag, TypeDescriptorTag>
    static constexpr auto get_input_mean() {
        constexpr DataType input_data_type = InputTypeTag::data_type();
        using InputRawType = typename InputTypeTag::DataTypeTag::raw_type;
        if constexpr (is_unsigned_type(input_data_type)) {
            return std::array<InputRawType, 7>{5, 0, 1, 10, 5, 6, 4};
        } else if constexpr (is_signed_type(input_data_type) || is_floating_point_type(input_data_type) ||
                             is_time_type(input_data_type)) {
            return std::array<InputRawType, 14>{0, -4, 5, 1, -6, 0, -5, 5, -1, 4, 6, -5, -10, 10};
        } else if constexpr (is_bool_type(InputTypeTag::data_type())) {
            return std::array<InputRawType, 12>{
                    true, false, true, false, false, false, true, true, true, true, false, false
            };
        } else if constexpr (is_empty_type(InputTypeTag::data_type())) {
            return std::array<InputRawType, 0>{};
        }
    }

    template<typename InputTypeTag>
    requires util::instantiation_of<InputTypeTag, TypeDescriptorTag>
    static constexpr auto get_expected_result_mean() {
        if constexpr (is_time_type(InputTypeTag::data_type())) {
            return std::array<typename InputTypeTag::DataTypeTag::raw_type, 6>{3, 3, -3, -3, 10, -10};
        } else if constexpr (is_signed_type(InputTypeTag::data_type()) ||
                             is_floating_point_type(InputTypeTag::data_type())) {
            return std::array{
                    (1 + 4 + 5) / 3.0, (0 + 5 + 6) / 3.0, -(1 + 4 + 5) / 3.0, -(0 + 5 + 6) / 3.0, 10.0, -10.0
            };
        } else if constexpr (is_unsigned_type(InputTypeTag::data_type())) {
            return std::array{(1 + 4 + 5) / 3.0, (0 + 5 + 6) / 3.0, 10.0};
        }
        if constexpr (is_bool_type(InputTypeTag::data_type())) {
            return std::array{2 / 3.0, 0.0, 1.0, 1 / 3.0};
        } else if constexpr (is_empty_type(InputTypeTag::data_type())) {
            return std::array<double, 0>{};
        }
    }

    template<typename InputTypeTag>
    requires util::instantiation_of<InputTypeTag, TypeDescriptorTag>
    static std::vector<size_t> get_groups_mean() {
        constexpr DataType input_data_type = InputTypeTag::data_type();
        if constexpr (is_unsigned_type(input_data_type)) {
            return std::vector<size_t>{0, 1, 0, 2, 1, 1, 0};
        } else if constexpr (is_signed_type(input_data_type) || is_floating_point_type(input_data_type) ||
                             is_time_type(input_data_type)) {
            return std::vector<size_t>{3, 2, 1, 0, 3, 1, 2, 0, 2, 0, 1, 3, 5, 4};
        } else if constexpr (is_bool_type(InputTypeTag::data_type())) {
            return std::vector<size_t>{0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3};
        } else if constexpr (is_empty_type(input_data_type)) {
            return std::vector<size_t>{0, 1, 0, 2, 2};
        }
    }

    template<typename InputTypeTag>
    requires util::instantiation_of<InputTypeTag, TypeDescriptorTag>
    static constexpr size_t get_group_count_mean() {
        constexpr DataType input_data_type = InputTypeTag::data_type();
        if constexpr (is_unsigned_type(input_data_type)) {
            return 3;
        } else if constexpr (is_signed_type(input_data_type) || is_floating_point_type(input_data_type) ||
                             is_time_type(input_data_type)) {
            return 6;
        } else if constexpr (is_bool_type(InputTypeTag::data_type())) {
            return 4;
        } else if constexpr (is_empty_type(input_data_type)) {
            return 3;
        }
    }
};

TEST_P(AggregationResult, Mean) {
    details::visit_type(GetParam(), []<typename TypeTag>(TypeTag) {
        if constexpr (is_allowed_mean_input(TypeTag::data_type)) {
            using OutputDataTypeTag = std::conditional_t<
                    is_time_type(TypeTag::data_type),
                    ScalarTagType<TypeTag>,
                    ScalarTagType<DataTypeTag<DataType::FLOAT64>>>;
            using InputDataTypeTag = ScalarTagType<TypeTag>;
            MeanAggregatorData aggregator_data;
            aggregator_data.add_data_type(GetParam());
            const std::vector<size_t> groups = get_groups_mean<InputDataTypeTag>();
            constexpr size_t group_count = get_group_count_mean<InputDataTypeTag>();
            constexpr std::array data = get_input_mean<InputDataTypeTag>();
            if constexpr (!is_empty_type(TypeTag::data_type)) {
                ASSERT_EQ(groups.size(), data.size());
            } else {
                ASSERT_EQ(data.size(), 0);
            }
            const ColumnWithStrings input(
                    Column::create_dense_column(data, InputDataTypeTag::type_descriptor()), nullptr, "input"
            );
            aggregator_data.aggregate(input, groups, group_count);
            const SegmentInMemory result = aggregator_data.finalize(ColumnName{"output"}, false, group_count);
            ASSERT_EQ(result.field(0).type(), make_scalar_type(OutputDataTypeTag::data_type()));
            ASSERT_EQ(result.field(0).name(), "output");
            const Column& aggregated_column = result.column(0);
            if constexpr (!is_empty_type(TypeTag::data_type)) {
                ASSERT_EQ(aggregated_column.row_count(), group_count);
            } else {
                ASSERT_EQ(aggregated_column.row_count(), 0);
            }
            constexpr static std::array expected = get_expected_result_mean<InputDataTypeTag>();
            arcticdb::for_each_enumerated<OutputDataTypeTag>(aggregated_column, [&](const auto& row) {
                ASSERT_EQ(row.value(), expected[row.idx()]);
            });
        }
    });
}

INSTANTIATE_TEST_SUITE_P(Mean, AggregationResult, ::testing::ValuesIn(allowed_mean_input_types()));
