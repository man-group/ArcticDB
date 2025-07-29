#include <gtest/gtest.h>
#include <arcticdb/pipeline/value.hpp>

using namespace arcticdb;

TEST(Value, DefaultConstruct) {
    const Value v;
    ASSERT_EQ(v.data_type(), arcticdb::entity::DataType::UNKNOWN);
    ASSERT_EQ(v.len(), 0);
    ASSERT_EQ(v.get<uint64_t>(), 0);
}

class ValueDataType : public ::testing::TestWithParam<DataType> {};

consteval auto data_types() {
    return std::array{
        DataType::INT8,
        DataType::INT16,
        DataType::INT32,
        DataType::UINT8,
        DataType::UINT16,
        DataType::UINT32,
        DataType::FLOAT32,
        DataType::FLOAT64,
        DataType::UTF_DYNAMIC64
    };
};

template<typename TypeTag>
requires std::derived_from<TypeTag, DataTypeTagBase>
consteval auto generate_numeric_testing_values() {
    using raw_type = typename TypeTag::raw_type;
    constexpr DataType data_type = TypeTag::data_type;
    if constexpr(is_signed_type(data_type) || is_time_type(data_type)) {
        return std::array<raw_type, 3>{-23, 0, 54};
    } else if constexpr(is_unsigned_type(data_type)) {
        return std::array<raw_type, 3>{0, 54};
    } else if constexpr(is_floating_point_type(data_type)) {
        return std::array<raw_type, 4>{-23.43, 0.0, 1e-5, 54.23 };
    } else if constexpr(is_bool_type(data_type)) {
        return std::array{true, false};
    }
}

TEST_P(ValueDataType, ValueConstruct) {
    details::visit_type(GetParam(), []<typename TypeTag>(TypeTag) {
        if constexpr (is_numeric_type(TypeTag::data_type) || is_bool_type(TypeTag::data_type)) {
            using raw_type = typename TypeTag::raw_type;
            constexpr static std::array values = generate_numeric_testing_values<TypeTag>();
            for (const auto& v : values) {
                // Construction
                Value runtime_value{v, GetParam()};
                ASSERT_EQ(runtime_value.data_type(), GetParam());
                ASSERT_EQ(runtime_value.len(), 0);
                ASSERT_EQ(runtime_value.get<raw_type>(), v);
                ASSERT_EQ(runtime_value.to_string<raw_type>(), fmt::format("{}", v));
                ASSERT_EQ(runtime_value.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Move construction
                Value moved_in{std::move(runtime_value)};
                ASSERT_EQ(moved_in.data_type(), GetParam());
                ASSERT_EQ(moved_in.len(), 0);
                ASSERT_EQ(moved_in.get<raw_type>(), v);
                ASSERT_EQ(moved_in.to_string<raw_type>(), fmt::format("{}", v));
                ASSERT_EQ(moved_in.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Move assign
                Value move_assigned;
                move_assigned = std::move(moved_in);
                ASSERT_EQ(move_assigned.data_type(), GetParam());
                ASSERT_EQ(move_assigned.len(), 0);
                ASSERT_EQ(move_assigned.get<raw_type>(), v);
                ASSERT_EQ(move_assigned.to_string<raw_type>(), fmt::format("{}", v));
                ASSERT_EQ(move_assigned.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Copy construct
                const Value copy_constructed(move_assigned);
                ASSERT_EQ(copy_constructed.data_type(), GetParam());
                ASSERT_EQ(copy_constructed.len(), 0);
                ASSERT_EQ(copy_constructed.get<raw_type>(), v);
                ASSERT_EQ(copy_constructed.to_string<raw_type>(), fmt::format("{}", v));
                ASSERT_EQ(copy_constructed.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Copy assign
                Value copy_assigned;
                copy_assigned = copy_constructed;
                ASSERT_EQ(copy_assigned.data_type(), GetParam());
                ASSERT_EQ(copy_assigned.len(), 0);
                ASSERT_EQ(copy_assigned.get<raw_type>(), v);
                ASSERT_EQ(copy_assigned.to_string<raw_type>(), fmt::format("{}", v));
                ASSERT_EQ(copy_assigned.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));
                ASSERT_EQ(copy_assigned, copy_constructed);
           }
        } else if constexpr (is_sequence_type(TypeTag::data_type)) {
            constexpr static std::array data = {"short", "very long string avoiding SSO"};
            for (const auto& str : data) {
                // Construction
                Value runtime_value{str, GetParam()};
                ASSERT_EQ(runtime_value.data_type(), GetParam());
                ASSERT_EQ(runtime_value.len(), strlen(str));
                ASSERT_EQ(std::strcmp(*runtime_value.str_data(), str), 0);
                ASSERT_EQ(runtime_value.to_string<uint64_t>(), fmt::format("\"{}\"", str));
                ASSERT_EQ(runtime_value.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Move construction
                const Value moved_in{std::move(runtime_value)};
                ASSERT_EQ(moved_in.data_type(), GetParam());
                ASSERT_EQ(moved_in.len(), strlen(str));
                ASSERT_EQ(std::strcmp(*moved_in.str_data(), str), 0);
                ASSERT_EQ(moved_in.to_string<uint64_t>(), fmt::format("\"{}\"", str));
                ASSERT_EQ(moved_in.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Move assign
                Value move_assigned;
                move_assigned = std::move(moved_in);
                ASSERT_EQ(move_assigned.data_type(), GetParam());
                ASSERT_EQ(move_assigned.len(), strlen(str));
                ASSERT_EQ(std::strcmp(*move_assigned.str_data(), str), 0);
                ASSERT_EQ(move_assigned.to_string<uint64_t>(), fmt::format("\"{}\"", str));
                ASSERT_EQ(move_assigned.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));

                // Copy construct
                const Value copy_constructed(move_assigned);
                ASSERT_EQ(copy_constructed.data_type(), GetParam());
                ASSERT_EQ(copy_constructed.len(), strlen(str));
                ASSERT_EQ(std::strcmp(*copy_constructed.str_data(), str), 0);
                ASSERT_EQ(copy_constructed.to_string<uint64_t>(), fmt::format("\"{}\"", str));
                ASSERT_EQ(copy_constructed.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));
                ASSERT_EQ(copy_constructed, move_assigned);

                // Copy assignment
                Value copy_assigned;
                copy_assigned = copy_constructed;
                ASSERT_EQ(copy_assigned.data_type(), GetParam());
                ASSERT_EQ(copy_assigned.len(), strlen(str));
                ASSERT_EQ(std::strcmp(*copy_assigned.str_data(), str), 0);
                ASSERT_EQ(copy_assigned.to_string<uint64_t>(), fmt::format("\"{}\"", str));
                ASSERT_EQ(copy_assigned.descriptor(), arcticdb::TypeDescriptor(GetParam(), Dimension::Dim0));
                ASSERT_EQ(copy_constructed, copy_assigned);
            }
        }
    });
}

INSTANTIATE_TEST_SUITE_P(ValueConstruct, ValueDataType, testing::ValuesIn(data_types()));

class ValueDataTypePair : public ::testing::TestWithParam<std::tuple<DataType, DataType>> {};

template<typename TypeTag>
requires std::derived_from<TypeTag, DataTypeTagBase>
consteval auto generate_pair_of_different_values_for_data_type() {
    using raw_type = typename TypeTag::raw_type;
    constexpr DataType data_type = TypeTag::data_type;
    if constexpr (is_signed_type(data_type) || is_time_type(data_type)) {
        return std::pair<raw_type, raw_type>{-4, 6};
    } else if constexpr (is_unsigned_type(data_type)) {
        return std::pair<raw_type, raw_type>{4, 6};
    } else if constexpr (is_bool_type(data_type)) {
        return std::pair<raw_type, raw_type>{true, false};
    } else if constexpr (is_sequence_type(data_type)) {
        return std::pair{"short", "very long value avoiding SSO"};
    } else if constexpr(is_floating_point_type(data_type)) {
        return std::pair<raw_type, raw_type>{-23.5, 37.45};
    } else {
        static_assert(sizeof(TypeTag) == 0, "Unknown type category");
    }
}

TEST_P(ValueDataTypePair, ValueDoesNotCompareEqual) {
    const DataType left_type = std::get<0>(GetParam());
    const DataType right_type = std::get<1>(GetParam());
    if (left_type == right_type) {
        details::visit_type(left_type, []<typename TypeTag>(TypeTag) {
            if constexpr(!(is_empty_type(TypeTag::data_type) || is_bool_object_type(TypeTag::data_type))) {
                const auto [left, right] = generate_pair_of_different_values_for_data_type<TypeTag>();
                ASSERT_FALSE(Value(left, TypeTag::data_type) == Value(right, TypeTag::data_type));
                ASSERT_EQ(Value(left, TypeTag::data_type), Value(left, TypeTag::data_type));
                ASSERT_EQ(Value(right, TypeTag::data_type), Value(right, TypeTag::data_type));
            }
        });
    } else {
        details::visit_type(left_type, [&]<typename LeftTypeTag>(LeftTypeTag) {
            if constexpr(!(is_empty_type(LeftTypeTag::data_type) || is_bool_object_type(LeftTypeTag::data_type))) {
                const std::pair left_raw_values = generate_pair_of_different_values_for_data_type<LeftTypeTag>();
                details::visit_type(right_type, [&]<typename RightTypeTag>(RightTypeTag) {
                    if constexpr(!(is_empty_type(RightTypeTag::data_type) || is_bool_object_type(RightTypeTag::data_type))) {
                        const std::pair right_raw_values = generate_pair_of_different_values_for_data_type<RightTypeTag>();
                        ASSERT_FALSE(Value(left_raw_values.first, LeftTypeTag::data_type) == Value(right_raw_values.first, RightTypeTag::data_type));
                        ASSERT_FALSE(Value(left_raw_values.first, LeftTypeTag::data_type) == Value(right_raw_values.second, RightTypeTag::data_type));
                        ASSERT_FALSE(Value(left_raw_values.second, LeftTypeTag::data_type) == Value(right_raw_values.second, RightTypeTag::data_type));
                        ASSERT_FALSE(Value(left_raw_values.second, LeftTypeTag::data_type) == Value(right_raw_values.first, RightTypeTag::data_type));
                    }
                });
            }
        });
    }
}

INSTANTIATE_TEST_SUITE_P(
    ValueDoesNotCompareEqual,
    ValueDataTypePair,
    testing::Combine(
        testing::ValuesIn(data_types()),
        testing::ValuesIn(data_types())
    )
);
