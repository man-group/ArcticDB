/*
 * Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_ternary.hpp>

namespace arcticdb {

// TODO: See how many of these combinations can be combined
VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& left, const util::BitSet& right) {
    util::BitSet output_bitset;
    // TODO: relax condition when adding sparse support
    auto output_size = condition.size();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(left.size() == output_size && right.size() == output_size, "Mismatching bitset sizes");
    output_bitset = right;
    auto end_bit = condition.end();
    for (auto set_bit = condition.first(); set_bit < end_bit; ++set_bit) {
        output_bitset[*set_bit] = left[*set_bit];
    }
    return VariantData{std::move(output_bitset)};
}

VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& left, bool right) {
    util::BitSet output_bitset;
    // TODO: relax condition when adding sparse support
    auto output_size = condition.size();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(left.size() == output_size, "Mismatching bitset sizes");
    if (right) {
        output_bitset = ~condition | left;
    } else {
        output_bitset = condition & left;
    }
    return VariantData{std::move(output_bitset)};
}

VariantData ternary_operator(const util::BitSet& condition, bool left, const util::BitSet& right) {
    util::BitSet output_bitset;
    // TODO: relax condition when adding sparse support
    auto output_size = condition.size();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(right.size() == output_size, "Mismatching bitset sizes");
    if (left) {
        output_bitset = condition | right;
    } else {
        output_bitset = ~condition & right;
    }
    return VariantData{std::move(output_bitset)};
}

VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& left, const ColumnWithStrings& right) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(left.column_->type().data_type()) && !is_empty_type(right.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(left.column_->type().data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.column_->type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr(is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                if constexpr(left_type_info::data_type == right_type_info::data_type && is_dynamic_string_type(left_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(left_type_info::data_type), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    // TODO: Could this be more efficient?
                    size_t idx{0};
                    Column::transform<typename left_type_info::TDT, typename right_type_info::TDT, typename left_type_info::TDT>(
                            *(left.column_),
                            *(right.column_),
                            *output_column,
                            [&condition, &idx, &string_pool, &left, &right](auto left_value, auto right_value) -> typename left_type_info::RawType {
                                std::optional<std::string_view> string_at_offset;
                                if (condition[idx]) {
                                    string_at_offset = left.string_at_offset(left_value);
                                } else {
                                    string_at_offset = right.string_at_offset(right_value);
                                }
                                if (string_at_offset.has_value()) {
                                    ++idx;
                                    auto offset_string = string_pool->get(*string_at_offset);
                                    return offset_string.offset();
                                } else {
                                    return condition[idx++] ? left_value : right_value;
                                }
                            });
                }
                // TODO: Handle else?
            } else if constexpr ((is_numeric_type(left_type_info::data_type) && is_numeric_type(right_type_info::data_type)) ||
                                 (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type))) {
                // TODO: Hacky to reuse type_arithmetic_promoted_type with IsInOperator, and incorrect when there is a uint64_t and an int64_t column
                using TargetType = typename type_arithmetic_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, IsInOperator>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                // TODO: Could this be more efficient?
                size_t idx{0};
                Column::transform<typename left_type_info::TDT, typename right_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        *(left.column_),
                        *(right.column_),
                        *output_column,
                        [&condition, &idx](auto left_value, auto right_value) -> TargetType {
                            return condition[idx++] ? left_value : right_value;
                        });
            } else {
                // TODO: Add equivalent of binary_operation_with_types_to_string for ternary
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid comparison");
            }
        });
    });
    // TODO: add equivalent of binary_operation_column_name for ternary operator
    return {ColumnWithStrings(std::move(output_column), string_pool, "some string")};
}

VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, const Value& val) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.type().data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type) && is_sequence_type(val_type_info::data_type)) {
                if constexpr(col_type_info::data_type == val_type_info::data_type && is_dynamic_string_type(col_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(col_type_info::data_type), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    auto value = std::string(*val.str_data(), val.len());
                    // TODO: Could this be more efficient?
                    size_t idx{0};
                    Column::transform<typename col_type_info::TDT, typename col_type_info::TDT>(
                            *(col.column_),
                            *output_column,
                            [&condition, &idx, &string_pool, &col, &value](auto col_value) -> typename col_type_info::RawType {
                                std::optional<std::string_view> string_at_offset;
                                if (condition[idx++]) {
                                    string_at_offset = col.string_at_offset(col_value);
                                } else {
                                    string_at_offset = value;
                                }
                                if (string_at_offset.has_value()) {
                                    auto offset_string = string_pool->get(*string_at_offset);
                                    return offset_string.offset();
                                } else {
                                    // string_at_offset will only be valueless if the condition was true and so was
                                    // selected from the column
                                    return col_value;
                                }
                            });
                }
                // TODO: Handle else?
            } else if constexpr ((is_numeric_type(col_type_info::data_type) && is_numeric_type(val_type_info::data_type)) ||
                                 (is_bool_type(col_type_info::data_type) && is_bool_type(val_type_info::data_type))) {
                // TODO: Hacky to reuse type_arithmetic_promoted_type with IsInOperator, and incorrect when there is a uint64_t and an int64_t column
                using TargetType = typename type_arithmetic_promoted_type<typename col_type_info::RawType, typename val_type_info::RawType, IsInOperator>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
                // TODO: Could this be more efficient?
                size_t idx{0};
                Column::transform<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        *(col.column_),
                        *output_column,
                        [&condition, &idx, value](auto col_value) -> TargetType {
                            return condition[idx++] ? col_value : value;
                        });
            } else {
                // TODO: Add equivalent of binary_operation_with_types_to_string for ternary
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid comparison");
            }
        });
    });
    // TODO: add equivalent of binary_operation_column_name for ternary operator
    return {ColumnWithStrings(std::move(output_column), string_pool, "some string")};
}

VariantData ternary_operator(const util::BitSet& condition, const Value& val, const ColumnWithStrings& col) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()),
            "Empty column provided to ternary operator");
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.type().data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr(is_sequence_type(col_type_info::data_type) && is_sequence_type(val_type_info::data_type)) {
                if constexpr(col_type_info::data_type == val_type_info::data_type && is_dynamic_string_type(col_type_info::data_type)) {
                    output_column = std::make_unique<Column>(make_scalar_type(col_type_info::data_type), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    auto value = std::string(*val.str_data(), val.len());
                    // TODO: Could this be more efficient?
                    size_t idx{0};
                    Column::transform<typename col_type_info::TDT, typename col_type_info::TDT>(
                            *(col.column_),
                            *output_column,
                            [&condition, &idx, &string_pool, &col, &value](auto col_value) -> typename col_type_info::RawType {
                                std::optional<std::string_view> string_at_offset;
                                if (condition[idx++]) {
                                    string_at_offset = value;
                                } else {
                                    string_at_offset = col.string_at_offset(col_value);
                                }
                                if (string_at_offset.has_value()) {
                                    auto offset_string = string_pool->get(*string_at_offset);
                                    return offset_string.offset();
                                } else {
                                    // string_at_offset will only be valueless if the condition was true and so was
                                    // selected from the column
                                    return col_value;
                                }
                            });
                }
                // TODO: Handle else?
            } else if constexpr ((is_numeric_type(col_type_info::data_type) && is_numeric_type(val_type_info::data_type)) ||
                                 (is_bool_type(col_type_info::data_type) && is_bool_type(val_type_info::data_type))) {
                // TODO: Hacky to reuse type_arithmetic_promoted_type with IsInOperator, and incorrect when there is a uint64_t and an int64_t column
                using TargetType = typename type_arithmetic_promoted_type<typename col_type_info::RawType, typename val_type_info::RawType, IsInOperator>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
                // TODO: Could this be more efficient?
                size_t idx{0};
                Column::transform<typename col_type_info::TDT, ScalarTagType<DataTypeTag<output_data_type>>>(
                        *(col.column_),
                        *output_column,
                        [&condition, &idx, value](auto col_value) -> TargetType {
                            return condition[idx++] ? value : col_value;
                        });
            } else {
                // TODO: Add equivalent of binary_operation_with_types_to_string for ternary
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid comparison");
            }
        });
    });
    // TODO: add equivalent of binary_operation_column_name for ternary operator
    return {ColumnWithStrings(std::move(output_column), string_pool, "some string")};
}

VariantData visit_ternary_operator(const VariantData& condition, const VariantData& left, const VariantData& right) {
    auto transformed_condition = transform_to_bitset(condition);

    return std::visit(util::overload {
            [] (const util::BitSet& c, const util::BitSet& l, const util::BitSet& r) -> VariantData {
                auto result = ternary_operator(c, l, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const util::BitSet& l, const ColumnWithStrings& r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                auto result = ternary_operator(c, l, bitset);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const ColumnWithStrings& l, const util::BitSet& r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                auto result = ternary_operator(c, bitset, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const util::BitSet& l, const std::shared_ptr<Value>& r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(r->data_type_), "Invalid input types to ternary operator");
                auto value = r->get<bool>();
                auto result = ternary_operator(c, l, value);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const std::shared_ptr<Value>& l, const util::BitSet& r) -> VariantData {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(is_bool_type(l->data_type_), "Invalid input types to ternary operator");
                auto value = l->get<bool>();
                auto result = ternary_operator(c, value, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const util::BitSet& l, FullResult) -> VariantData {
                auto result = ternary_operator(c, l, true);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, FullResult, const util::BitSet& r) -> VariantData {
                auto result = ternary_operator(c, true, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const util::BitSet& l, EmptyResult) -> VariantData {
                auto result = ternary_operator(c, l, false);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, EmptyResult, const util::BitSet& r) -> VariantData {
                auto result = ternary_operator(c, false, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const ColumnWithStrings& l, const ColumnWithStrings& r) -> VariantData {
                auto result = ternary_operator(c, l, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const ColumnWithStrings& l, const std::shared_ptr<Value>& r) -> VariantData {
                auto result = ternary_operator(c, l, *r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const std::shared_ptr<Value>& l, const ColumnWithStrings& r) -> VariantData {
                auto result = ternary_operator(c, *l, r);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const ColumnWithStrings& l, FullResult) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                auto result = ternary_operator(c, bitset, true);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, FullResult, const ColumnWithStrings& r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                auto result = ternary_operator(c, true, bitset);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, const ColumnWithStrings& l, EmptyResult) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                auto result = ternary_operator(c, bitset, false);
                return transform_to_placeholder(result);
            },
            [] (const util::BitSet& c, EmptyResult, const ColumnWithStrings& r) -> VariantData {
                auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                auto result = ternary_operator(c, false, bitset);
                return transform_to_placeholder(result);
            },
            [](const auto &, const auto&, const auto&) -> VariantData {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid input types to ternary operator");
                return EmptyResult{};
            }
    }, transformed_condition, left, right);
}

VariantData dispatch_ternary(const VariantData& condition, const VariantData& left, const VariantData& right, OperationType operation) {
    switch(operation) {
        case OperationType::TERNARY:
            return visit_ternary_operator(condition, left, right);
        default:
            util::raise_rte("Unknown operation {}", int(operation));
    }
}

}
