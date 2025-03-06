/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/processing/operation_dispatch_unary.hpp>

namespace arcticdb {

[[nodiscard]] std::optional<std::string_view> ColumnWithStrings::string_at_offset(entity::position_t offset, bool strip_fixed_width_trailing_nulls) const {
    if (UNLIKELY(!column_ || !string_pool_))
        return std::nullopt;
    util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
    if (!is_a_string(offset)) {
        return std::nullopt;
    }
    std::string_view raw = string_pool_->get_view(offset);
    if (strip_fixed_width_trailing_nulls && is_fixed_string_type(column_->type().data_type())) {
        auto char_width = is_utf_type(slice_value_type(column_->type().data_type())) ? UNICODE_WIDTH : ASCII_WIDTH;
        const std::string_view null_char_view("\0\0\0\0", char_width);
        while(!raw.empty() && raw.substr(raw.size() - char_width) == null_char_view) {
            raw.remove_suffix(char_width);
        }
    }
    return raw;
}

[[nodiscard]] std::optional<size_t> ColumnWithStrings::get_fixed_width_string_size() const {
    if (!column_ || !string_pool_)
        return std::nullopt;

    util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
    for(position_t i = 0; i < column_->row_count(); ++i) {
        auto offset = column_->scalar_at<entity::position_t>(i);
        if (offset != std::nullopt) {
            std::string_view raw = string_pool_->get_view(*offset);
            return raw.size();
        }
    }
    return std::nullopt;
}

ExpressionNode::ExpressionNode(VariantNode left, VariantNode right, OperationType op) :
    left_(std::move(left)),
    right_(std::move(right)),
    operation_type_(op) {
    util::check(is_binary_operation(op), "Left and right expressions supplied to non-binary operator");
}

ExpressionNode::ExpressionNode(VariantNode left, OperationType op) :
    left_(std::move(left)),
    operation_type_(op) {
    util::check(!is_binary_operation(op), "Binary expression expects both left and right children");
}

VariantData ExpressionNode::compute(ProcessingUnit& seg) const {
    if (is_binary_operation(operation_type_)) {
        return dispatch_binary(seg.get(left_), seg.get(right_), operation_type_);
    } else {
        return dispatch_unary(seg.get(left_), operation_type_);
    }
}

    std::variant<BitSetTag, DataType> ExpressionNode::compute(
            const ExpressionContext& expression_context,
            const ankerl::unordered_dense::map<std::string, DataType>& column_types) const {
    // Default to BitSetTag
    std::variant<BitSetTag, DataType> res;
    ValueSetState left_value_set_state;
    auto left_type = child_return_type(left_, expression_context, column_types, left_value_set_state);
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(left_value_set_state == ValueSetState::NOT_A_SET,
                                                          "Unexpected value set operand");
    if (is_unary_operation(operation_type_)) {
        switch (operation_type_) {
            case OperationType::ABS:
            case OperationType::NEG:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(left_type), "Unexpected bitset input to unary arithmetic operation");
                details::visit_type(std::get<DataType>(left_type), [this, &res](auto tag) {
                    using type_info = ScalarTypeInfo<decltype(tag)>;
                    if constexpr (is_numeric_type(type_info::data_type)) {
                        if (operation_type_ == OperationType::ABS) {
                            using TargetType = typename unary_arithmetic_promoted_type<typename type_info::RawType, std::remove_reference_t<AbsOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                        } else {
                            // operation_type_ == OperationType::NEG
                            using TargetType = typename unary_arithmetic_promoted_type<typename type_info::RawType, std::remove_reference_t<NegOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                        }
                    } else {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unexpected data type input to unary arithmetic operation {}",
                                                                              type_info::data_type);
                    }
                });
                break;
            case OperationType::ISNULL:
            case OperationType::NOTNULL:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        std::holds_alternative<DataType>(left_type) && (is_floating_point_type(std::get<DataType>(left_type)) || is_sequence_type(std::get<DataType>(left_type)) ||
                                is_time_type(std::get<DataType>(left_type))),
                        "Unexpected data type input to unary comparison operation");
                break;
            case OperationType::IDENTITY:
            case OperationType::NOT:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(!std::holds_alternative<DataType>(left_type) || std::get<DataType>(left_type) == DataType::BOOL8,
                                                                    "Unexpected data type input to unary boolean operation");
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected unary operator");
        }
    } else {
        // Binary operation
        ValueSetState right_value_set_state;
        auto right_type = child_return_type(right_, expression_context, column_types, right_value_set_state);
        switch (operation_type_) {
            case OperationType::ADD:
            case OperationType::SUB:
            case OperationType::MUL:
            case OperationType::DIV:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(left_type), "Unexpected bitset input to binary arithmetic operator");
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(right_type) && right_value_set_state == ValueSetState::NOT_A_SET, "Unexpected input to binary arithmetic operator");
                details::visit_type(std::get<DataType>(left_type), [this, &res, right_type](auto left_tag) {
                    using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
                    details::visit_type(std::get<DataType>(right_type), [this, &res](auto right_tag) {
                        using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
                        if constexpr (is_numeric_type(left_type_info::data_type) && is_numeric_type(right_type_info::data_type)) {
                            switch (operation_type_) {
                                case OperationType::ADD: {
                                    using TargetType = typename type_arithmetic_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<PlusOperator>>::type;
                                    res = data_type_from_raw_type<TargetType>();
                                    break;
                                }
                                case OperationType::SUB: {
                                    using TargetType = typename type_arithmetic_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<MinusOperator>>::type;
                                    res = data_type_from_raw_type<TargetType>();
                                    break;
                                }
                                case OperationType::MUL: {
                                    using TargetType = typename type_arithmetic_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<TimesOperator>>::type;
                                    res = data_type_from_raw_type<TargetType>();
                                    break;
                                }
                                case OperationType::DIV: {
                                    using TargetType = typename type_arithmetic_promoted_type<typename left_type_info::RawType, typename right_type_info::RawType, std::remove_reference_t<DivideOperator>>::type;
                                    res = data_type_from_raw_type<TargetType>();
                                    break;
                                }
                                default:
                                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected binary operator");
                            }
                        } else {
                            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unexpected data types input to binary arithmetic operation {} {}",
                                                                                  left_type_info::data_type, right_type_info::data_type);
                        }
                    });
                });
                break;
            case OperationType::EQ:
            case OperationType::NE:
            case OperationType::LT:
            case OperationType::LE:
            case OperationType::GT:
            case OperationType::GE:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(left_type), "Unexpected bitset input to binary comparison operator");
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(right_type) && right_value_set_state == ValueSetState::NOT_A_SET, "Unexpected input to binary comparison operator");
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        (is_numeric_type(std::get<DataType>(left_type)) && is_numeric_type(std::get<DataType>(right_type))) ||
                        (is_bool_type(std::get<DataType>(left_type)) && is_bool_type(std::get<DataType>(right_type))) ||
                        (is_sequence_type(std::get<DataType>(left_type)) && is_sequence_type(std::get<DataType>(right_type)) && (operation_type_ == OperationType::EQ || operation_type_ == OperationType::NE)),
                        "Incompatible data types provided in binary comparison {}, {}",
                        std::get<DataType>(left_type), std::get<DataType>(right_type));
                break;
            case OperationType::ISIN:
            case OperationType::ISNOTIN:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(left_type), "Unexpected bitset input to binary comparison operator");
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(std::holds_alternative<DataType>(right_type) && right_value_set_state != ValueSetState::NOT_A_SET, "Unexpected input to binary comparison operator");
                if (right_value_set_state == ValueSetState::NON_EMPTY_SET) {
                    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            (is_sequence_type(std::get<DataType>(left_type)) && is_sequence_type(std::get<DataType>(right_type))) || (is_numeric_type(std::get<DataType>(left_type)) && is_numeric_type(std::get<DataType>(right_type))),
                            "Incompatible data types provided in set membership operator {}, {}",
                            std::get<DataType>(left_type), std::get<DataType>(right_type));
                } // else - Empty value set compatible with all data types
                break;
            case OperationType::AND:
            case OperationType::OR:
            case OperationType::XOR:
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        (!std::holds_alternative<DataType>(left_type) || std::get<DataType>(left_type) == DataType::BOOL8) && (!std::holds_alternative<DataType>(right_type) || std::get<DataType>(right_type) == DataType::BOOL8),
                        "Unexpected data types input to binary boolean operation");
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected binary operator");
        }
    }
    return res;
}

    std::variant<BitSetTag, DataType> ExpressionNode::child_return_type(
            const VariantNode& child,
            const ExpressionContext& expression_context,
            const ankerl::unordered_dense::map<std::string, DataType>& column_types,
            ValueSetState& value_set_state) const {
    value_set_state = ValueSetState::NOT_A_SET;
    return util::variant_match(
            child,
            [&column_types] (const ColumnName& column_name) -> std::variant<BitSetTag, DataType> {
                auto it = column_types.find(column_name.value);
                schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(it != column_types.end(),
                                                                "Clause requires column '{}' to exist in input data"
                        ,column_name.value);
                return it->second;
            },
            [&expression_context] (const ValueName& value_name) -> std::variant<BitSetTag, DataType> {
                return expression_context.values_.get_value(value_name.value)->data_type_;
            },
            [&expression_context, &value_set_state] (const ValueSetName& value_set_name) -> std::variant<BitSetTag, DataType> {
                auto value_set = expression_context.value_sets_.get_value(value_set_name.value);
                value_set_state = value_set->empty() ? ValueSetState::EMPTY_SET : ValueSetState::NON_EMPTY_SET;
                return value_set->base_type().data_type();
            },
            [&expression_context, &column_types] (const ExpressionName& expression_name) -> std::variant<BitSetTag, DataType> {
                auto expr = expression_context.expression_nodes_.get_value(expression_name.value);
                return expr->compute(expression_context, column_types);
            },
            [] (auto&&) -> std::variant<BitSetTag, DataType> {
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected expression argument type");
                return {};
            }
    );
}

} //namespace arcticdb
