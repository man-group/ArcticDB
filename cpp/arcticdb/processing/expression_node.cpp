/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/operation_dispatch_unary.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {

ColumnWithStrings::ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name) :
    column_(std::move(col)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(
        std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(std::move(string_pool)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name) :
    column_(std::make_shared<Column>(std::move(col))),
    string_pool_(std::move(string_pool)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(
        std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(string_pool),
    column_name_(col_name) {}

[[nodiscard]] std::optional<std::string_view> ColumnWithStrings::string_at_offset(
        entity::position_t offset, bool strip_fixed_width_trailing_nulls
) const {
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
        while (!raw.empty() && raw.substr(raw.size() - char_width) == null_char_view) {
            raw.remove_suffix(char_width);
        }
    }
    return raw;
}

[[nodiscard]] std::optional<size_t> ColumnWithStrings::get_fixed_width_string_size() const {
    if (!column_ || !string_pool_)
        return std::nullopt;

    util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
    for (position_t i = 0; i < column_->row_count(); ++i) {
        auto offset = column_->scalar_at<entity::position_t>(i);
        if (offset != std::nullopt) {
            std::string_view raw = string_pool_->get_view(*offset);
            return raw.size();
        }
    }
    return std::nullopt;
}

namespace {

std::string label_for(const ExpressionNode::Leaf& leaf) {
    return util::variant_match(
            leaf,
            [](const ColumnName& column_name) { return fmt::format("Column[\"{}\"]", column_name.value); },
            [](const std::shared_ptr<Value>& value) {
                return details::visit_type(value->data_type(), [&value](auto tag) {
                    using info = ScalarTypeInfo<decltype(tag)>;
                    return fmt::format(
                            "Val({}:{})", value->data_type(), value->template to_string<typename info::RawType>()
                    );
                });
            },
            [](const std::shared_ptr<ValueSet>& value_set) {
                return fmt::format("ValueSet({},n={})", value_set->base_type().data_type(), value_set->size());
            },
            [](const std::shared_ptr<util::RegexGeneric>& regex) { return fmt::format("Regex({})", regex->text()); }
    );
}

std::string label_for(const ExpressionNode::Operation& op) {
    if (is_ternary_operation(op.operation_type_)) {
        return fmt::format("{} if {} else {}", op.left_->label_, op.condition_->label_, op.right_->label_);
    } else if (is_binary_operation(op.operation_type_)) {
        return fmt::format("({} {} {})", op.left_->label_, op.operation_type_, op.right_->label_);
    } else {
        return fmt::format("{}({})", op.operation_type_, op.left_->label_);
    }
}

bool value_sets_equal(ValueSet& a, ValueSet& b) {
    if (a.empty() != b.empty()) {
        return false;
    }
    if (a.base_type() != b.base_type()) {
        // This might cause some false negatives but the simplicity is worth it
        return false;
    }
    return details::visit_type(a.base_type().data_type(), [&a, &b](auto tag) {
        using info = ScalarTypeInfo<decltype(tag)>;
        if constexpr (is_sequence_type(info::data_type)) {
            return *a.get_set<std::string>() == *b.get_set<std::string>();
        } else {
            return *a.get_set<typename info::RawType>() == *b.get_set<typename info::RawType>();
        }
    });
}

bool nodes_equal(const ExpressionNode& a, const ExpressionNode& b);

bool equal_child(const std::shared_ptr<ExpressionNode>& a, const std::shared_ptr<ExpressionNode>& b) {
    if (!a && !b) {
        return true;
    }
    if (!a || !b) {
        return false;
    }
    return nodes_equal(*a, *b);
}

bool nodes_equal(const ExpressionNode& a, const ExpressionNode& b) {
    if (a.kind_.index() != b.kind_.index()) {
        return false;
    }
    // Now we know that a and b hold the same variant type member
    if (std::holds_alternative<ExpressionNode::Leaf>(a.kind_)) {
        const auto& leaf_a = std::get<ExpressionNode::Leaf>(a.kind_);
        const auto& leaf_b = std::get<ExpressionNode::Leaf>(b.kind_);
        if (leaf_a.index() != leaf_b.index()) {
            return false;
        }
        // Now we know that leaf_a and leaf_b hold the same variant type member
        return util::variant_match(
                leaf_a,
                [&leaf_b](const ColumnName& a_col) { return a_col.value == std::get<ColumnName>(leaf_b).value; },
                [&leaf_b](const std::shared_ptr<Value>& a_val) {
                    return *a_val == *std::get<std::shared_ptr<Value>>(leaf_b);
                },
                [&leaf_b](const std::shared_ptr<ValueSet>& a_set) {
                    return value_sets_equal(*a_set, *std::get<std::shared_ptr<ValueSet>>(leaf_b));
                },
                [&leaf_b](const std::shared_ptr<util::RegexGeneric>& a_regex) {
                    return a_regex->text() == std::get<std::shared_ptr<util::RegexGeneric>>(leaf_b)->text();
                }
        );
    }
    const auto& operation_a = std::get<ExpressionNode::Operation>(a.kind_);
    const auto& operation_b = std::get<ExpressionNode::Operation>(b.kind_);
    if (operation_a.operation_type_ != operation_b.operation_type_) {
        return false;
    }
    return equal_child(operation_a.condition_, operation_b.condition_) &&
           equal_child(operation_a.left_, operation_b.left_) && equal_child(operation_a.right_, operation_b.right_);
}

} // namespace

ExpressionNode::ExpressionNode(Leaf leaf) : kind_(std::move(leaf)), label_(label_for(std::get<Leaf>(kind_))) {}

ExpressionNode::ExpressionNode(
        std::shared_ptr<ExpressionNode> condition, std::shared_ptr<ExpressionNode> left,
        std::shared_ptr<ExpressionNode> right, OperationType op
) :
    kind_(Operation{op, std::move(left), std::move(right), std::move(condition)}) {
    util::check(is_ternary_operation(op), "Non-ternary expression provided with three arguments");
    label_ = label_for(std::get<Operation>(kind_));
}

ExpressionNode::ExpressionNode(
        std::shared_ptr<ExpressionNode> left, std::shared_ptr<ExpressionNode> right, OperationType op
) :
    kind_(Operation{op, std::move(left), std::move(right)}) {
    util::check(is_binary_operation(op), "Non-binary expression provided with two arguments");
    label_ = label_for(std::get<Operation>(kind_));
}

ExpressionNode::ExpressionNode(std::shared_ptr<ExpressionNode> left, OperationType op) :
    kind_(Operation{op, std::move(left)}) {
    util::check(is_unary_operation(op), "Non-unary expression provided with single argument");
    label_ = label_for(std::get<Operation>(kind_));
}

VariantData ExpressionNode::compute(ProcessingUnit& seg) const {
    return util::variant_match(
            kind_,
            [&seg](const Leaf& leaf) -> VariantData {
                return util::variant_match(
                        leaf,
                        [&seg](const ColumnName& column_name) -> VariantData { return seg.get(column_name); },
                        [](const std::shared_ptr<Value>& value) -> VariantData { return value; },
                        [](const std::shared_ptr<ValueSet>& value_set) -> VariantData { return value_set; },
                        [](const std::shared_ptr<util::RegexGeneric>& regex) -> VariantData { return regex; }
                );
            },
            [&seg, this](const Operation& op) -> VariantData {
                if (auto it = seg.computed_data_.find(label_); it != seg.computed_data_.end()) {
                    const auto& [other, cached] = it->second;
                    if (other == this || nodes_equal(*this, *other)) {
                        return cached;
                    }
                }
                VariantData result = [&seg, &op]() -> VariantData {
                    if (is_ternary_operation(op.operation_type_)) {
                        return dispatch_ternary(
                                op.condition_->compute(seg),
                                op.left_->compute(seg),
                                op.right_->compute(seg),
                                op.operation_type_
                        );
                    } else if (is_binary_operation(op.operation_type_)) {
                        return dispatch_binary(op.left_->compute(seg), op.right_->compute(seg), op.operation_type_);
                    } else {
                        return dispatch_unary(op.left_->compute(seg), op.operation_type_);
                    }
                }();
                // On a label collision the slot is already held by a structurally-different node; try_emplace is a
                // no-op there, so that node keeps the slot and this result simply isn't memoized.
                if (!seg.computed_data_.try_emplace(label_, this, result).second) {
                    log::version().debug(
                            "Expression label collision for {}; result not memoized, similar queries may see "
                            "redundant recomputation",
                            label_
                    );
                }
                return result;
            }
    );
}

std::variant<BitSetTag, DataType> ExpressionNode::compute(
        const ankerl::unordered_dense::map<std::string, DataType>& column_types
) const {
    if (std::holds_alternative<Leaf>(kind_)) {
        ValueSetState value_set_state;
        return leaf_return_type(std::get<Leaf>(kind_), column_types, value_set_state);
    }
    const auto& operation = std::get<Operation>(kind_);
    const OperationType operation_type_ = operation.operation_type_;
    // Default to BitSetTag
    std::variant<BitSetTag, DataType> res;
    ValueSetState left_value_set_state;
    auto left_type = child_return_type(*operation.left_, column_types, left_value_set_state);
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            left_value_set_state == ValueSetState::NOT_A_SET, "Unexpected value set input to {}", operation_type_
    );
    if (is_unary_operation(operation_type_)) {
        switch (operation_type_) {
        case OperationType::ABS:
        case OperationType::NEG:
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type), "Unexpected bitset input to {}", operation_type_
            );
            details::visit_type(std::get<DataType>(left_type), [operation_type_, &res](auto tag) {
                using type_info = ScalarTypeInfo<decltype(tag)>;
                if constexpr (is_numeric_type(type_info::data_type)) {
                    if (operation_type_ == OperationType::ABS) {
                        using TargetType = typename unary_operation_promoted_type<
                                typename type_info::RawType,
                                std::remove_reference_t<AbsOperator>>::type;
                        res = data_type_from_raw_type<TargetType>();
                    } else {
                        // operation_type_ == OperationType::NEG
                        using TargetType = typename unary_operation_promoted_type<
                                typename type_info::RawType,
                                std::remove_reference_t<NegOperator>>::type;
                        res = data_type_from_raw_type<TargetType>();
                    }
                } else {
                    user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                            "Unexpected data type {} input to {}", type_info::data_type, operation_type_
                    );
                }
            });
            break;
        case OperationType::ISNULL:
        case OperationType::NOTNULL:
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type), "Unexpected bitset input to {}", operation_type_
            );
            break;
        case OperationType::IDENTITY:
        case OperationType::NOT:
            if (!std::holds_alternative<BitSetTag>(left_type)) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        std::get<DataType>(left_type) == DataType::BOOL8,
                        "Unexpected data type {} input to {}",
                        std::get<DataType>(left_type),
                        operation_type_
                );
            }
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected unary operator {}", operation_type_);
        }
    } else if (is_binary_operation(operation_type_)) {
        ValueSetState right_value_set_state;
        auto right_type = child_return_type(*operation.right_, column_types, right_value_set_state);
        switch (operation_type_) {
        case OperationType::ADD:
        case OperationType::SUB:
        case OperationType::MUL:
        case OperationType::DIV:
        case OperationType::POW:
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type),
                    "Unexpected bitset input as left operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(right_type),
                    "Unexpected bitset input as right operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    right_value_set_state == ValueSetState::NOT_A_SET,
                    "Unexpected value set input to {}",
                    operation_type_
            );
            details::visit_type(std::get<DataType>(left_type), [operation_type_, &res, right_type](auto left_tag) {
                using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
                details::visit_type(std::get<DataType>(right_type), [operation_type_, &res](auto right_tag) {
                    using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
                    if constexpr (is_numeric_type(left_type_info::data_type) &&
                                  is_numeric_type(right_type_info::data_type)) {
                        switch (operation_type_) {
                        case OperationType::ADD: {
                            using TargetType = typename binary_operation_promoted_type<
                                    typename left_type_info::RawType,
                                    typename right_type_info::RawType,
                                    std::remove_reference_t<PlusOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                            break;
                        }
                        case OperationType::SUB: {
                            using TargetType = typename binary_operation_promoted_type<
                                    typename left_type_info::RawType,
                                    typename right_type_info::RawType,
                                    std::remove_reference_t<MinusOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                            break;
                        }
                        case OperationType::MUL: {
                            using TargetType = typename binary_operation_promoted_type<
                                    typename left_type_info::RawType,
                                    typename right_type_info::RawType,
                                    std::remove_reference_t<TimesOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                            break;
                        }
                        case OperationType::DIV: {
                            using TargetType = typename binary_operation_promoted_type<
                                    typename left_type_info::RawType,
                                    typename right_type_info::RawType,
                                    std::remove_reference_t<DivideOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                            break;
                        }
                        case OperationType::POW: {
                            if constexpr (!is_integer_type(right_type_info::data_type)) {
                                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                        "POW operator does not support floating point exponents, got {}",
                                        right_type_info::data_type
                                );
                            }

                            using TargetType = typename binary_operation_promoted_type<
                                    typename left_type_info::RawType,
                                    typename right_type_info::RawType,
                                    std::remove_reference_t<PowOperator>>::type;
                            res = data_type_from_raw_type<TargetType>();
                            break;
                        }

                        default:
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected binary operator");
                        }
                    } else {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                "Unexpected data types {} {} input to {}",
                                left_type_info::data_type,
                                right_type_info::data_type,
                                operation_type_
                        );
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
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type),
                    "Unexpected bitset input as left operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(right_type),
                    "Unexpected bitset input as right operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    right_value_set_state == ValueSetState::NOT_A_SET,
                    "Unexpected value set input to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    (is_numeric_type(std::get<DataType>(left_type)) && is_numeric_type(std::get<DataType>(right_type))
                    ) ||
                            (is_bool_type(std::get<DataType>(left_type)) && is_bool_type(std::get<DataType>(right_type))
                            ) ||
                            (is_sequence_type(std::get<DataType>(left_type)) &&
                             is_sequence_type(std::get<DataType>(right_type)) &&
                             (operation_type_ == OperationType::EQ || operation_type_ == OperationType::NE)),
                    "Unexpected data types {} {} input to {}",
                    std::get<DataType>(left_type),
                    std::get<DataType>(right_type),
                    operation_type_
            );
            break;
        case OperationType::REGEX_MATCH:
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type),
                    "Unexpected bitset input as left operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(right_type),
                    "Unexpected bitset input as right operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    right_value_set_state == ValueSetState::NOT_A_SET,
                    "Unexpected value set input to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    is_sequence_type(std::get<DataType>(left_type)) && is_sequence_type(std::get<DataType>(right_type)),
                    "Unexpected data types {} {} input to {}",
                    std::get<DataType>(left_type),
                    std::get<DataType>(right_type),
                    operation_type_
            );
            break;
        case OperationType::ISIN:
        case OperationType::ISNOTIN:
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(left_type),
                    "Unexpected bitset input as left operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::holds_alternative<DataType>(right_type),
                    "Unexpected bitset input as right operand to {}",
                    operation_type_
            );
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    right_value_set_state != ValueSetState::NOT_A_SET,
                    "Unexpected non value-set input as right operand to {}",
                    operation_type_
            );
            if (right_value_set_state == ValueSetState::NON_EMPTY_SET) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        (is_sequence_type(std::get<DataType>(left_type)) &&
                         is_sequence_type(std::get<DataType>(right_type))) ||
                                (is_numeric_type(std::get<DataType>(left_type)) &&
                                 is_numeric_type(std::get<DataType>(right_type))),
                        "Unexpected data types {} {} input to {}",
                        std::get<DataType>(left_type),
                        std::get<DataType>(right_type),
                        operation_type_
                );
            } // else - Empty value set compatible with all data types
            break;
        case OperationType::AND:
        case OperationType::OR:
        case OperationType::XOR:
            if (!std::holds_alternative<BitSetTag>(left_type)) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        std::get<DataType>(left_type) == DataType::BOOL8,
                        "Unexpected data type {} input as left operand to {}",
                        std::get<DataType>(left_type),
                        operation_type_
                );
            }
            if (!std::holds_alternative<BitSetTag>(right_type)) {
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        std::get<DataType>(right_type) == DataType::BOOL8,
                        "Unexpected data type {} input as right operand to {}",
                        std::get<DataType>(right_type),
                        operation_type_
                );
            }
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected binary operator {}", operation_type_);
        }
    } else {
        // Ternary operation
        ValueSetState condition_value_set_state;
        auto condition_type = child_return_type(*operation.condition_, column_types, condition_value_set_state);
        ValueSetState right_value_set_state;
        auto right_type = child_return_type(*operation.right_, column_types, right_value_set_state);
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                condition_value_set_state == ValueSetState::NOT_A_SET &&
                        right_value_set_state == ValueSetState::NOT_A_SET,
                "Unexpected value set input to {}",
                operation_type_
        );
        if (!std::holds_alternative<BitSetTag>(condition_type)) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::get<DataType>(condition_type) == DataType::BOOL8,
                    "Unexpected data type {} input as condition operand to {}",
                    std::get<DataType>(condition_type),
                    operation_type_
            );
        }
        if (std::holds_alternative<DataType>(left_type) && std::holds_alternative<DataType>(right_type)) {
            details::visit_type(std::get<DataType>(left_type), [operation_type_, &res, right_type](auto left_tag) {
                using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
                details::visit_type(std::get<DataType>(right_type), [operation_type_, &res](auto right_tag) {
                    using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
                    if constexpr (is_sequence_type(left_type_info::data_type) &&
                                  is_sequence_type(right_type_info::data_type)) {
                        if constexpr (left_type_info::data_type == right_type_info::data_type &&
                                      is_dynamic_string_type(left_type_info::data_type)) {
                            res = left_type_info::data_type;
                        } else {
                            // Fixed width string columns
                            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                    "Unexpected data types {} {} input to {}",
                                    left_type_info::data_type,
                                    right_type_info::data_type,
                                    operation_type_
                            );
                        }
                    } else if constexpr (is_numeric_type(left_type_info::data_type) &&
                                         is_numeric_type(right_type_info::data_type)) {
                        using TargetType = typename ternary_operation_promoted_type<
                                typename left_type_info::RawType,
                                typename right_type_info::RawType>::type;
                        res = data_type_from_raw_type<TargetType>();
                    } else if constexpr (is_bool_type(left_type_info::data_type) &&
                                         is_bool_type(right_type_info::data_type)) {
                        res = DataType::BOOL8;
                    } else {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                "Unexpected data types {} {} input to {}",
                                left_type_info::data_type,
                                right_type_info::data_type,
                                operation_type_
                        );
                    }
                });
            });
        } else if (std::holds_alternative<DataType>(left_type)) {
            // right_type holds a bitset, so left_type needs to be bool
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::get<DataType>(left_type) == DataType::BOOL8,
                    "Unexpected data types {}/bitset input to {}",
                    std::get<DataType>(left_type),
                    operation_type_
            );
        } else if (std::holds_alternative<DataType>(right_type)) {
            // left_type holds a bitset, so right_type needs to be bool
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    std::get<DataType>(right_type) == DataType::BOOL8,
                    "Unexpected data types bitset/{} input to {}",
                    std::get<DataType>(right_type),
                    operation_type_
            );
        } // else both hold bitsets, so the result will be a bitset
    }
    return res;
}

std::variant<BitSetTag, DataType> ExpressionNode::leaf_return_type(
        const Leaf& leaf, const ankerl::unordered_dense::map<std::string, DataType>& column_types,
        ValueSetState& value_set_state
) {
    value_set_state = ValueSetState::NOT_A_SET;
    return util::variant_match(
            leaf,
            [&column_types](const ColumnName& column_name) -> std::variant<BitSetTag, DataType> {
                auto it = column_types.find(column_name.value);
                if (it == column_types.end()) {
                    // The column might be a part of multi-index. In that case the name gets mangled so it won't be
                    // found by column_types.find(column_name.value). We need to retry with the mangled name.
                    it = column_types.find(stream::mangled_name(column_name.value));
                }
                schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
                        it != column_types.end(),
                        "Clause requires column '{}' to exist in input data",
                        column_name.value
                );
                return it->second;
            },
            [](const std::shared_ptr<Value>& value) -> std::variant<BitSetTag, DataType> { return value->data_type(); },
            [&value_set_state](const std::shared_ptr<ValueSet>& value_set) -> std::variant<BitSetTag, DataType> {
                value_set_state = value_set->empty() ? ValueSetState::EMPTY_SET : ValueSetState::NON_EMPTY_SET;
                return value_set->base_type().data_type();
            },
            [](const std::shared_ptr<util::RegexGeneric>&) -> std::variant<BitSetTag, DataType> {
                return DataType::UTF_DYNAMIC64;
            }
    );
}

std::variant<BitSetTag, DataType> ExpressionNode::child_return_type(
        const ExpressionNode& child, const ankerl::unordered_dense::map<std::string, DataType>& column_types,
        ValueSetState& value_set_state
) {
    value_set_state = ValueSetState::NOT_A_SET;
    if (std::holds_alternative<Leaf>(child.kind_)) {
        return leaf_return_type(std::get<Leaf>(child.kind_), column_types, value_set_state);
    }
    return child.compute(column_types);
}

} // namespace arcticdb
