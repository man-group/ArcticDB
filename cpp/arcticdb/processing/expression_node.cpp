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

namespace {

// A name embeds its operands' names, so it would grow like a label does. It only appears in error messages.
std::string bounded_expression_name(std::string_view name) {
    constexpr size_t max_length = 1024;
    return name.size() > max_length ? fmt::format("{}...", name.substr(0, max_length)) : std::string(name);
}

} // namespace

ColumnWithStrings::ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name) :
    column_(std::move(col)),
    column_name_(bounded_expression_name(col_name)) {}

ColumnWithStrings::ColumnWithStrings(
        std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(std::move(string_pool)),
    column_name_(bounded_expression_name(col_name)) {}

ColumnWithStrings::ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name) :
    column_(std::make_shared<Column>(std::move(col))),
    string_pool_(std::move(string_pool)),
    column_name_(bounded_expression_name(col_name)) {}

ColumnWithStrings::ColumnWithStrings(
        std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(string_pool),
    column_name_(bounded_expression_name(col_name)) {}

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
        auto char_width = is_utf_type(slice_value_type(column_->type().data_type())) ? UTF32_WIDTH : ASCII_WIDTH;
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

size_t combine_hash(size_t seed, size_t value) {
    return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

size_t hash_for(const ExpressionNode::Operation& op) {
    size_t hash = std::hash<int>{}(static_cast<int>(op.operation_type_));
    for (const auto* child : {op.condition_.get(), op.left_.get(), op.right_.get()}) {
        hash = combine_hash(hash, child ? child->hash_ : 0);
    }
    return hash;
}

// Labels embed their operands' labels, so without a bound their size would grow quadratically with the depth of an
// expression, and exponentially with the depth of a shared sub-expression
constexpr size_t max_label_length = 1024;

// Bounds what a processing unit retains for an expression with very many nodes. Beyond it results are not memoized,
// which only costs recomputation for a structurally equal expression in a later clause.
constexpr size_t max_memoized_results = 128;

// A truncated label ends in the hash so that labels, and so QueryBuilder comparison, still tell expressions apart
std::string label_for(const ExpressionNode::Operation& op, size_t hash) {
    std::string label;
    if (is_ternary_operation(op.operation_type_)) {
        label = fmt::format("{} if {} else {}", op.left_->label_, op.condition_->label_, op.right_->label_);
    } else if (is_binary_operation(op.operation_type_)) {
        label = fmt::format("({} {} {})", op.left_->label_, op.operation_type_, op.right_->label_);
    } else {
        label = fmt::format("{}({})", op.operation_type_, op.left_->label_);
    }
    if (label.size() > max_label_length) {
        label.resize(max_label_length);
        label.append(fmt::format("...#{:x}", hash));
    }
    return label;
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

bool leaves_equal(const ExpressionNode::Leaf& leaf_a, const ExpressionNode::Leaf& leaf_b) {
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

struct NodePairHash {
    size_t operator()(const std::pair<const ExpressionNode*, const ExpressionNode*>& pair) const {
        return combine_hash(std::hash<const void*>{}(pair.first), std::hash<const void*>{}(pair.second));
    }
};

// Iterative, and compares each pair of nodes once, so deep and shared sub-expressions are cheap to compare
bool nodes_equal(const ExpressionNode& a, const ExpressionNode& b) {
    using NodePair = std::pair<const ExpressionNode*, const ExpressionNode*>;
    std::vector<NodePair> stack{{&a, &b}};
    ankerl::unordered_dense::set<NodePair, NodePairHash> compared;
    while (!stack.empty()) {
        const auto [x, y] = stack.back();
        stack.pop_back();
        if (x == y) {
            continue;
        }
        if (!x || !y || x->hash_ != y->hash_ || x->kind_.index() != y->kind_.index()) {
            return false;
        }
        if (!compared.emplace(x, y).second) {
            continue;
        }
        // Now we know that x and y hold the same variant type member
        if (std::holds_alternative<ExpressionNode::Leaf>(x->kind_)) {
            if (!leaves_equal(std::get<ExpressionNode::Leaf>(x->kind_), std::get<ExpressionNode::Leaf>(y->kind_))) {
                return false;
            }
            continue;
        }
        const auto& operation_x = std::get<ExpressionNode::Operation>(x->kind_);
        const auto& operation_y = std::get<ExpressionNode::Operation>(y->kind_);
        if (operation_x.operation_type_ != operation_y.operation_type_) {
            return false;
        }
        stack.emplace_back(operation_x.right_.get(), operation_y.right_.get());
        stack.emplace_back(operation_x.left_.get(), operation_y.left_.get());
        stack.emplace_back(operation_x.condition_.get(), operation_y.condition_.get());
    }
    return true;
}

} // namespace

ExpressionNode::ExpressionNode(Leaf leaf) :
    kind_(std::move(leaf)),
    label_(label_for(std::get<Leaf>(kind_))),
    hash_(std::hash<std::string>{}(label_)) {}

ExpressionNode::ExpressionNode(
        std::shared_ptr<ExpressionNode> condition, std::shared_ptr<ExpressionNode> left,
        std::shared_ptr<ExpressionNode> right, OperationType op
) :
    kind_(Operation{op, std::move(left), std::move(right), std::move(condition)}) {
    util::check(is_ternary_operation(op), "Non-ternary expression provided with three arguments");
    hash_ = hash_for(std::get<Operation>(kind_));
    label_ = label_for(std::get<Operation>(kind_), hash_);
}

ExpressionNode::ExpressionNode(
        std::shared_ptr<ExpressionNode> left, std::shared_ptr<ExpressionNode> right, OperationType op
) :
    kind_(Operation{op, std::move(left), std::move(right)}) {
    util::check(is_binary_operation(op), "Non-binary expression provided with two arguments");
    hash_ = hash_for(std::get<Operation>(kind_));
    label_ = label_for(std::get<Operation>(kind_), hash_);
}

ExpressionNode::ExpressionNode(std::shared_ptr<ExpressionNode> left, OperationType op) :
    kind_(Operation{op, std::move(left)}) {
    util::check(is_unary_operation(op), "Non-unary expression provided with single argument");
    hash_ = hash_for(std::get<Operation>(kind_));
    label_ = label_for(std::get<Operation>(kind_), hash_);
}

ExpressionNode::~ExpressionNode() {
    auto* operation = std::get_if<Operation>(&kind_);
    if (!operation) {
        return;
    }
    // Release descendants one at a time: a node is only taken apart once this is its last owner
    std::vector<std::shared_ptr<ExpressionNode>> pending;
    const auto adopt_operands = [&pending](Operation& op) {
        for (auto* child : {&op.left_, &op.right_, &op.condition_}) {
            if (*child) {
                pending.emplace_back(std::move(*child));
            }
        }
    };
    adopt_operands(*operation);
    while (!pending.empty()) {
        auto node = std::move(pending.back());
        pending.pop_back();
        if (node.use_count() == 1) {
            if (auto* child_operation = std::get_if<Operation>(&node->kind_)) {
                adopt_operands(*child_operation);
            }
        }
    }
}

VariantData ExpressionNode::compute(ProcessingUnit& seg) const {
    return evaluate_bottom_up<VariantData>(
            *this,
            [&seg](const Leaf& leaf) -> VariantData {
                return util::variant_match(
                        leaf,
                        [&seg](const ColumnName& column_name) -> VariantData { return seg.get(column_name); },
                        [](const std::shared_ptr<Value>& value) -> VariantData { return value; },
                        [](const std::shared_ptr<ValueSet>& value_set) -> VariantData { return value_set; },
                        [](const std::shared_ptr<util::RegexGeneric>& regex) -> VariantData { return regex; }
                );
            },
            [&seg](const ExpressionNode& node) -> std::optional<VariantData> {
                if (auto it = seg.computed_data_.find(node.label_); it != seg.computed_data_.end()) {
                    const auto& [other, cached] = it->second;
                    if (other == &node || nodes_equal(node, *other)) {
                        return cached;
                    }
                }
                return std::nullopt;
            },
            [&seg](const ExpressionNode& node, const Operation& op, const auto& operand) -> VariantData {
                VariantData result = [&op, &operand]() -> VariantData {
                    if (is_ternary_operation(op.operation_type_)) {
                        return dispatch_ternary(
                                operand(op.condition_), operand(op.left_), operand(op.right_), op.operation_type_
                        );
                    } else if (is_binary_operation(op.operation_type_)) {
                        return dispatch_binary(operand(op.left_), operand(op.right_), op.operation_type_);
                    } else {
                        return dispatch_unary(operand(op.left_), op.operation_type_);
                    }
                }();
                // On a label collision the slot is already held by a structurally-different node; try_emplace is a
                // no-op there, so that node keeps the slot and this result simply isn't memoized.
                if (seg.computed_data_.size() < max_memoized_results &&
                    !seg.computed_data_.try_emplace(node.label_, &node, result).second) {
                    log::version().debug(
                            "Expression label collision for {}; result not memoized, similar queries may see "
                            "redundant recomputation",
                            node.label_
                    );
                }
                return result;
            }
    );
}

std::variant<BitSetTag, DataType> ExpressionNode::compute(
        const ankerl::unordered_dense::map<std::string, DataType>& column_types
) const {
    return evaluate_bottom_up<ReturnType>(
                   *this,
                   [&column_types](const Leaf& leaf) {
                       ReturnType res;
                       res.type_ = leaf_return_type(leaf, column_types, res.value_set_state_);
                       return res;
                   },
                   [](const ExpressionNode&) -> std::optional<ReturnType> { return std::nullopt; },
                   [](const ExpressionNode&, const Operation& operation, const auto& operand) {
                       return ReturnType{
                               operation_return_type(
                                       operation,
                                       operand(operation.left_),
                                       operation.right_ ? &operand(operation.right_) : nullptr,
                                       operation.condition_ ? &operand(operation.condition_) : nullptr
                               ),
                               ValueSetState::NOT_A_SET
                       };
                   }
    ).type_;
}

std::variant<BitSetTag, DataType> ExpressionNode::operation_return_type(
        const Operation& operation, const ReturnType& left, const ReturnType* right, const ReturnType* condition
) {
    const OperationType operation_type_ = operation.operation_type_;
    // Default to BitSetTag
    std::variant<BitSetTag, DataType> res;
    const ValueSetState left_value_set_state = left.value_set_state_;
    const auto& left_type = left.type_;
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
            details::visit_type(std::get<DataType>(left_type), [operation_type_, &res, &operation](auto tag) {
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
                            "Cannot perform unary operation {} ({})",
                            unary_operation_to_string(operation_type_, operation.left_->label_),
                            get_user_friendly_type_string(type_info::TDT::type_descriptor())
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
        const ValueSetState right_value_set_state = right->value_set_state_;
        const auto& right_type = right->type_;
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
            details::visit_type(
                    std::get<DataType>(left_type),
                    [operation_type_, &res, right_type, &operation](auto left_tag) {
                        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
                        details::visit_type(
                                std::get<DataType>(right_type),
                                [operation_type_, &res, &operation](auto right_tag) {
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
                                                        "POW operator does not support floating point exponents, got "
                                                        "{}",
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
                                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected binary operator"
                                            );
                                        }
                                    } else {
                                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                                "Non-numeric operand provided to binary operation: {}",
                                                binary_operation_with_types_to_string(
                                                        operation.left_->label_,
                                                        left_type_info::TDT::type_descriptor(),
                                                        operation_type_,
                                                        operation.right_->label_,
                                                        right_type_info::TDT::type_descriptor()
                                                )
                                        );
                                    }
                                }
                        );
                    }
            );
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
                    "Invalid comparison {}",
                    binary_operation_with_types_to_string(
                            operation.left_->label_,
                            TypeDescriptor{std::get<DataType>(left_type), Dimension::Dim0},
                            operation_type_,
                            operation.right_->label_,
                            TypeDescriptor{std::get<DataType>(right_type), Dimension::Dim0}
                    )
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
                        "Cannot check membership '{}' of {} ({}) in set of {}",
                        operation_type_,
                        operation.left_->label_,
                        get_user_friendly_type_string(std::get<DataType>(left_type)),
                        get_user_friendly_type_string(std::get<DataType>(right_type))
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
        const ValueSetState condition_value_set_state = condition->value_set_state_;
        const auto& condition_type = condition->type_;
        const ValueSetState right_value_set_state = right->value_set_state_;
        const auto& right_type = right->type_;
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

} // namespace arcticdb
