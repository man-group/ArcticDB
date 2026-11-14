/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/string_wrapping_value.hpp>
#include <arcticdb/util/regex_filter.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/constructors.hpp>
#include <ankerl/unordered_dense.h>
#include <array>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace arcticdb {

struct ColumnNameTag {};
using ColumnName = util::StringWrappingValue<ColumnNameTag>;

struct ProcessingUnit;
class Column;
class StringPool;

/*
 * Wrapper class combining a column with the string pool containing strings for that column
 */
struct ColumnWithStrings {
    std::shared_ptr<Column> column_;
    const std::shared_ptr<StringPool> string_pool_;
    std::string column_name_;

    ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name);

    ColumnWithStrings(
            std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
    );

    ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name);

    ColumnWithStrings(
            std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool,
            std::string_view col_name = ""
    );

    [[nodiscard]] std::optional<std::string_view> string_at_offset(
            entity::position_t offset, bool strip_fixed_width_trailing_nulls = false
    ) const;

    [[nodiscard]] std::optional<size_t> get_fixed_width_string_size() const;
};

struct FullResult {};

struct EmptyResult {};

using VariantData = std::variant<
        FullResult, EmptyResult, std::shared_ptr<Value>, std::shared_ptr<ValueSet>, ColumnWithStrings, util::BitSet,
        std::shared_ptr<util::RegexGeneric>>;

// Used to represent that an ExpressionNode returns a bitset
struct BitSetTag {};

/*
 * Basic AST node.
 */
struct ExpressionNode {
    using Leaf = std::variant<
            ColumnName, std::shared_ptr<Value>, std::shared_ptr<ValueSet>, std::shared_ptr<util::RegexGeneric>>;

    struct Operation {
        Operation(
                OperationType operation_type, std::shared_ptr<ExpressionNode> left,
                std::shared_ptr<ExpressionNode> right = nullptr, std::shared_ptr<ExpressionNode> condition = nullptr
        ) :
            operation_type_(operation_type),
            left_(std::move(left)),
            right_(std::move(right)),
            condition_(std::move(condition)) {}

        OperationType operation_type_;
        std::shared_ptr<ExpressionNode> left_;
        std::shared_ptr<ExpressionNode> right_;
        std::shared_ptr<ExpressionNode> condition_;
    };

    std::variant<Leaf, Operation> kind_;

    // Used as a memoization hash key and for debugging. Large expressions are truncated, ending in their hash_.
    std::string label_;

    // Structural hash, consistent with structural equality of the nodes
    size_t hash_{0};

    explicit ExpressionNode(Leaf leaf);

    ExpressionNode(
            std::shared_ptr<ExpressionNode> condition, std::shared_ptr<ExpressionNode> left,
            std::shared_ptr<ExpressionNode> right, OperationType op
    );

    ExpressionNode(std::shared_ptr<ExpressionNode> left, std::shared_ptr<ExpressionNode> right, OperationType op);

    ExpressionNode(std::shared_ptr<ExpressionNode> left, OperationType op);

    // Iterative, so that destroying a deeply nested expression does not overflow the stack
    ~ExpressionNode();

    ARCTICDB_MOVE_COPY_DEFAULT(ExpressionNode)

    [[nodiscard]] bool is_operation() const { return std::holds_alternative<Operation>(kind_); }

    [[nodiscard]] bool is_value() const {
        return std::holds_alternative<Leaf>(kind_) &&
               std::holds_alternative<std::shared_ptr<Value>>(std::get<Leaf>(kind_));
    }

    VariantData compute(ProcessingUnit& seg) const;

    std::variant<BitSetTag, DataType> compute(const ankerl::unordered_dense::map<std::string, DataType>& column_types
    ) const;

  private:
    enum class ValueSetState { NOT_A_SET, EMPTY_SET, NON_EMPTY_SET };

    struct ReturnType {
        std::variant<BitSetTag, DataType> type_;
        ValueSetState value_set_state_{ValueSetState::NOT_A_SET};
    };

    static std::variant<BitSetTag, DataType> operation_return_type(
            const Operation& operation, const ReturnType& left, const ReturnType* right, const ReturnType* condition
    );

    static std::variant<BitSetTag, DataType> leaf_return_type(
            const Leaf& leaf, const ankerl::unordered_dense::map<std::string, DataType>& column_types,
            ValueSetState& value_set_state
    );
};

namespace expression_detail {

using PendingUses = ankerl::unordered_dense::map<const ExpressionNode*, size_t>;
using EvaluationStack = std::vector<std::pair<const ExpressionNode*, bool>>;

// Right, condition, left: pushed onto a stack in this order, operands are evaluated left, condition, right
inline std::array<const ExpressionNode*, 3> operands_in_push_order(const ExpressionNode::Operation& op) {
    return {op.right_.get(), op.condition_.get(), op.left_.get()};
}

// Number of parent evaluations that need each node's result, counting a node used twice by a parent twice
PendingUses count_uses(const ExpressionNode& root);

// Which results ExpressionNode::compute keeps for structurally equal copies of a sub-expression: the group of equal
// nodes each memoized node belongs to, and for each group the number of its nodes that will reuse the result.
struct MemoPlan {
    // The expression planned, kept alive so that the plan's pointers stay valid and are not mistaken for another's
    std::shared_ptr<const ExpressionNode> root;
    ankerl::unordered_dense::map<const ExpressionNode*, const ExpressionNode*> group_of;
    ankerl::unordered_dense::map<const ExpressionNode*, size_t> reuses;
};

MemoPlan plan_memoization(const ExpressionNode& root);

// Frees each operand's result once no other parent still needs it
template<typename Results>
void release_operands(const ExpressionNode::Operation& op, PendingUses& pending_uses, Results& results) {
    for (const ExpressionNode* child : operands_in_push_order(op)) {
        if (child && --pending_uses.at(child) == 0) {
            results.erase(child);
        }
    }
}

template<typename Results>
void push_unevaluated_operands(const ExpressionNode::Operation& op, const Results& results, EvaluationStack& stack) {
    for (const ExpressionNode* child : operands_in_push_order(op)) {
        if (child && !results.contains(child)) {
            stack.emplace_back(child, false);
        }
    }
}

} // namespace expression_detail

/*
 * Evaluates the expression rooted at root bottom-up, without recursion, so the depth of the expression is not limited
 * by the stack. Each distinct node is evaluated once, so sub-expressions shared between several parents cost the same
 * as in a tree. A result is freed once every parent has used it, except that a node given a result by before_operation
 * does not use its operands, so results they share with other parents are kept until the evaluation ends. Operands
 * are evaluated left, condition, right.
 *  - on_leaf(const ExpressionNode::Leaf&) -> Result
 *  - before_operation(const ExpressionNode&, const ExpressionNode::Operation&) -> std::optional<Result>: called when
 *    a node is reached, before its operands. A result skips them, and it may also raise, before they are evaluated.
 *  - on_operation(const ExpressionNode&, const ExpressionNode::Operation&, operand) -> Result, where operand(child)
 *    returns the result of an operand as a const reference
 */
template<typename Result, typename OnLeaf, typename BeforeOperation, typename OnOperation>
Result evaluate_bottom_up(
        const ExpressionNode& root, const OnLeaf& on_leaf, const BeforeOperation& before_operation,
        const OnOperation& on_operation
) {
    auto pending_uses = expression_detail::count_uses(root);
    // Not ankerl: erasing from it needs move-assignment, which results do not have
    std::unordered_map<const ExpressionNode*, Result> results;
    const auto operand = [&results](const std::shared_ptr<ExpressionNode>& child) -> const Result& {
        return results.at(child.get());
    };
    // Each entry is a node and whether its operands have been evaluated
    expression_detail::EvaluationStack stack{{&root, false}};
    while (!stack.empty()) {
        const auto [node, operands_ready] = stack.back();
        const auto* op = std::get_if<ExpressionNode::Operation>(&node->kind_);
        if (results.contains(node)) {
            stack.pop_back();
        } else if (!op) {
            results.emplace(node, on_leaf(std::get<ExpressionNode::Leaf>(node->kind_)));
            stack.pop_back();
        } else if (operands_ready) {
            results.emplace(node, on_operation(*node, *op, operand));
            stack.pop_back();
            expression_detail::release_operands(*op, pending_uses, results);
        } else if (std::optional<Result> known = before_operation(*node, *op)) {
            results.emplace(node, std::move(*known));
            stack.pop_back();
        } else {
            stack.back().second = true;
            expression_detail::push_unevaluated_operands(*op, results, stack);
        }
    }
    return std::move(results.at(&root));
}

} // namespace arcticdb
