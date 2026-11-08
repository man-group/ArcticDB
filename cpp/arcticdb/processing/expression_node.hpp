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

/*
 * Evaluates the expression rooted at root bottom-up, without recursion, so the depth of the expression is not limited
 * by the stack. Each distinct node is evaluated once, so sub-expressions shared between several parents cost the same
 * as in a tree, and a result is freed once its last parent has used it. Operands are evaluated left, condition, right.
 *  - on_leaf(const ExpressionNode::Leaf&) -> Result
 *  - before_operation(const ExpressionNode&) -> std::optional<Result>: a result for the node that skips its operands
 *  - on_operation(const ExpressionNode&, const ExpressionNode::Operation&, operand) -> Result, where operand(child)
 *    returns the result of an operand as a const reference
 */
template<typename Result, typename OnLeaf, typename BeforeOperation, typename OnOperation>
Result evaluate_bottom_up(
        const ExpressionNode& root, OnLeaf&& on_leaf, BeforeOperation&& before_operation, OnOperation&& on_operation
) {
    const auto operands_of = [](const ExpressionNode::Operation& op) {
        return std::array<const ExpressionNode*, 3>{op.right_.get(), op.condition_.get(), op.left_.get()};
    };
    // Number of parent evaluations that still need each node's result, counting a node used twice by a parent twice
    ankerl::unordered_dense::map<const ExpressionNode*, size_t> pending_uses{{&root, 0}};
    std::vector<const ExpressionNode*> to_count{&root};
    while (!to_count.empty()) {
        const auto* node = to_count.back();
        to_count.pop_back();
        if (const auto* op = std::get_if<ExpressionNode::Operation>(&node->kind_)) {
            for (const ExpressionNode* child : operands_of(*op)) {
                if (child) {
                    const auto [it, first_use] = pending_uses.try_emplace(child, 0);
                    ++it->second;
                    if (first_use) {
                        to_count.push_back(child);
                    }
                }
            }
        }
    }
    // Not ankerl: erasing from it needs move-assignment, which results do not have
    std::unordered_map<const ExpressionNode*, Result> results;
    const auto operand = [&results](const std::shared_ptr<ExpressionNode>& child) -> const Result& {
        return results.at(child.get());
    };
    std::vector<std::pair<const ExpressionNode*, bool>> stack{{&root, false}};
    while (!stack.empty()) {
        const auto [node, operands_ready] = stack.back();
        if (results.contains(node)) {
            stack.pop_back();
        } else if (const auto* leaf = std::get_if<ExpressionNode::Leaf>(&node->kind_)) {
            results.emplace(node, on_leaf(*leaf));
            stack.pop_back();
        } else {
            const auto& op = std::get<ExpressionNode::Operation>(node->kind_);
            if (operands_ready) {
                results.emplace(node, on_operation(*node, op, operand));
                stack.pop_back();
                for (const ExpressionNode* child : operands_of(op)) {
                    if (child && --pending_uses.at(child) == 0) {
                        results.erase(child);
                    }
                }
            } else if (std::optional<Result> known = before_operation(*node)) {
                results.emplace(node, std::move(*known));
                stack.pop_back();
            } else {
                stack.back().second = true;
                for (const ExpressionNode* child : operands_of(op)) {
                    if (child && !results.contains(child)) {
                        stack.emplace_back(child, false);
                    }
                }
            }
        }
    }
    return std::move(results.at(&root));
}

} // namespace arcticdb
