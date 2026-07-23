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
#include <memory>
#include <utility>

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

    // Used as a memoization hash key and for debugging
    std::string label_;

    explicit ExpressionNode(Leaf leaf);

    ExpressionNode(
            std::shared_ptr<ExpressionNode> condition, std::shared_ptr<ExpressionNode> left,
            std::shared_ptr<ExpressionNode> right, OperationType op
    );

    ExpressionNode(std::shared_ptr<ExpressionNode> left, std::shared_ptr<ExpressionNode> right, OperationType op);

    ExpressionNode(std::shared_ptr<ExpressionNode> left, OperationType op);

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

    static std::variant<BitSetTag, DataType> leaf_return_type(
            const Leaf& leaf, const ankerl::unordered_dense::map<std::string, DataType>& column_types,
            ValueSetState& value_set_state
    );

    static std::variant<BitSetTag, DataType> child_return_type(
            const ExpressionNode& child, const ankerl::unordered_dense::map<std::string, DataType>& column_types,
            ValueSetState& value_set_state
    );
};

} // namespace arcticdb
