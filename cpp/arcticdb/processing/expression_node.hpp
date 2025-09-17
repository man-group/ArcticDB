/* Copyright 2023 Man Group Operations Limited
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

struct ExpressionContext;

struct ColumnNameTag {};
using ColumnName = util::StringWrappingValue<ColumnNameTag>;

struct ValueNameTag {};
using ValueName = util::StringWrappingValue<ValueNameTag>;

struct ValueSetNameTag {};
using ValueSetName = util::StringWrappingValue<ValueSetNameTag>;

struct ExpressionNameTag {};
using ExpressionName = util::StringWrappingValue<ExpressionNameTag>;

struct RegexNameTag {};
using RegexName = util::StringWrappingValue<RegexNameTag>;

using VariantNode = std::variant<std::monostate, ColumnName, ValueName, ValueSetName, ExpressionName, RegexName>;

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

    ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name) :
        column_(std::move(col)),
        column_name_(col_name) {}

    ColumnWithStrings(
            std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
    ) :
        column_(std::move(column)),
        string_pool_(std::move(string_pool)),
        column_name_(col_name) {}

    ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name) :
        column_(std::make_shared<Column>(std::move(col))),
        string_pool_(std::move(string_pool)),
        column_name_(col_name) {}

    ColumnWithStrings(
            std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool, std::string_view col_name
    ) :
        column_(std::move(column)),
        string_pool_(string_pool),
        column_name_(col_name) {}

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
    VariantNode condition_;
    VariantNode left_;
    VariantNode right_;
    OperationType operation_type_;

    ExpressionNode(VariantNode condition, VariantNode left, VariantNode right, OperationType op);

    ExpressionNode(VariantNode left, VariantNode right, OperationType op);

    ExpressionNode(VariantNode left, OperationType op);

    VariantData compute(ProcessingUnit& seg) const;

    std::variant<BitSetTag, DataType> compute(
            const ExpressionContext& expression_context,
            const ankerl::unordered_dense::map<std::string, DataType>& column_types
    ) const;

  private:
    enum class ValueSetState { NOT_A_SET, EMPTY_SET, NON_EMPTY_SET };

    std::variant<BitSetTag, DataType> child_return_type(
            const VariantNode& child, const ExpressionContext& expression_context,
            const ankerl::unordered_dense::map<std::string, DataType>& column_types, ValueSetState& value_set_state
    ) const;
};

} // namespace arcticdb
