/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/string_wrapping_value.hpp>
#include <utility>

namespace arcticdb {

struct ColumnNameTag {};
using ColumnName = util::StringWrappingValue<ColumnNameTag>;

struct ValueNameTag {};
using ValueName = util::StringWrappingValue<ValueNameTag>;

struct ValueSetNameTag {};
using ValueSetName = util::StringWrappingValue<ValueSetNameTag>;

struct ExpressionNameTag {};
using ExpressionName = util::StringWrappingValue<ExpressionNameTag>;

using VariantNode =
    std::variant<std::monostate, ColumnName, ValueName, ValueSetName, ExpressionName>;

struct ProcessingUnit;
class Column;
class StringPool;

/*
 * Wrapper class combining a column with the string pool containing strings for that
 * column
 */
struct ColumnWithStrings {
  std::shared_ptr<Column> column_;
  const std::shared_ptr<StringPool> string_pool_;
  std::string column_name_;

  ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name)
      : column_(std::move(col)), column_name_(col_name) {}

  ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool,
                    std::string_view col_name)
      : column_(std::make_shared<Column>(std::move(col))),
        string_pool_(std::move(string_pool)), column_name_(col_name) {}

  ColumnWithStrings(std::shared_ptr<Column> column,
                    const std::shared_ptr<StringPool>& string_pool,
                    std::string_view col_name)
      : column_(std::move(column)), string_pool_(string_pool), column_name_(col_name) {}

  [[nodiscard]] std::optional<std::string_view>
  string_at_offset(entity::position_t offset,
                   bool strip_fixed_width_trailing_nulls = false) const;

  [[nodiscard]] std::optional<size_t> get_fixed_width_string_size() const;
};

struct FullResult {};

struct EmptyResult {};

using VariantData =
    std::variant<FullResult, EmptyResult, std::shared_ptr<Value>,
                 std::shared_ptr<ValueSet>, ColumnWithStrings, util::BitSet>;

/*
 * Basic AST node.
 */
struct ExpressionNode {
  VariantNode left_;
  VariantNode right_;
  OperationType operation_type_;

  ExpressionNode(VariantNode left, VariantNode right, OperationType op);

  ExpressionNode(VariantNode left, OperationType op);

  VariantData compute(ProcessingUnit& seg) const;
};

} // namespace arcticdb
