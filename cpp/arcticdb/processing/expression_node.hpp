/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/string_wrapping_value.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {

struct ColumnNameTag{};
using ColumnName = util::StringWrappingValue<ColumnNameTag>;

struct ValueNameTag{};
using ValueName = util::StringWrappingValue<ValueNameTag>;

struct ValueSetNameTag{};
using ValueSetName = util::StringWrappingValue<ValueSetNameTag>;

struct ExpressionNameTag{};
using ExpressionName = util::StringWrappingValue<ExpressionNameTag>;

using VariantNode = std::variant<std::monostate, ColumnName, ValueName, ValueSetName, ExpressionName>;

struct ProcessingUnit;
class Store;

/*
 * Wrapper class combining a column with the string pool containing strings for that column
 */
struct ColumnWithStrings {
    std::shared_ptr<Column> column_;
    const std::shared_ptr<StringPool> string_pool_;

    explicit ColumnWithStrings(std::unique_ptr<Column>&& col) :
        column_(std::move(col)) {
    }

    ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool) :
        column_(std::make_shared<Column>(std::move(col))),
        string_pool_(std::move(string_pool)) {
    }

    ColumnWithStrings(std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool) :
        column_(std::move(column)),
        string_pool_(string_pool) {
    }

    [[nodiscard]] std::optional<std::string_view> string_at_offset(StringPool::offset_t offset, bool strip_fixed_width_trailing_nulls=false) const {
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

    [[nodiscard]] std::optional<size_t> get_fixed_width_string_size() const {
        if (!column_ || !string_pool_)
            return std::nullopt;

        util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
        for(position_t i = 0; i < column_->row_count(); ++i) {
            auto offset = column_->scalar_at<StringPool::offset_t>(i);
            if (offset != std::nullopt) {
                std::string_view raw = string_pool_->get_view(offset.value());
                return raw.size();
            }
        }
        return std::nullopt;
    }
};

struct FullResult {};

struct EmptyResult {};

using VariantData = std::variant<FullResult, EmptyResult, std::shared_ptr<Value>, std::shared_ptr<ValueSet>, ColumnWithStrings, std::shared_ptr<util::BitSet>>;

/*
 * Basic AST node.
 */
struct ExpressionNode {
    VariantNode left_;
    VariantNode right_;
    OperationType operation_type_;

    ExpressionNode(VariantNode left, VariantNode right, OperationType op);

    ExpressionNode(VariantNode left, OperationType op);

    VariantData compute(ProcessingUnit& seg, const std::shared_ptr<Store>& store) const;
};

} //namespace arcticdb
