/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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

ExpressionNode::ExpressionNode(VariantNode left, VariantNode right, OperationType op) :
    left_(std::move(left)),
    right_(std::move(right)),
    operation_type_(op) {
    util::check(is_binary_operation(op), "Left and right expressions supplied to non-binary operator");
}

ExpressionNode::ExpressionNode(VariantNode left, OperationType op) : left_(std::move(left)), operation_type_(op) {
    util::check(!is_binary_operation(op), "Binary expression expects both left and right children");
}

VariantData ExpressionNode::compute(ProcessingUnit& seg) const {
    if (is_binary_operation(operation_type_)) {
        return dispatch_binary(seg.get(left_), seg.get(right_), operation_type_);
    } else {
        return dispatch_unary(seg.get(left_), operation_type_);
    }
}

} // namespace arcticdb
