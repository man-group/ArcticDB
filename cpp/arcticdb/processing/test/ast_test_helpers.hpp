/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace arcticdb {

// Factories for building expression trees directly as node pointers in C++ tests.
using ExpressionChild = std::shared_ptr<ExpressionNode>;

inline ExpressionChild col(std::string_view name) {
    return std::make_shared<ExpressionNode>(ColumnName(std::string(name)));
}

inline ExpressionChild val(std::shared_ptr<Value> value) { return std::make_shared<ExpressionNode>(std::move(value)); }

template<typename T>
inline ExpressionChild val(T value, DataType data_type) {
    return std::make_shared<ExpressionNode>(std::make_shared<Value>(value, data_type));
}

inline ExpressionChild vset(std::shared_ptr<ValueSet> value_set) {
    return std::make_shared<ExpressionNode>(std::move(value_set));
}

inline ExpressionChild node(ExpressionChild left, OperationType op) {
    return std::make_shared<ExpressionNode>(std::move(left), op);
}

inline ExpressionChild node(ExpressionChild left, ExpressionChild right, OperationType op) {
    return std::make_shared<ExpressionNode>(std::move(left), std::move(right), op);
}

inline ExpressionChild node(ExpressionChild condition, ExpressionChild left, ExpressionChild right, OperationType op) {
    return std::make_shared<ExpressionNode>(std::move(condition), std::move(left), std::move(right), op);
}

} // namespace arcticdb
