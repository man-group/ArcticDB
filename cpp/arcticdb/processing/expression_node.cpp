/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/processing/operation_dispatch_unary.hpp>

namespace arcticdb {

ExpressionNode::ExpressionNode(VariantNode left, VariantNode right, OperationType op) :
    left_(std::move(left)),
    right_(std::move(right)),
    operation_type_(op) {
    util::check(is_binary_operation(op), "Left and right expressions supplied to non-binary operator");
}

ExpressionNode::ExpressionNode(VariantNode left, OperationType op) :
    left_(std::move(left)),
    operation_type_(op) {
    util::check(!is_binary_operation(op), "Binary expression expects both left and right children");
}

VariantData ExpressionNode::compute(ProcessingUnit& seg, const std::shared_ptr<Store>& store) const {
    if (is_binary_operation(operation_type_)) {
        return dispatch_binary(seg.get(left_, store), seg.get(right_, store), operation_type_);
    } else {
        return dispatch_unary(seg.get(left_, store), operation_type_);
    }
}

} //namespace arcticdb