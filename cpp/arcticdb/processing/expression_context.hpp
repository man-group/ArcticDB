/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <utility>

#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

/*
 * An expression tree (an AST).
 */
struct ExpressionContext {
    ExpressionContext() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(ExpressionContext)

    std::shared_ptr<ExpressionNode> root_;
    bool dynamic_schema_{false};
    // Which results to memoize, the same for every processing unit, so planned once by the clause. It names the root
    // it was planned for, which ExpressionNode::compute checks.
    std::shared_ptr<const expression_detail::MemoPlan> memo_plan_;

    void plan_memoization() {
        util::check(static_cast<bool>(root_), "Cannot plan memoization without an expression");
        auto plan = expression_detail::plan_memoization(*root_);
        plan.root = root_;
        memo_plan_ = std::make_shared<const expression_detail::MemoPlan>(std::move(plan));
    }
};

} // namespace arcticdb
