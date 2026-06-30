/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/processing/expression_node.hpp>
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
};

} // namespace arcticdb
