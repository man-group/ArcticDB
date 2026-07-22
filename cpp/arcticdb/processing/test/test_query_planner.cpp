/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/query_planner.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>
#include <arcticdb/util/error_code.hpp>

using namespace arcticdb;

namespace {

std::shared_ptr<ExpressionContext> make_filter_context(bool dynamic_schema) {
    auto context = std::make_shared<ExpressionContext>();
    context->root_ = node(col("a"), val(int64_t{0}, DataType::INT64), OperationType::GT);
    context->dynamic_schema_ = dynamic_schema;
    return context;
}

} // namespace

TEST(QueryPlanner, AndFilterExpressionContextsInheritsDynamicSchemaTrue) {
    std::vector<std::shared_ptr<ExpressionContext>> contexts{make_filter_context(true), make_filter_context(true)};
    auto merged = and_filter_expression_contexts(contexts);
    ASSERT_TRUE(merged.root_ && merged.root_->is_operation());
    ASSERT_TRUE(merged.dynamic_schema_);
}

TEST(QueryPlanner, AndFilterExpressionContextsInheritsDynamicSchemaFalse) {
    std::vector<std::shared_ptr<ExpressionContext>> contexts{make_filter_context(false), make_filter_context(false)};
    auto merged = and_filter_expression_contexts(contexts);
    ASSERT_FALSE(merged.dynamic_schema_);
}

TEST(QueryPlanner, AndFilterExpressionContextsSingleInheritsDynamicSchema) {
    std::vector<std::shared_ptr<ExpressionContext>> contexts{make_filter_context(true)};
    auto merged = and_filter_expression_contexts(contexts);
    ASSERT_TRUE(merged.dynamic_schema_);
}

TEST(QueryPlanner, AndFilterExpressionContextsRejectsInconsistentDynamicSchema) {
    std::vector<std::shared_ptr<ExpressionContext>> contexts{make_filter_context(true), make_filter_context(false)};
    ASSERT_THROW(and_filter_expression_contexts(contexts), InternalException);
}
