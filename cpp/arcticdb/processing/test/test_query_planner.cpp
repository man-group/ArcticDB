/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/processing/clause.hpp>
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

std::shared_ptr<FilterClause> make_filter(
        const std::string& column, std::optional<PipelineOptimisation> optimisation = std::nullopt
) {
    ExpressionContext ec;
    ec.root_ = node(col(column), OperationType::IDENTITY);
    return std::make_shared<FilterClause>(std::unordered_set<std::string>{column}, ec, optimisation);
}

std::shared_ptr<ProjectClause> make_project(const std::string& column) {
    ExpressionContext ec;
    ec.root_ = node(col(column), OperationType::IDENTITY);
    return std::make_shared<ProjectClause>(std::unordered_set<std::string>{column}, "projected", ec);
}

std::shared_ptr<DateRangeClause> make_date_range(timestamp start = 0, timestamp end = 100) {
    return std::make_shared<DateRangeClause>(start, end);
}

std::shared_ptr<RowRangeClause> make_row_range() {
    return std::make_shared<RowRangeClause>(RowRangeClause::RowRangeType::HEAD, int64_t{10});
}

const FilterClause& as_filter(const ClauseVariant& clause) { return *std::get<std::shared_ptr<FilterClause>>(clause); }

OperationType root_operation(const FilterClause& filter) {
    return std::get<ExpressionNode::Operation>(filter.expression_context_->root_->kind_).operation_type_;
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

TEST(QueryPlanner, MergeThreeFilters) {
    std::vector<ClauseVariant> clauses{make_filter("a"), make_filter("b"), make_filter("c")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    const auto& merged = as_filter(result[0]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(*merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b", "c"}));
}

TEST(QueryPlanner, SingleFilterUnchanged) {
    std::vector<ClauseVariant> clauses{make_filter("a")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    // Not merged, so we keep the original IDENTITY root
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::IDENTITY);
}

TEST(QueryPlanner, DateRangeMovedLeftBetweenFiltersEnablesMerge) {
    std::vector<ClauseVariant> clauses{make_filter("a"), make_date_range(), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<DateRangeClause>>(result[0]));
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[1]));
    const auto& merged = as_filter(result[1]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(*merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
}

TEST(QueryPlanner, ProjectBlocksMergingFilters) {
    std::vector<ClauseVariant> clauses{make_filter("a"), make_project("a"), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[0]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<ProjectClause>>(result[1]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[2]));
}

TEST(QueryPlanner, RowRangeBlocksMergingFilters) {
    // Filters on each side of the RowRange merge within their side, but not across it.
    std::vector<ClauseVariant> clauses{
            make_filter("a"), make_filter("b"), make_row_range(), make_filter("c"), make_filter("d")
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[0]));
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::AND);
    EXPECT_EQ(*as_filter(result[0]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<RowRangeClause>>(result[1]));
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[2]));
    EXPECT_EQ(root_operation(as_filter(result[2])), OperationType::AND);
    EXPECT_EQ(*as_filter(result[2]).clause_info_.input_columns_, (std::unordered_set<std::string>{"c", "d"}));
}

TEST(QueryPlanner, DateRangeNotMovedLeftPastGroupBy) {
    std::vector<ClauseVariant> clauses{make_filter("a"), std::make_shared<GroupByClause>("g"), make_date_range()};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[0]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<GroupByClause>>(result[1]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<DateRangeClause>>(result[2]));
}

TEST(QueryPlanner, MergedOptimisationIsMemoryIfAnyMember) {
    {
        std::vector<ClauseVariant> clauses{
                make_filter("a", PipelineOptimisation::MEMORY), make_filter("b", PipelineOptimisation::SPEED)
        };
        auto result = plan_query(std::move(clauses));
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(as_filter(result[0]).optimisation_, PipelineOptimisation::MEMORY);
    }
    {
        std::vector<ClauseVariant> clauses{
                make_filter("a", PipelineOptimisation::SPEED), make_filter("b", PipelineOptimisation::SPEED)
        };
        auto result = plan_query(std::move(clauses));
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(as_filter(result[0]).optimisation_, PipelineOptimisation::SPEED);
    }
}

TEST(QueryPlanner, DateRangeMovedLeftPastProjectButStopsAtRowRange) {
    // The date range moves left past the Project but not past the RowRange.
    std::vector<ClauseVariant> clauses{make_filter("a"), make_row_range(), make_project("a"), make_date_range()};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 4);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[0]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<RowRangeClause>>(result[1]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<DateRangeClause>>(result[2]));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<ProjectClause>>(result[3]));
}

TEST(QueryPlanner, MultipleDateRangesMovedToFrontInOrder) {
    // Matches the move_date_ranges_left doc example: both date ranges move to the front in order,
    // leaving the four filters next to each other so they merge into one.
    std::vector<ClauseVariant> clauses{
            make_filter("a"),
            make_filter("b"),
            make_date_range(0, 100),
            make_filter("c"),
            make_filter("d"),
            make_date_range(50, 200)
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<DateRangeClause>>(result[0]));
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<DateRangeClause>>(result[1]));
    EXPECT_EQ(std::get<std::shared_ptr<DateRangeClause>>(result[0])->start_, 0);
    EXPECT_EQ(std::get<std::shared_ptr<DateRangeClause>>(result[1])->start_, 50);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[2]));
    const auto& merged = as_filter(result[2]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(*merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b", "c", "d"}));
}

TEST(QueryPlanner, TwoSeparateMergeRunsAroundABarrier) {
    std::vector<ClauseVariant> clauses{
            make_filter("a"), make_filter("b"), make_project("a"), make_filter("c"), make_filter("d")
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[0]));
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::AND);
    EXPECT_EQ(*as_filter(result[0]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<ProjectClause>>(result[1]));
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[2]));
    EXPECT_EQ(root_operation(as_filter(result[2])), OperationType::AND);
    EXPECT_EQ(*as_filter(result[2]).clause_info_.input_columns_, (std::unordered_set<std::string>{"c", "d"}));
}

TEST(QueryPlanner, RunMergedWhenItIsTheFinalClausesAfterABarrier) {
    std::vector<ClauseVariant> clauses{std::make_shared<GroupByClause>("g"), make_filter("a"), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<GroupByClause>>(result[0]));
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FilterClause>>(result[1]));
    EXPECT_EQ(root_operation(as_filter(result[1])), OperationType::AND);
    EXPECT_EQ(*as_filter(result[1]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
}

TEST(QueryPlanner, EmptyAndSingleNonFilter) {
    EXPECT_TRUE(plan_query({}).empty());

    std::vector<ClauseVariant> clauses{std::make_shared<GroupByClause>("g")};
    auto result = plan_query(std::move(clauses));
    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<GroupByClause>>(result[0]));
}
