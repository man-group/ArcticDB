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

template<typename ClauseType>
bool is(const std::shared_ptr<Clause>& clause) {
    return folly::poly_type(*clause) == typeid(ClauseType);
}

template<typename ClauseType>
const ClauseType& as(const std::shared_ptr<Clause>& clause) {
    return folly::poly_cast<ClauseType>(*clause);
}

std::shared_ptr<ExpressionContext> make_filter_context(bool dynamic_schema) {
    auto context = std::make_shared<ExpressionContext>();
    context->root_ = node(col("a"), val(int64_t{0}, DataType::INT64), OperationType::GT);
    context->dynamic_schema_ = dynamic_schema;
    return context;
}

std::shared_ptr<Clause> make_filter(
        const std::string& column, std::optional<PipelineOptimisation> optimisation = std::nullopt
) {
    ExpressionContext ec;
    ec.root_ = node(col(column), OperationType::IDENTITY);
    return std::make_shared<Clause>(FilterClause(std::unordered_set<std::string>{column}, ec, optimisation));
}

std::shared_ptr<Clause> make_project(const std::string& column) {
    ExpressionContext ec;
    ec.root_ = node(col(column), OperationType::IDENTITY);
    return std::make_shared<Clause>(ProjectClause(std::unordered_set<std::string>{column}, "projected", ec));
}

std::shared_ptr<Clause> make_date_range(timestamp start = 0, timestamp end = 100) {
    return std::make_shared<Clause>(DateRangeClause(start, end));
}

std::shared_ptr<Clause> make_row_range() {
    return std::make_shared<Clause>(RowRangeClause(RowRangeClause::RowRangeType::HEAD, int64_t{10}));
}

std::shared_ptr<Clause> make_group(const std::string& column) {
    return std::make_shared<Clause>(GroupByClause(column));
}

std::shared_ptr<Clause> make_resample(ResampleOrigin origin = ResampleOrigin{std::string{"epoch"}}) {
    ResampleClause<ResampleBoundary::LEFT>::BucketGeneratorT generate_bucket_boundaries =
            [](timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, const ResampleOrigin&) {
                return std::vector<timestamp>{};
            };
    return std::make_shared<Clause>(ResampleClause<ResampleBoundary::LEFT>(
            "dummy", ResampleBoundary::LEFT, std::move(generate_bucket_boundaries), timestamp{0}, std::move(origin)
    ));
}

const FilterClause& as_filter(const std::shared_ptr<Clause>& clause) { return as<FilterClause>(clause); }

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
    std::vector<std::shared_ptr<Clause>> clauses{make_filter("a"), make_filter("b"), make_filter("c")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    const auto& merged = as_filter(result[0]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b", "c"}));
}

TEST(QueryPlanner, SingleFilterUnchanged) {
    std::vector<std::shared_ptr<Clause>> clauses{make_filter("a")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    // Not merged, so we keep the original IDENTITY root
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::IDENTITY);
}

TEST(QueryPlanner, DateRangeMovedLeftBetweenFiltersEnablesMerge) {
    std::vector<std::shared_ptr<Clause>> clauses{make_filter("a"), make_date_range(), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(is<DateRangeClause>(result[0]));
    ASSERT_TRUE(is<FilterClause>(result[1]));
    const auto& merged = as_filter(result[1]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
}

TEST(QueryPlanner, ProjectBlocksMergingFilters) {
    std::vector<std::shared_ptr<Clause>> clauses{make_filter("a"), make_project("a"), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    EXPECT_TRUE(is<FilterClause>(result[0]));
    EXPECT_TRUE(is<ProjectClause>(result[1]));
    EXPECT_TRUE(is<FilterClause>(result[2]));
}

TEST(QueryPlanner, RowRangeBlocksMergingFilters) {
    // Filters on each side of the RowRange merge within their side, but not across it.
    std::vector<std::shared_ptr<Clause>> clauses{
            make_filter("a"), make_filter("b"), make_row_range(), make_filter("c"), make_filter("d")
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(is<FilterClause>(result[0]));
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::AND);
    EXPECT_EQ(as_filter(result[0]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
    EXPECT_TRUE(is<RowRangeClause>(result[1]));
    ASSERT_TRUE(is<FilterClause>(result[2]));
    EXPECT_EQ(root_operation(as_filter(result[2])), OperationType::AND);
    EXPECT_EQ(as_filter(result[2]).clause_info_.input_columns_, (std::unordered_set<std::string>{"c", "d"}));
}

TEST(QueryPlanner, DateRangeNotMovedLeftPastGroupBy) {
    std::vector<std::shared_ptr<Clause>> clauses{make_filter("a"), make_group("g"), make_date_range()};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    EXPECT_TRUE(is<FilterClause>(result[0]));
    EXPECT_TRUE(is<GroupByClause>(result[1]));
    EXPECT_TRUE(is<DateRangeClause>(result[2]));
}

TEST(QueryPlanner, MergedOptimisationIsMemoryIfAnyMember) {
    {
        std::vector<std::shared_ptr<Clause>> clauses{
                make_filter("a", PipelineOptimisation::MEMORY), make_filter("b", PipelineOptimisation::SPEED)
        };
        auto result = plan_query(std::move(clauses));
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(as_filter(result[0]).optimisation_, PipelineOptimisation::MEMORY);
    }
    {
        std::vector<std::shared_ptr<Clause>> clauses{
                make_filter("a", PipelineOptimisation::SPEED), make_filter("b", PipelineOptimisation::SPEED)
        };
        auto result = plan_query(std::move(clauses));
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(as_filter(result[0]).optimisation_, PipelineOptimisation::SPEED);
    }
}

TEST(QueryPlanner, DateRangeMovedLeftPastProjectButStopsAtRowRange) {
    // The date range moves left past the Project but not past the RowRange.
    std::vector<std::shared_ptr<Clause>> clauses{
            make_filter("a"), make_row_range(), make_project("a"), make_date_range()
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 4);
    EXPECT_TRUE(is<FilterClause>(result[0]));
    EXPECT_TRUE(is<RowRangeClause>(result[1]));
    EXPECT_TRUE(is<DateRangeClause>(result[2]));
    EXPECT_TRUE(is<ProjectClause>(result[3]));
}

TEST(QueryPlanner, DateRangesMergedAroundFilterAndProject) {
    // Mirrors test_querybuilder_filter_then_date_range_then_project_then_date_range: a filter and a
    // project both sit between the two date ranges, and both are movable, so the ranges slide to the
    // front and merge into one intersected range, leaving the filter and project behind it in order.
    std::vector<std::shared_ptr<Clause>> clauses{
            make_filter("a"), make_date_range(0, 60), make_project("b"), make_date_range(40, 100)
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(is<DateRangeClause>(result[0]));
    EXPECT_EQ(as<DateRangeClause>(result[0]).start_, 40);
    EXPECT_EQ(as<DateRangeClause>(result[0]).end_, 60);
    ASSERT_TRUE(is<FilterClause>(result[1]));
    EXPECT_EQ(root_operation(as_filter(result[1])), OperationType::IDENTITY);
    EXPECT_TRUE(is<ProjectClause>(result[2]));
}

TEST(QueryPlanner, MultipleDateRangesMovedToFrontInOrder) {
    // Matches the move_date_ranges_left doc example: both date ranges move to the front, where they
    // are then merged into one intersected DateRangeClause, leaving the four filters next to each
    // other so they merge into one.
    std::vector<std::shared_ptr<Clause>> clauses{
            make_filter("a"),
            make_filter("b"),
            make_date_range(0, 100),
            make_filter("c"),
            make_filter("d"),
            make_date_range(50, 200)
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    ASSERT_TRUE(is<DateRangeClause>(result[0]));
    EXPECT_EQ(as<DateRangeClause>(result[0]).start_, 50);
    EXPECT_EQ(as<DateRangeClause>(result[0]).end_, 100);
    ASSERT_TRUE(is<FilterClause>(result[1]));
    const auto& merged = as_filter(result[1]);
    EXPECT_EQ(root_operation(merged), OperationType::AND);
    EXPECT_EQ(merged.clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b", "c", "d"}));
}

TEST(QueryPlanner, MergeConsecutiveDateRangesIntersectsBounds) {
    std::vector<std::shared_ptr<Clause>> clauses{make_date_range(0, 100), make_date_range(50, 200)};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(is<DateRangeClause>(result[0]));
    EXPECT_EQ(as<DateRangeClause>(result[0]).start_, 50);
    EXPECT_EQ(as<DateRangeClause>(result[0]).end_, 100);
}

TEST(QueryPlanner, DisjointDateRangesMergeToInvertedRange) {
    // The planner just intersects bounds, it is DateRangeClause::process's job to treat start_ >
    // end_ as an empty result.
    std::vector<std::shared_ptr<Clause>> clauses{make_date_range(0, 10), make_date_range(20, 30)};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(is<DateRangeClause>(result[0]));
    EXPECT_EQ(as<DateRangeClause>(result[0]).start_, 20);
    EXPECT_EQ(as<DateRangeClause>(result[0]).end_, 10);
}

TEST(QueryPlanner, MultipleLeadingDateRangesFoldIntoResample) {
    auto resample = make_resample();
    std::vector<std::shared_ptr<Clause>> clauses{make_date_range(0, 100), make_date_range(50, 200), resample};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(is<ResampleClause<ResampleBoundary::LEFT>>(result[0]));
    const auto& folded = as<ResampleClause<ResampleBoundary::LEFT>>(result[0]);
    ASSERT_TRUE(folded.date_range_.has_value());
    EXPECT_EQ(folded.date_range_->first, 50);
    EXPECT_EQ(folded.date_range_->second, 100);
}

TEST(QueryPlanner, MultipleDisjointLeadingDateRangesFoldIntoResample) {
    auto resample = make_resample();
    std::vector<std::shared_ptr<Clause>> clauses{make_date_range(0, 10), make_date_range(20, 30), resample};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(is<ResampleClause<ResampleBoundary::LEFT>>(result[0]));
    const auto& folded = as<ResampleClause<ResampleBoundary::LEFT>>(result[0]);
    ASSERT_TRUE(folded.date_range_.has_value());
    EXPECT_EQ(folded.date_range_->first, 20);
    EXPECT_EQ(folded.date_range_->second, 10);
}

TEST(QueryPlanner, DateRangesBeforeAndAfterResample) {
    std::vector<std::shared_ptr<Clause>> clauses{
            make_date_range(0, 100),
            make_date_range(50, 200),
            make_resample(),
            make_date_range(60, 120),
            make_date_range(70, 300)
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    ASSERT_TRUE(is<ResampleClause<ResampleBoundary::LEFT>>(result[0]));
    const auto& folded = as<ResampleClause<ResampleBoundary::LEFT>>(result[0]);
    ASSERT_TRUE(folded.date_range_.has_value());
    EXPECT_EQ(folded.date_range_->first, 50);
    EXPECT_EQ(folded.date_range_->second, 100);
    ASSERT_TRUE(is<DateRangeClause>(result[1]));
    EXPECT_EQ(as<DateRangeClause>(result[1]).start_, 70);
    EXPECT_EQ(as<DateRangeClause>(result[1]).end_, 120);
}

TEST(QueryPlanner, TwoSeparateMergeRunsAroundABarrier) {
    std::vector<std::shared_ptr<Clause>> clauses{
            make_filter("a"), make_filter("b"), make_project("a"), make_filter("c"), make_filter("d")
    };
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 3);
    ASSERT_TRUE(is<FilterClause>(result[0]));
    EXPECT_EQ(root_operation(as_filter(result[0])), OperationType::AND);
    EXPECT_EQ(as_filter(result[0]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
    EXPECT_TRUE(is<ProjectClause>(result[1]));
    ASSERT_TRUE(is<FilterClause>(result[2]));
    EXPECT_EQ(root_operation(as_filter(result[2])), OperationType::AND);
    EXPECT_EQ(as_filter(result[2]).clause_info_.input_columns_, (std::unordered_set<std::string>{"c", "d"}));
}

TEST(QueryPlanner, RunMergedWhenItIsTheFinalClausesAfterABarrier) {
    std::vector<std::shared_ptr<Clause>> clauses{make_group("g"), make_filter("a"), make_filter("b")};
    auto result = plan_query(std::move(clauses));

    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(is<GroupByClause>(result[0]));
    ASSERT_TRUE(is<FilterClause>(result[1]));
    EXPECT_EQ(root_operation(as_filter(result[1])), OperationType::AND);
    EXPECT_EQ(as_filter(result[1]).clause_info_.input_columns_, (std::unordered_set<std::string>{"a", "b"}));
}

TEST(QueryPlanner, EmptyAndSingleNonFilter) {
    EXPECT_TRUE(plan_query({}).empty());

    std::vector<std::shared_ptr<Clause>> clauses{make_group("g")};
    auto result = plan_query(std::move(clauses));
    ASSERT_EQ(result.size(), 1);
    EXPECT_TRUE(is<GroupByClause>(result[0]));
}
