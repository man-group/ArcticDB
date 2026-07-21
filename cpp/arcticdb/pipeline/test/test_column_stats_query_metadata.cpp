/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/test/ast_test_helpers.hpp>

using namespace arcticdb;

namespace {

std::shared_ptr<FilterClause> make_filter(const std::string& column) {
    ExpressionContext ec;
    ec.root_ = node(col(column), OperationType::IDENTITY);
    return std::make_shared<FilterClause>(std::unordered_set<std::string>{column}, ec, std::nullopt);
}

std::shared_ptr<RowRangeClause> make_head(int64_t n) {
    return std::make_shared<RowRangeClause>(RowRangeClause::RowRangeType::HEAD, n);
}

std::shared_ptr<DateRangeClause> make_date_range(timestamp start, timestamp end) {
    return std::make_shared<DateRangeClause>(start, end);
}

template<typename T>
std::shared_ptr<Clause> wrap(const std::shared_ptr<T>& clause) {
    return std::make_shared<Clause>(*clause);
}

} // namespace

TEST(ColumnStatsQueryMetadata, FilterThenHeadThenFilterOnlyUsesLeadingFilter) {
    std::vector<std::shared_ptr<Clause>> clauses{wrap(make_filter("a")), wrap(make_head(2)), wrap(make_filter("b"))};

    ColumnStatsQueryMetadata metadata(clauses);

    ASSERT_TRUE(metadata.filter_expression.has_value());
    EXPECT_EQ(metadata.columns_of_interest, (std::unordered_set<std::string>{"a"}));
}

TEST(ColumnStatsQueryMetadata, TwoDateRangeClausesViolatesMergedInvariant) {
    // plan_query always merges consecutive DateRangeClauses into one before ColumnStatsQueryMetadata is
    // constructed, so this shape is unreachable via the planner. Build it by hand to assert the guard fires.
    std::vector<std::shared_ptr<Clause>> clauses{wrap(make_date_range(0, 100)), wrap(make_date_range(50, 200))};

    EXPECT_THROW(ColumnStatsQueryMetadata metadata(clauses), InternalException);
}
