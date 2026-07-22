/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/query_planner.hpp>

#include <algorithm>
#include <string>
#include <type_traits>
#include <unordered_set>

namespace arcticdb {

namespace {

bool is_type(const std::shared_ptr<Clause>& clause, const std::type_info& type) {
    return folly::poly_type(*clause) == type;
}

bool is_filter(const std::shared_ptr<Clause>& clause) { return is_type(clause, typeid(FilterClause)); }

bool is_date_range(const std::shared_ptr<Clause>& clause) { return is_type(clause, typeid(DateRangeClause)); }

bool date_range_can_move_to_left_of(const std::shared_ptr<Clause>& clause) {
    return is_filter(clause) || is_type(clause, typeid(ProjectClause));
}

// Move each DateRangeClause as far left as it can go and merge any that end up adjacent into a single
// DateRangeClause, so that filters that were only separated by a date range end up next to
// each other (and can then be merged by merge_consecutive_filter_clauses), eg
// [F1, F2, DR1, F3, F4, DR2] -> [DR_12, F1, F2, F3, F4]
std::vector<std::shared_ptr<Clause>> move_and_merge_date_ranges_left(std::vector<std::shared_ptr<Clause>>&& clauses) {
    std::vector<std::shared_ptr<Clause>> result;
    result.reserve(clauses.size());
    // The date ranges merged so far for the run currently being built, and the filters/projects seen since the
    // last one, both still pending output until the run ends (so the merged date range can be emitted first).
    std::shared_ptr<Clause> merged_date_range;
    std::vector<std::shared_ptr<Clause>> run;
    auto flush_run = [&]() {
        if (merged_date_range) {
            result.emplace_back(std::move(merged_date_range));
        }
        for (auto& clause : run) {
            result.push_back(std::move(clause));
        }
        run.clear();
    };
    for (auto& clause : clauses) {
        if (is_date_range(clause)) {
            if (merged_date_range) {
                const auto& existing = folly::poly_cast<DateRangeClause>(*merged_date_range);
                const auto& incoming = folly::poly_cast<DateRangeClause>(*clause);
                merged_date_range = std::make_shared<Clause>(DateRangeClause(
                        std::max(existing.start_, incoming.start_), std::min(existing.end_, incoming.end_)
                ));
            } else {
                merged_date_range = std::move(clause);
            }
        } else {
            const bool can_move = date_range_can_move_to_left_of(clause);
            run.push_back(std::move(clause));
            if (!can_move) {
                flush_run();
            }
        }
    }
    flush_run();
    return result;
}

std::shared_ptr<Clause> merge_filter_run(const std::vector<std::shared_ptr<Clause>>& filters) {
    if (filters.size() == 1) {
        return filters.front();
    }
    std::vector<std::shared_ptr<ExpressionContext>> expression_contexts;
    std::unordered_set<std::string> input_columns;
    bool any_memory = false;
    for (const auto& clause : filters) {
        const auto& filter = folly::poly_cast<FilterClause>(*clause);
        expression_contexts.push_back(filter.expression_context_);
        input_columns.insert(filter.clause_info_.input_columns_.begin(), filter.clause_info_.input_columns_.end());
        any_memory = any_memory || filter.optimisation_ == PipelineOptimisation::MEMORY;
    }
    // Respect the (opt-in) memory request if any clause in the run asked for it (speed is the default if the user
    // doesn't specify).
    auto optimisation = any_memory ? PipelineOptimisation::MEMORY : PipelineOptimisation::SPEED;
    return std::make_shared<Clause>(
            FilterClause(std::move(input_columns), and_filter_expression_contexts(expression_contexts), optimisation)
    );
}

std::vector<std::shared_ptr<Clause>> merge_consecutive_filter_clauses(std::vector<std::shared_ptr<Clause>>&& clauses) {
    // Merge runs of consecutive filters together so [f1, f2, p1, f3, f4] becomes [F_12, p1, F_34].
    // A projection between two filters stops them from being merged together (the second filter might reference the
    // projected column, so it cannot be merged with the first).
    std::vector<std::shared_ptr<Clause>> result;
    std::vector<std::shared_ptr<Clause>> run;
    auto flush_run = [&]() {
        if (!run.empty()) {
            result.emplace_back(merge_filter_run(run));
        }
        run.clear();
    };
    for (auto& clause : clauses) {
        if (is_filter(clause)) {
            run.push_back(std::move(clause));
        } else {
            flush_run();
            result.push_back(std::move(clause));
        }
    }
    flush_run();
    return result;
}

} // namespace

std::vector<std::shared_ptr<Clause>> plan_query(std::vector<std::shared_ptr<Clause>>&& clauses) {
    clauses = move_and_merge_date_ranges_left(std::move(clauses));
    if (clauses.size() >= 2 && is_date_range(clauses[0])) {
        const auto& date_range_clause = folly::poly_cast<DateRangeClause>(*clauses[0]);
        const auto date_range_start = date_range_clause.start_;
        const auto date_range_end = date_range_clause.end_;
        auto& following = *clauses[1];
        if (is_type(clauses[1], typeid(ResampleClause<ResampleBoundary::LEFT>))) {
            folly::poly_cast<ResampleClause<ResampleBoundary::LEFT>>(following).set_date_range(
                    date_range_start, date_range_end
            );
            clauses.erase(clauses.cbegin());
        } else if (is_type(clauses[1], typeid(ResampleClause<ResampleBoundary::RIGHT>))) {
            folly::poly_cast<ResampleClause<ResampleBoundary::RIGHT>>(following).set_date_range(
                    date_range_start, date_range_end
            );
            clauses.erase(clauses.cbegin());
        }
    }
    return merge_consecutive_filter_clauses(std::move(clauses));
}

/**
 * Create a single expression context by AND-ing together the supplied expression contexts, which should all come from
 * FilterClause objects (and hence have an ExpressionNode at their root).
 */
ExpressionContext and_filter_expression_contexts(
        const std::vector<std::shared_ptr<ExpressionContext>>& expression_contexts
) {
    util::check(!expression_contexts.empty(), "Expression context cannot be empty");
    std::shared_ptr<ExpressionNode> root;
    const bool dynamic_schema = expression_contexts.front()->dynamic_schema_;
    for (const auto& expression_context : expression_contexts) {
        util::check(
                expression_context->root_ && expression_context->root_->is_operation(),
                "Only expect to be called with filter expressions"
        );
        util::check(
                expression_context->dynamic_schema_ == dynamic_schema,
                "Cannot AND-together filter expressions with differing dynamic_schema_"
        );
        if (root) {
            root = std::make_shared<ExpressionNode>(root, expression_context->root_, OperationType::AND);
        } else {
            root = expression_context->root_;
        }
    }

    ExpressionContext res;
    res.root_ = std::move(root);
    res.dynamic_schema_ = dynamic_schema;
    return res;
}

} // namespace arcticdb
