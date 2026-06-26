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
#include <unordered_set>

namespace arcticdb {

namespace {

bool is_filter(const ClauseVariant& clause) { return std::holds_alternative<std::shared_ptr<FilterClause>>(clause); }

bool is_project(const ClauseVariant& clause) { return std::holds_alternative<std::shared_ptr<ProjectClause>>(clause); }

bool is_date_range(const ClauseVariant& clause) {
    return std::holds_alternative<std::shared_ptr<DateRangeClause>>(clause);
}

bool date_range_can_move_to_left_of(const ClauseVariant& clause) { return is_filter(clause) || is_project(clause); }

// Move each DateRangeClause as far left as it can go, so that filters that were only separated by a
// date range end up next to each other and can be merged. Each date range ends up at the front of the
// run of clauses it is allowed to move past, keeping the original order, eg
// [F1, F2, DR1, F3, F4, DR2] -> [DR1, DR2, F1, F2, F3, F4]
void move_date_ranges_left(std::vector<ClauseVariant>& clauses) {
    size_t insert_pos = 0; // location of the clause we can move a date range left to
    // Move each date range we see left to insert_pos. If we have [F1, F2, DR1, F3] then we hit rotate(start=0,
    // middle=2, last=3). This rotates within [0, 3) and makes index 2 the new start, that is it rotates [F1, F2, DR1]
    // until DR1 is the new start. This results in [DR1, F1, F2, F3].
    for (size_t i = 0; i < clauses.size(); ++i) {
        if (is_date_range(clauses.at(i))) {
            std::rotate(clauses.begin() + insert_pos, clauses.begin() + i, clauses.begin() + i + 1);
            ++insert_pos;
        } else if (!date_range_can_move_to_left_of(clauses.at(i))) {
            insert_pos = i + 1;
        }
    }
}

std::shared_ptr<FilterClause> merge_filter_run(const std::vector<std::shared_ptr<FilterClause>>& filters) {
    if (filters.size() == 1) {
        return filters.front();
    }
    std::vector<std::shared_ptr<ExpressionContext>> expression_contexts;
    std::unordered_set<std::string> input_columns;
    bool any_memory = false;
    for (const auto& filter : filters) {
        expression_contexts.push_back(filter->expression_context_);
        if (filter->clause_info_.input_columns_.has_value()) {
            input_columns.insert(
                    filter->clause_info_.input_columns_->begin(), filter->clause_info_.input_columns_->end()
            );
        }
        any_memory = any_memory || filter->optimisation_ == PipelineOptimisation::MEMORY;
    }
    // Respect the (opt-in) memory request if any clause in the run asked for it (speed is the default if the user
    // doesn't specify).
    auto optimisation = any_memory ? PipelineOptimisation::MEMORY : PipelineOptimisation::SPEED;
    return std::make_shared<FilterClause>(
            std::move(input_columns), and_filter_expression_contexts(expression_contexts), optimisation
    );
}

std::vector<ClauseVariant> merge_consecutive_filter_clauses(std::vector<ClauseVariant>&& clauses) {
    std::vector<ClauseVariant> result;
    std::vector<std::shared_ptr<FilterClause>> run;
    auto flush_run = [&]() {
        if (!run.empty()) {
            result.emplace_back(merge_filter_run(run));
        }
        run.clear();
    };
    for (auto& clause : clauses) {
        if (is_filter(clause)) {
            run.push_back(std::get<std::shared_ptr<FilterClause>>(clause));
        } else {
            flush_run();
            result.push_back(std::move(clause));
        }
    }
    flush_run();
    return result;
}

} // namespace

std::vector<ClauseVariant> plan_query(std::vector<ClauseVariant>&& clauses) {
    if (clauses.size() >= 2 && std::holds_alternative<std::shared_ptr<DateRangeClause>>(clauses[0])) {
        util::variant_match(clauses[1], [&clauses](auto&& clause) {
            if constexpr (is_resample<typename std::remove_cvref_t<decltype(clause)>::element_type>::value) {
                const auto& date_range_clause = *std::get<std::shared_ptr<DateRangeClause>>(clauses[0]);
                auto date_range_start = date_range_clause.start_;
                auto date_range_end = date_range_clause.end_;
                clause->set_date_range(date_range_start, date_range_end);
                clauses.erase(clauses.cbegin());
            }
        });
    }
    move_date_ranges_left(clauses);
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
