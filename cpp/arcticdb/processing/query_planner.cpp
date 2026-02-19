/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/query_planner.hpp>

namespace arcticdb {

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
    return clauses;
}

/**
 * Create a single expression context by AND-ing together the supplied expression contexts, which should all come from
 * FilterClause objects (and hence have an ExpressionNode at their root).
 */
ExpressionContext and_filter_expression_contexts(
        const std::vector<std::shared_ptr<ExpressionContext>>& expression_contexts
) {
    util::check(expression_contexts.size() > 0, "Expression context cannot be empty");
    std::optional<ExpressionNode> overall_root;
    std::optional<ExpressionName> overall_root_name;

    ExpressionContext res;
    for (const auto& expression_context : expression_contexts) {
        util::check(
                std::holds_alternative<ExpressionName>(expression_context->root_node_name_),
                "Only expect to be called with filter expressions"
        );
        res.expression_nodes_.merge_from(expression_context->expression_nodes_);
        res.values_.merge_from(expression_context->values_);
        res.value_sets_.merge_from(expression_context->value_sets_);
        res.regex_matches_.merge_from(expression_context->regex_matches_);
        auto root_name = std::get<ExpressionName>(expression_context->root_node_name_);
        auto root_expr = expression_context->expression_nodes_.get_value(root_name.value);

        if (!overall_root) {
            overall_root = *root_expr;
            overall_root_name = root_name;
            continue;
        }

        overall_root = ExpressionNode{*overall_root_name, root_name, OperationType::AND};
    }

    res.add_expression_node("combined-expression", std::make_shared<ExpressionNode>(*overall_root));
    res.root_node_name_ = ExpressionName{"combined-expression"};
    return res;
}

} // namespace arcticdb
