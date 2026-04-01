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
    util::check(!expression_contexts.empty(), "Expression context cannot be empty");
    std::optional<ExpressionName> overall_root_name;

    ExpressionContext res;
    for (auto&& [idx, expression_context] : folly::enumerate(expression_contexts)) {
        util::check(
                std::holds_alternative<ExpressionName>(expression_context->root_node_name_),
                "Only expect to be called with filter expressions"
        );
        res.merge_from(*expression_context);
        auto root_name = std::get<ExpressionName>(expression_context->root_node_name_);

        if (!overall_root_name) {
            overall_root_name = root_name;
            continue;
        }

        auto intermediate_name = fmt::format("combined-{}", idx);
        auto and_node = ExpressionNode{*overall_root_name, root_name, OperationType::AND};
        res.add_expression_node(intermediate_name, std::make_shared<ExpressionNode>(and_node));
        overall_root_name = ExpressionName{intermediate_name};
    }

    res.root_node_name_ = *overall_root_name;
    return res;
}

} // namespace arcticdb
