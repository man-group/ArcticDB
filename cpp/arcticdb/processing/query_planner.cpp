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

} // namespace arcticdb
