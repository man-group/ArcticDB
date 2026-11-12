/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/query_planner.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace arcticdb::pipelines {

ReadQuery::ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses) : clauses_(plan_query(std::move(clauses))) {}

ReadQuery ReadQuery::clone() const {
    ReadQuery res;
    res.columns = columns;
    res.row_range = row_range;
    res.row_filter = row_filter;
    res.needs_post_processing = needs_post_processing;
    // Deep copy clauses as processing pipeline modifies them (e.g. attaching ComponentManager)
    res.clauses_ = util::reserve_vector<std::shared_ptr<Clause>>(clauses_.size());
    std::ranges::transform(clauses_, std::back_inserter(res.clauses_), [](const std::shared_ptr<Clause>& clause) {
        return std::make_shared<Clause>(*clause);
    });
    return res;
}

void ReadQuery::add_clauses(std::vector<std::shared_ptr<Clause>>&& clauses) {
    clauses_ = plan_query(std::move(clauses));
}

// TODO: Rename this method
std::optional<RowRange> ReadQuery::convert_to_positive_row_filter(int64_t total_rows) {
    return util::variant_match(
            row_range,
            [](const std::monostate&) -> std::optional<RowRange> { return std::nullopt; },
            [](const RowRange& row_range) -> std::optional<RowRange> { return row_range; },
            [this, total_rows](const SignedRowRange& signed_row_range) -> std::optional<RowRange> {
                int64_t supplied_start = signed_row_range.start_.value_or(0);

                size_t start = supplied_start >= 0 ? std::min(supplied_start, total_rows)
                                                   : std::max(total_rows + supplied_start, static_cast<int64_t>(0));

                int64_t supplied_end = signed_row_range.end_.value_or(total_rows);
                size_t end = supplied_end >= 0 ? std::min(supplied_end, total_rows)
                                               : std::max(total_rows + supplied_end, static_cast<int64_t>(0));
                this->row_range = RowRange(start, end);
                return std::get<RowRange>(this->row_range);
            }
    );
    //    if (!row_range) {
    //        return std::nullopt;
    //    }
    //
    //    int64_t supplied_start = row_range->start_.value_or(0);
    //
    //    size_t start = supplied_start >= 0 ? std::min(supplied_start, total_rows)
    //                                       : std::max(total_rows + supplied_start, static_cast<int64_t>(0));
    //
    //    int64_t supplied_end = row_range->end_.value_or(total_rows);
    //    size_t end = supplied_end >= 0 ? std::min(supplied_end, total_rows)
    //                                   : std::max(total_rows + supplied_end, static_cast<int64_t>(0));
    //    return pipelines::RowRange(start, end);
}

} // namespace arcticdb::pipelines
