#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb::pipelines {

ReadQuery::ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses) : clauses_(std::move(clauses)) {}

void ReadQuery::add_clauses(std::vector<std::shared_ptr<Clause>>& clauses) { clauses_ = clauses; }

void ReadQuery::convert_to_positive_row_filter(int64_t total_rows) {
    if (!row_range) {
        return;
    }

    int64_t supplied_start = row_range->start_.value_or(0);

    size_t start = supplied_start >= 0 ? std::min(supplied_start, total_rows)
                                       : std::max(total_rows + supplied_start, static_cast<int64_t>(0));

    int64_t supplied_end = row_range->end_.value_or(total_rows);
    size_t end = supplied_end >= 0 ? std::min(supplied_end, total_rows)
                                   : std::max(total_rows + supplied_end, static_cast<int64_t>(0));
    row_filter = pipelines::RowRange(start, end);
}

} // namespace arcticdb::pipelines
