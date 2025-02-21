#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb::pipelines {

ReadQuery::ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses) : clauses_(std::move(clauses)) {}

void ReadQuery::add_clauses(std::vector<std::shared_ptr<Clause>>& clauses) { clauses_ = clauses; }

void ReadQuery::convert_to_positive_row_filter(int64_t total_rows) {
    if (row_range.has_value()) {
        size_t start = row_range->start_ >= 0 ? std::min(row_range->start_, total_rows)
                                              : std::max(total_rows + row_range->start_, static_cast<int64_t>(0));
        size_t end = row_range->end_ >= 0 ? std::min(row_range->end_, total_rows)
                                          : std::max(total_rows + row_range->end_, static_cast<int64_t>(0));
        row_filter = pipelines::RowRange(start, end);
    }
}

} // namespace arcticdb::pipelines
