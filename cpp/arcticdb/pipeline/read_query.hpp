#pragma once

#include <arcticdb/pipeline/filter_range.hpp>
#include <arcticdb/processing/clause.hpp>
#include <cstdint>
#include <optional>

namespace arcticdb::pipelines {
using arcticdb::FilterRange;

struct SignedRowRange {
    std::optional<int64_t> start_;
    std::optional<int64_t> end_;
};

struct ReadQuery {
    // std::nullopt -> all columns
    // empty vector -> only the index
    mutable std::optional<std::vector<std::string>> columns;
    std::optional<SignedRowRange> row_range;
    FilterRange row_filter; // no filter by default
    std::vector<std::shared_ptr<Clause>> clauses_;
    bool needs_post_processing{true};

    ReadQuery() = default;

    explicit ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses);

    void add_clauses(std::vector<std::shared_ptr<Clause>>&& clauses);

    void convert_to_positive_row_filter(int64_t total_rows);
};

} // namespace arcticdb::pipelines