/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <cstdint>
#include <optional>
#include <variant>

namespace arcticdb::pipelines {

struct SignedRowRange {
    std::optional<int64_t> start_;
    std::optional<int64_t> end_;
};

struct ReadQuery {
    // std::nullopt -> all columns
    // empty vector -> only the index
    mutable std::optional<std::vector<std::string>> columns;
    // row_range and row_filter are only both set by read_streaming, otherwise they are mutually exclusive
    std::variant<std::monostate, SignedRowRange, RowRange> row_range;
    // row_range now handled by the above, so in practice this is always a date_range
    std::optional<IndexRange> row_filter; // no filter by default
    std::vector<std::shared_ptr<Clause>> clauses_;
    bool needs_post_processing{true};

    ReadQuery() = default;

    ReadQuery clone() const;

    explicit ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses);

    void add_clauses(std::vector<std::shared_ptr<Clause>>&& clauses);

    void set_row_range(std::optional<int64_t> start, std::optional<int64_t> end);

    void set_row_filter(const IndexRange& index_range);

    void convert_to_positive_row_filter(int64_t total_rows);

    bool has_row_filtering() const;

    std::variant<std::monostate, RowRange, IndexRange> post_processing_filter() const;
};

} // namespace arcticdb::pipelines