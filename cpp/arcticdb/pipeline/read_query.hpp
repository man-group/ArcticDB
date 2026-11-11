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
using FilterRange = std::variant<std::monostate, entity::IndexRange, pipelines::RowRange>;

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

    ReadQuery clone() const;

    explicit ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses);

    void add_clauses(std::vector<std::shared_ptr<Clause>>&& clauses);

    void convert_to_positive_row_filter(int64_t total_rows);
};

} // namespace arcticdb::pipelines