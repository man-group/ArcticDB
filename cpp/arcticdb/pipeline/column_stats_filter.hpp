/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <memory>
#include <optional>
#include <vector>

namespace arcticdb {

namespace util {

struct PairHasher {
    template<typename T1, typename T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        return folly::hash::hash_combine(p.first, p.second);
    }
};

} // namespace util

struct ColumnStatsValues {
    std::optional<Value> min;
    std::optional<Value> max;
};

/**
 * Represents column statistics for a single row-slice, mapping column names to their statistics.
 */
struct ColumnStatsRow {
    timestamp start_index;
    timestamp end_index;
    // Map from column name to its stats
    std::unordered_map<std::string, ColumnStatsValues> stats_for_column;
};

/**
 * Parsed column statistics from a column stats segment, organized for efficient lookup
 * by start_index/end_index.
 */
class ColumnStatsData {
  public:
    explicit ColumnStatsData(SegmentInMemory&& segment);

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnStatsData)

    /**
     * Find the column stats for a given row-slice identified by start_index and end_index.
     * Returns nullptr if no matching stats found.
     */
    const ColumnStatsRow* find_stats(timestamp start_index, timestamp end_index) const;

    bool empty() const;

  private:
    std::vector<ColumnStatsRow> rows_;
    // (start_index, end_index) -> row index
    // The index values in the column stats segment are just rowcounts if the symbol is string-indexed
    std::unordered_map<std::pair<timestamp, timestamp>, size_t, util::PairHasher> index_to_row_;
};

/**
 * Evaluates a filter expression against column statistics for a single row-slice.
 * Returns true if the row-slice should be kept (might contain matching data),
 * false if it can be pruned (definitely no matching data).
 *
 * @param expression_context The expression context containing the AST
 * @param stats The column stats for this row-slice
 * @return true if the segment should be kept, false if it can be pruned
 */
bool evaluate_expression_against_stats(const ExpressionContext& expression_context, const ColumnStatsRow& stats);

/**
 * Create a filter query that uses column stats to prune segments that cannot
 * possibly match predicates in the given expression.
 *
 * @param column_stats_data The loaded column stats data
 * @param expression_context The expression to apply column stats to
 * @return A filter query that can be used with filter_index()
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        ColumnStatsData&& column_stats_data, ExpressionContext&& expression_context
);

bool should_try_column_stats_read(const ReadQuery& read_query);

/**
 * Create a column stats filter from an already-read column stats segment and query clauses.
 *
 * Precondition: should_try_column_stats_read(clauses) == true
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const std::vector<std::shared_ptr<Clause>>& clauses
);

/**
 * Test whether column stats are feature-flagged on for queries.
 */
bool is_column_stats_enabled();

} // namespace arcticdb