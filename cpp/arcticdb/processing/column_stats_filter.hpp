/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/bitset.hpp>
#include <optional>
#include <memory>

namespace arcticdb {

namespace util {
// TODO aseaton Use folly::Hash not this
struct PairHasher {
    template<typename T1, typename T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);
        return h1 ^ (h2 << 1);
    }
};
}  // namespace util

/**
 * Represents column statistics for a single row-slice, mapping column names to their MIN/MAX values.
 * The values are stored as raw bytes that can be compared using the appropriate type.
 */
struct ColumnStatsRow {
    timestamp start_index;
    timestamp end_index;
    // Map from column name to pair of (min_value, max_value) stored as Value objects
    // TODO aseaton store std::optional<Value> not std::shared_ptr<Value>
    std::unordered_map<std::string, std::pair<std::shared_ptr<Value>, std::shared_ptr<Value>>> column_min_max;
};

/**
 * Parsed column statistics from a column stats segment, organized for efficient lookup
 * by start_index/end_index.
 */
class ColumnStatsData {
public:
    explicit ColumnStatsData(SegmentInMemory&& segment);

    /**
     * Find the column stats for a given row-slice identified by start_index and end_index.
     * Returns nullptr if no matching stats found.
     */
    const ColumnStatsRow* find_stats(timestamp start_index, timestamp end_index) const;

    /**
     * Check if stats exist for a given column.
     */
    bool has_stats_for_column(const std::string& column_name) const;

    /**
     * Get the set of columns that have statistics.
     */
    const std::unordered_set<std::string>& columns_with_stats() const { return columns_with_stats_; }

    bool empty() const { return rows_.empty(); }

private:
    std::vector<ColumnStatsRow> rows_;
    std::unordered_set<std::string> columns_with_stats_;
    // Map from (start_index, end_index) -> row index for fast lookup
    std::unordered_map<std::pair<timestamp, timestamp>, size_t, util::PairHasher> index_to_row_;
};

/**
 * Creates a filter function that uses column statistics to prune segments from the index
 * that cannot possibly match the given filter clauses.
 *
 * @param column_stats_data The parsed column statistics
 * @param clauses The query clauses - only leading FilterClauses are considered
 * @return A filter query that produces a bitset indicating which segments to keep
 */
pipelines::FilterQuery<pipelines::index::IndexSegmentReader> create_column_stats_filter(
    std::shared_ptr<ColumnStatsData> column_stats_data,
    const std::vector<std::shared_ptr<Clause>>& clauses
);

/**
 * Evaluates a filter expression against column statistics for a single row-slice.
 * Returns true if the row-slice should be kept (might contain matching data),
 * false if it can be pruned (definitely no matching data).
 *
 * @param expression_context The expression context containing the AST
 * @param root_node_name The name of the root node in the AST
 * @param stats The column stats for this row-slice
 * @return true if the segment should be kept, false if it can be pruned
 */
bool evaluate_expression_against_stats(
    const ExpressionContext& expression_context,
    const ExpressionName& root_node_name,
    const ColumnStatsRow& stats
);

} // namespace arcticdb
