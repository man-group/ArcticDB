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
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/versioned_item.hpp>

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

enum class StatsComparison {
    /** All rows in the block satisfy the predicate. */
    ALL_MATCH,
    /** None of the rows in the block satisfy the predicate. */
    NONE_MATCH,
    /** Some of the rows in the block satisfy the predicate, or we don't have enough information to draw a conclusion.
     */
    UNKNOWN
};

/**
 * Represents column statistics for a single row-slice, mapping column names to their MIN/MAX values.
 * The values are stored as raw bytes that can be compared using the appropriate type.
 */
struct ColumnStatsRow {
    timestamp start_index;
    timestamp end_index;
    // TODO use a struct for the values
    // TODO keying this off of strings is so weird
    // Map from column name to pair of (min_value, max_value) stored as Value objects
    std::unordered_map<std::string, std::pair<std::optional<Value>, std::optional<Value>>> column_min_max;
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
    // TODO support the different index types properly
    std::unordered_map<std::pair<timestamp, timestamp>, size_t, util::PairHasher> index_to_row_;
};

/**
 * Try to load column stats for a versioned item.
 * Returns std::nullopt if no column stats exist for this version.
 */
std::optional<ColumnStatsData> try_load_column_stats(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item
);

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

/**
 * Extract predicates from a FilterClause and create a column stats filter if applicable.
 *
 * @param store The storage backend
 * @param versioned_item The versioned item being read
 * @param clauses The query clauses (looking for FilterClause)
 * @return A filter query if column stats exist and contain relevant columns, std::nullopt otherwise
 */
std::optional<FilterQuery<index::IndexSegmentReader>> try_create_column_stats_filter_for_clauses(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::vector<std::shared_ptr<Clause>>& clauses
);

} // namespace arcticdb