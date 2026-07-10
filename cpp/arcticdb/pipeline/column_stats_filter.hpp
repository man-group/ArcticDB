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
#include <unordered_set>
#include <utility>
#include <vector>

namespace arcticdb {

struct ColumnStatsValues {
    std::optional<Value> min;
    std::optional<Value> max;
    // In-band sentinels (NaN for floats, NaT for time types) counted during write-time aggregation.
    uint64_t nan_count = 0;
    // Sparse-map gaps (rows genuinely absent from the data segment) counted during write-time aggregation.
    uint64_t null_count = 0;
    bool column_absent = false;

    ColumnStatsValues() = default;

    ColumnStatsValues(std::optional<Value> min, std::optional<Value> max) : min(std::move(min)), max(std::move(max)) {
        util::check(min.has_value() == max.has_value(), "min and max should either both be present or both be absent");
    };
};

/**
 * The calculated column statistics for a single row-slice, keyed by its starting row offset.
 */
struct CalculatedColumnStats {
    uint64_t start_row;
    // Map from column name to its stats
    std::unordered_map<std::string, ColumnStatsValues> col_name_to_stat;
};

/**
 * Parsed column statistics from a column stats segment.
 */
class ColumnStatsData {
  public:
    explicit ColumnStatsData(SegmentInMemory&& segment, const TimeseriesDescriptor& tsd);

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnStatsData)

    /**
     * Find the column stats for a given row-slice identified by its starting row offset.
     * Returns nullptr if no matching stats found.
     */
    const CalculatedColumnStats* find_stats(uint64_t start_row) const;

    bool empty() const;

  private:
    // start_row uniquely identifies a row-slice, so no duplicate handling is needed.
    std::unordered_map<uint64_t, CalculatedColumnStats> start_row_to_calculated_column_stats;
};

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
 * Is column stats feature flagged on?
 *
 * Does the query have a filter query before any resample, group by or projection clauses, so that it
 * might be able to benefit from using the column stats?
 */
bool should_try_column_stats_read(const ReadQuery& read_query);

/**
 * Create a column stats filter from an already-read column stats segment and query clauses.
 *
 * Precondition: should_try_column_stats_read(clauses) == true
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const TimeseriesDescriptor& tsd,
        const std::vector<std::shared_ptr<Clause>>& clauses
);

/**
 * Test whether column stats are feature-flagged on for queries.
 */
bool is_column_stats_enabled();

} // namespace arcticdb
