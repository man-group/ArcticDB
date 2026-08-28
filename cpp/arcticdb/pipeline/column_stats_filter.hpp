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
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/util/bitset.hpp>

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <column_stats.pb.h>

namespace arcticdb {

struct ColumnStatsValues {
    std::optional<Value> min;
    std::optional<Value> max;
    // Rows for which ArcticDB's ISNULL would be true, counted during write-time aggregation:
    // sparse-map gaps (rows genuinely absent from the data segment) plus in-band sentinel values
    // (NaN for floats, NaT for time types).
    uint64_t isnull_count = 0;
    bool column_absent = false;

    ColumnStatsValues() = default;

    ColumnStatsValues(std::optional<Value> min, std::optional<Value> max) : min(std::move(min)), max(std::move(max)) {
        util::check(min.has_value() == max.has_value(), "min and max should either both be present or both be absent");
    };

    bool all_isnull() const { return !min.has_value() && isnull_count > 0; }
};

struct StatsIndexAndType {
    size_t segment_col_idx;
    arcticc::pb2::column_stats_pb2::ColumnStatsType stat_type;
};

struct StatsMetadataForColumn {
    std::string col_name;
    DataType data_type{DataType::UNKNOWN};
    std::vector<StatsIndexAndType> entries;
};

struct StatsForColumn {
    std::vector<std::optional<Value>> mins;  // size == num_rows_
    std::vector<std::optional<Value>> maxes; // size == num_rows_
    std::vector<uint64_t> isnull_counts;     // size == num_rows_, default 0
};

/**
 * Parsed column statistics from a column stats segment.
 */
class ColumnStatsData {
  public:
    /**
     * @param segment The column stats segment.
     * @param tsd     The original symbol's TSD, used to resolve data_col_offsets in the column stats
     *                header back to user column names.
     * @param window  Range of data rows to load stats for. Stats for row slices that do not
     *                intersect it are not parsed, and find_row will not find them.
     */
    explicit ColumnStatsData(
            SegmentInMemory&& segment, const TimeseriesDescriptor& tsd,
            const std::optional<pipelines::RowRange>& window = std::nullopt
    );

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnStatsData)

    /**
     * Find the row index for the row slice covering the given range of data rows.
     * Returns nullopt if no matching stats found.
     */
    std::optional<size_t> find_row(const pipelines::RowRange& row_range) const;

    bool empty() const { return num_rows_ == 0; }

    /**
     * Return the min/max ColumnStatsValues for the requested column at each row index in row_indices.
     * Returns a vector of default-constructed (absent) entries if the column has no stats.
     */
    std::vector<ColumnStatsValues> values_for_column(
            const std::string& col_name, const std::vector<std::optional<size_t>>& row_indices
    ) const;

  private:
    std::pair<size_t, size_t> parse_row_ranges(
            const std::optional<pipelines::RowRange>& window, size_t segment_row_count, const Column& start_row_col,
            const Column& end_row_col
    );

    size_t num_rows_{0};
    std::unordered_map<std::string, StatsForColumn> stats_by_column_;

    std::unordered_map<pipelines::RowRange, size_t, pipelines::RowRange::Hasher> row_range_to_row_;
};

struct ColumnStatsQueryMetadata {
    // Filter expression we can apply column stats to.
    std::shared_ptr<ExpressionContext> filter_expression;
    // Columns referenced in the user's query.
    std::unordered_set<std::string> columns_of_interest;
    std::optional<std::pair<timestamp, timestamp>> date_range;

    ColumnStatsQueryMetadata() = default;
    explicit ColumnStatsQueryMetadata(const std::vector<std::shared_ptr<Clause>>& clauses);

    /**
     * True iff column stats are feature-flagged on and the query has at least one filter
     * expression in the column-stats-eligible prefix.
     */
    bool should_try_column_stats_read() const;
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
 * Create a column stats filter from compressed column stats bytes.
 *
 * Partially decodes the column stats segment so only the stats columns referenced by the query's
 * filter clauses are loaded.
 *
 * Precondition: query_metadata.should_try_column_stats_read() == true.
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        std::shared_ptr<Segment> column_stats_compressed, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
);

/**
 * Decode a column stats segment, only considering fields referenced by columns_of_interest.
 */
SegmentInMemory partial_decode_column_stats_segment(
        Segment& column_stats_segment, const TimeseriesDescriptor& tsd,
        const std::unordered_set<std::string>& columns_of_interest
);

/**
 * Test whether column stats are feature-flagged on for queries.
 */
bool is_column_stats_enabled();

} // namespace arcticdb
