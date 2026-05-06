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
#include <gtest/gtest_prod.h>
#include <column_stats.pb.h>

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
    bool column_absent = false;

    ColumnStatsValues() = default;

    ColumnStatsValues(std::optional<Value> min, std::optional<Value> max) : min(std::move(min)), max(std::move(max)) {
        util::check(min.has_value() == max.has_value(), "min and max should either both be present or both be absent");
    };
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

/**
 * Parsed column statistics from a column stats segment.
 */
class ColumnStatsData {
  public:
    /**
     * @param segment The column stats segment.
     * @param tsd     The original symbol's TSD, used to resolve data_col_offsets in the column stats
     *                header back to user column names.
     * @param date_range Date range to load stats for.
     */
    explicit ColumnStatsData(
            SegmentInMemory&& segment, const TimeseriesDescriptor& tsd,
            std::optional<std::pair<timestamp, timestamp>> date_range = std::nullopt
    );

    ARCTICDB_MOVE_ONLY_DEFAULT(ColumnStatsData)

    /**
     * Find the row index for a given row-slice identified by start_index and end_index.
     * Returns nullopt if no matching stats found.
     */
    std::optional<size_t> find_row(timestamp start_index, timestamp end_index) const;

    bool empty() const { return num_rows_ == 0; }

    std::optional<size_t> slot_for_column(const std::string& col_name) const;

    /**
     * Return the min/max ColumnStatsValues for the requested slot at each row index in row_indices.
     */
    std::vector<ColumnStatsValues> values_at_slot(size_t slot, const std::vector<std::optional<size_t>>& row_indices)
            const;

  private:
    FRIEND_TEST(ColumnStatsDataTest, FindStatsAllRowsPresent);
    FRIEND_TEST(ColumnStatsDataTest, DateRangePrunesNonOverlappingRows);
    FRIEND_TEST(ColumnStatsDataTest, DuplicateIndexPairDoesNotAffectOtherRows);
    FRIEND_TEST(ColumnStatsDataTest, SparseColumnAbsentMarkedCorrectly);

    ColumnStatsValues stats_for(size_t slot, size_t row) const;

    struct SlotData {
        std::vector<std::optional<Value>> mins;  // size == num_rows_
        std::vector<std::optional<Value>> maxes; // size == num_rows_
    };

    size_t num_rows_{0};
    size_t num_slots_{0};
    std::vector<timestamp> start_indices_; // size = num_rows_
    std::vector<timestamp> end_indices_;   // size = num_rows_
    std::vector<SlotData> slots_;          // size = num_slots_

    // (start_index, end_index) -> row index. The index values are rowcounts for string-indexed symbols.
    std::unordered_map<std::pair<timestamp, timestamp>, size_t, util::PairHasher> index_to_row_;
    std::unordered_map<std::string, size_t> col_name_to_slot_;
};

struct ColumnStatsQueryMetadata {
    // Filter expressions we can apply column stats to.
    std::vector<std::shared_ptr<ExpressionContext>> filter_expressions;
    // Columns referenced in the user's query.
    std::unordered_set<std::string> columns_of_interest;
    std::optional<std::pair<timestamp, timestamp>> date_range;

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
 * filter clauses are loaded; rows outside the intersection of any DateRangeClause are pruned.
 *
 * Precondition: query_metadata.should_try_column_stats_read() == true.
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        storage::KeySegmentPair&& column_stats_compressed, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
);

/**
 * Create a column stats filter from an already-decoded column stats segment.
 *
 * Used when the column stats segment has been pre-loaded (e.g. via PreloadedIndexQuery) and the
 * compressed bytes are no longer available. Date-range row pruning still applies, but column-set
 * filtering does not — the caller has already paid the full decode cost.
 *
 * Precondition: query_metadata.should_try_column_stats_read() == true.
 */
FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
);

/**
 * Metadata about the part of the user's query to which we can apply column stats.
 */
ColumnStatsQueryMetadata column_stats_query_metadata(const std::vector<std::shared_ptr<Clause>>& clauses);

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
