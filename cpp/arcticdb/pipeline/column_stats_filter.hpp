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
#include <arcticdb/processing/segment_filter_analysis.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/versioned_item.hpp>

#include <memory>
#include <optional>
#include <vector>

namespace arcticdb {

/**
 * Information about column stats loaded for a version.
 */
struct ColumnStatsData {
    SegmentInMemory segment;

    // Column indices for start_index and end_index
    std::optional<size_t> start_index_col;
    std::optional<size_t> end_index_col;

    // Map from column name to (min_column_index, max_column_index)
    ankerl::unordered_dense::map<std::string, std::pair<size_t, size_t>> column_min_max_indices;
};

/**
 * Try to load column stats for a versioned item.
 * Returns std::nullopt if no column stats exist for this version.
 */
std::optional<ColumnStatsData> try_load_column_stats(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item
);

/**
 * Create a filter query that uses column stats to prune segments that cannot
 * possibly match the given predicates.
 *
 * @param column_stats_data The loaded column stats data
 * @param predicates The predicates extracted from the FilterClause
 * @return A filter query that can be used with filter_index()
 */
pipelines::FilterQuery<pipelines::index::IndexSegmentReader> create_column_stats_filter(
        std::shared_ptr<ColumnStatsData> column_stats_data, std::vector<PruneablePredicate> predicates
);

/**
 * Extract predicates from a FilterClause and create a column stats filter if applicable.
 *
 * @param store The storage backend
 * @param versioned_item The versioned item being read
 * @param clauses The query clauses (looking for FilterClause)
 * @return A filter query if column stats exist and contain relevant columns, std::nullopt otherwise
 */
std::optional<pipelines::FilterQuery<pipelines::index::IndexSegmentReader>> try_create_column_stats_filter_for_clauses(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::vector<std::shared_ptr<Clause>>& clauses
);

} // namespace arcticdb
