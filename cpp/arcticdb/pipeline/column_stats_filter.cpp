/*
 Copyright 2026 Man Group Operations Limited

 Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

 As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
 be governed by the Apache License, version 2.0.
 */
#include <arcticdb/pipeline/column_stats_filter.hpp>

#include "segment_filter_analysis.hpp"

#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>

namespace arcticdb {

namespace {

// Get a Value from a segment column at a given row, preserving the native type
std::optional<Value> get_value_at(const SegmentInMemory& segment, size_t col_idx, size_t row) {
    if (row >= segment.row_count()) {
        return std::nullopt;
    }

    const auto& field = segment.descriptor().field(col_idx);
    const auto& column = segment.column(col_idx);
    const auto data_type = field.type().data_type();

    return details::visit_type(data_type, [&column, row, data_type]<typename TypeTag>(TypeTag) -> std::optional<Value> {
        using RawType = typename TypeTag::raw_type;
        if constexpr (std::is_arithmetic_v<RawType>) {
            auto val_opt = column.scalar_at<RawType>(row);
            if (!val_opt.has_value()) {
                return std::nullopt;
            }
            return Value(*val_opt, data_type);
        }
        return std::nullopt;
    });
}

} // anonymous namespace

std::optional<ColumnStatsData> try_load_column_stats(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item
) {
    auto column_stats_key = version_store::index_key_to_column_stats_key(versioned_item.key_);

    try {
        // Use read_compressed_sync instead of read_compressed().get() to avoid deadlock.
        // This function may be called from an IO thread (via SetupPipelineContextTask),
        // and read_compressed().get() would submit another task to the same IO pool
        // and block waiting for it, potentially causing a deadlock if no threads are free.
        auto segment = store->read_compressed_sync(column_stats_key).segment_ptr();
        auto segment_in_memory = decode_segment(*segment, AllocationType::DETACHABLE);

        ColumnStatsData data;
        data.segment = std::move(segment_in_memory);

        // Find column indices
        const auto& desc = data.segment.descriptor();
        for (size_t i = 0; i < static_cast<size_t>(desc.field_count()); ++i) {
            const auto& field = desc.field(i);
            std::string_view name = field.name();

            if (name == start_index_column_name) {
                data.start_index_col = i;
            } else if (name == end_index_column_name) {
                data.end_index_col = i;
            } else {
                auto parsed = from_segment_column_name_to_internal(name);
                const auto& [col_name, stat_type] = parsed;
                auto& indices = data.column_min_max_indices[col_name];
                if (stat_type == ColumnStatElement::MIN) {
                    indices.first = i;
                } else {
                    indices.second = i;
                }
            }
        }

        if (!data.start_index_col.has_value() || !data.end_index_col.has_value()) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Column stats segment missing start_index or end_index columns, cannot use for pruning"
            );
            return std::nullopt;
        }

        ARCTICDB_DEBUG(
                log::version(),
                "Loaded column stats with {} rows and {} column stats",
                data.segment.row_count(),
                data.column_min_max_indices.size()
        );

        return data;
    } catch (const storage::KeyNotFoundException&) {
        // Column stats key doesn't exist - this is normal when stats haven't been created
        ARCTICDB_DEBUG(log::version(), "No column stats key found for this version");
        return std::nullopt;
    }
}

pipelines::FilterQuery<pipelines::index::IndexSegmentReader> create_column_stats_filter(
        std::shared_ptr<ColumnStatsData> column_stats_data, std::vector<SimplePredicate> predicates
) {
    return [stats = std::move(column_stats_data), preds = std::move(predicates)](
                   const pipelines::index::IndexSegmentReader& isr, std::unique_ptr<util::BitSet>&& input
           ) mutable -> std::unique_ptr<util::BitSet> {
        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));

        if (!stats || preds.empty()) {
            // No stats or no predicates, don't prune anything
            if (input) {
                return std::move(input);
            }
            res->set(); // All bits set (include all)
            return res;
        }

        // Build a map from (start_row, end_row) -> column_stats_row for quick lookup
        ankerl::unordered_dense::map<std::pair<int64_t, int64_t>, size_t> stats_index;
        {
            const auto& start_col = stats->segment.column(*stats->start_index_col);
            const auto& end_col = stats->segment.column(*stats->end_index_col);

            for (size_t i = 0; i < stats->segment.row_count(); ++i) {
                auto start_idx = start_col.scalar_at<int64_t>(i).value();
                auto end_idx = end_col.scalar_at<int64_t>(i).value();
                stats_index[std::make_pair(start_idx, end_idx)] = i;
            }
        }

        // Get iterators for start_index and end_index in the index segment
        // Note: Column stats store timestamp indices (nanoseconds), not row numbers
        const auto& start_index_col = isr.column(pipelines::index::Fields::start_index);
        const auto& end_index_col = isr.column(pipelines::index::Fields::end_index);

        size_t pruned_count = 0;

        // Process each row in the index segment
        for (size_t r = 0; r < isr.size(); ++r) {
            int64_t start_idx = start_index_col.scalar_at<int64_t>(r).value();
            int64_t end_idx = end_index_col.scalar_at<int64_t>(r).value();

            // If there's an input bitset and this row is already excluded, skip it
            if (input && !input->test(r)) {
                continue;
            }

            // Look up column stats for this segment
            auto stats_it = stats_index.find(std::make_pair(start_idx, end_idx));
            if (stats_it == stats_index.end()) {
                // No stats for this segment, include it
                res->set_bit(r, true);
                continue;
            }

            size_t stats_row = stats_it->second;

            // Check if we can prune this segment based on any predicate
            bool should_prune =
                    can_prune_segment(preds, [&](const std::string& col_name) -> std::optional<ColumnStatValues> {
                        auto col_it = stats->column_min_max_indices.find(col_name);
                        if (col_it == stats->column_min_max_indices.end()) {
                            return std::nullopt;
                        }

                        auto min_val = get_value_at(stats->segment, col_it->second.first, stats_row);
                        auto max_val = get_value_at(stats->segment, col_it->second.second, stats_row);

                        if (!min_val.has_value() || !max_val.has_value()) {
                            return std::nullopt;
                        }

                        return ColumnStatValues{std::move(min_val), std::move(max_val)};
                    });

            if (should_prune) {
                ARCTICDB_DEBUG(
                        log::version(), "Column stats pruning: Skipping segment {} (index {}-{})", r, start_idx, end_idx
                );
                pruned_count++;
                // Don't set the bit - segment is pruned
            } else {
                res->set_bit(r, true);
            }
        }

        // If there was an input bitset, AND our result with it
        if (input) {
            *res &= *input;
        }

        ARCTICDB_DEBUG(log::version(), "Column stats filter pruned {} of {} segments", pruned_count, isr.size());

        return res;
    };
}

std::optional<FilterQuery<index::IndexSegmentReader>> try_create_column_stats_filter_for_clauses(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::vector<std::shared_ptr<Clause>>& clauses
) {
    if (ConfigsMap::instance()->get_int("ColumnStats.UseForQueries", 0) != 1) {
        // Feature-flagged off by default
        ARCTICDB_DEBUG(log::version(), "Not using column stats for query - feature flagged off");
        return std::nullopt;
    }

    // TODO aseaton - stop once you see Resample / GroupBy / Projection
    // TODO aseaton don't just look at the first clause!!!

    for (const auto& clause : clauses) {
        ARCTICDB_DEBUG(
                log::version(),
                "Clause type: {} vs FilterClause: {}",
                folly::poly_type(*clause).name(),
                typeid(FilterClause).name()
        );
        if (folly::poly_type(*clause) != typeid(FilterClause)) {
            continue;
        }

        FilterClause& filter = folly::poly_cast<FilterClause>(*clause);

        auto predicates = extract_simple_predicates(*filter.expression_context_);
        if (predicates.empty()) {
            ARCTICDB_DEBUG(log::version(), "No simple predicates found in FilterClause");
            continue;
        }

        ARCTICDB_DEBUG(log::version(), "Found {} simple predicates in FilterClause", predicates.size());

        // Try to load column stats
        auto column_stats = try_load_column_stats(store, versioned_item);
        if (!column_stats.has_value()) {
            ARCTICDB_DEBUG(log::version(), "No column stats available for pruning");
            return std::nullopt;
        }

        // Check if we have stats for any of the predicate columns
        bool has_relevant_stats = false;
        for (const auto& pred : predicates) {
            if (column_stats->column_min_max_indices.contains(pred.column_name)) {
                has_relevant_stats = true;
                break;
            }
        }

        if (!has_relevant_stats) {
            ARCTICDB_DEBUG(log::version(), "Column stats exist but don't contain relevant columns for predicates");
            return std::nullopt;
        }

        // Create and return the filter
        return create_column_stats_filter(
                std::make_shared<ColumnStatsData>(std::move(*column_stats)), std::move(predicates)
        );
    }

    return std::nullopt;
}

} // namespace arcticdb
