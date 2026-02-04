/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/codec/segment.hpp>

namespace arcticdb {

namespace {

// Parse column stats segment column name to extract the original column name and stat type
// Expected format: "vX.Y_MIN(column)" or "vX.Y_MAX(column)"
std::optional<std::pair<std::string, bool>> parse_stats_column_name(std::string_view name) {
    // Skip version prefix (e.g., "v1.0_")
    auto underscore_pos = name.find('_');
    if (underscore_pos == std::string_view::npos) {
        return std::nullopt;
    }

    auto pattern = name.substr(underscore_pos + 1);

    bool is_min = false;

    if (pattern.starts_with("MIN(")) {
        is_min = true;
        pattern = pattern.substr(4);  // Remove "MIN("
    } else if (pattern.starts_with("MAX(")) {
        // is_min stays false for MAX
        pattern = pattern.substr(4);  // Remove "MAX("
    } else {
        return std::nullopt;
    }

    // Remove trailing ")"
    if (!pattern.ends_with(")")) {
        return std::nullopt;
    }
    pattern = pattern.substr(0, pattern.size() - 1);

    return std::make_pair(std::string(pattern), is_min);
}

// Get a double value from a segment column at a given row
std::optional<double> get_double_value_at(const SegmentInMemory& segment, size_t col_idx, size_t row) {
    if (row >= segment.row_count()) {
        return std::nullopt;
    }

    const auto& field = segment.descriptor().field(col_idx);
    const auto& column = segment.column(col_idx);

    switch (field.type().data_type()) {
        case DataType::UINT8:
            return static_cast<double>(column.scalar_at<uint8_t>(row).value());
        case DataType::UINT16:
            return static_cast<double>(column.scalar_at<uint16_t>(row).value());
        case DataType::UINT32:
            return static_cast<double>(column.scalar_at<uint32_t>(row).value());
        case DataType::UINT64:
            return static_cast<double>(column.scalar_at<uint64_t>(row).value());
        case DataType::INT8:
            return static_cast<double>(column.scalar_at<int8_t>(row).value());
        case DataType::INT16:
            return static_cast<double>(column.scalar_at<int16_t>(row).value());
        case DataType::INT32:
            return static_cast<double>(column.scalar_at<int32_t>(row).value());
        case DataType::INT64:
            return static_cast<double>(column.scalar_at<int64_t>(row).value());
        case DataType::FLOAT32:
            return static_cast<double>(column.scalar_at<float>(row).value());
        case DataType::FLOAT64:
            return column.scalar_at<double>(row).value();
        case DataType::NANOSECONDS_UTC64:
            return static_cast<double>(column.scalar_at<int64_t>(row).value());
        default:
            return std::nullopt;
    }
}

} // anonymous namespace

std::optional<ColumnStatsData> try_load_column_stats(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item
) {
    auto column_stats_key = version_store::index_key_to_column_stats_key(versioned_item.key_);

    try {
        auto segment = store->read_compressed(column_stats_key).get().segment_ptr();
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
                // Try to parse as a stats column
                auto parsed = parse_stats_column_name(name);
                if (parsed.has_value()) {
                    const auto& [col_name, is_min] = *parsed;
                    auto& indices = data.column_min_max_indices[col_name];
                    if (is_min) {
                        indices.first = i;
                    } else {
                        indices.second = i;
                    }
                }
            }
        }

        if (!data.start_index_col.has_value() || !data.end_index_col.has_value()) {
            ARCTICDB_DEBUG(log::version(),
                "Column stats segment missing start_index or end_index columns, cannot use for pruning");
            return std::nullopt;
        }

        ARCTICDB_DEBUG(log::version(),
            "Loaded column stats with {} rows and {} column stats",
            data.segment.row_count(), data.column_min_max_indices.size());

        return data;
    } catch (const std::exception& e) {
        // Column stats don't exist or failed to read - this is normal
        ARCTICDB_DEBUG(log::version(), "No column stats available: {}", e.what());
        return std::nullopt;
    }
}

pipelines::FilterQuery<pipelines::index::IndexSegmentReader> create_column_stats_filter(
    std::shared_ptr<ColumnStatsData> column_stats_data,
    std::vector<PruneablePredicate> predicates
) {
    return [stats = std::move(column_stats_data), preds = std::move(predicates)](
        const pipelines::index::IndexSegmentReader& isr,
        std::unique_ptr<util::BitSet>&& input
    ) mutable -> std::unique_ptr<util::BitSet> {
        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));

        if (!stats || preds.empty()) {
            // No stats or no predicates, don't prune anything
            if (input) {
                return std::move(input);
            }
            res->set();  // All bits set (include all)
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

        // Get iterators for start_row and end_row in the index segment
        auto start_row_iter = isr.column(pipelines::index::Fields::start_row)
            .begin<stream::SliceTypeDescriptorTag>();
        auto end_row_iter = isr.column(pipelines::index::Fields::end_row)
            .begin<stream::SliceTypeDescriptorTag>();

        size_t pruned_count = 0;

        // Process each row in the index segment
        for (size_t r = 0; r < isr.size(); ++r, ++start_row_iter, ++end_row_iter) {
            int64_t start_row = *start_row_iter;
            int64_t end_row = *end_row_iter;

            // If there's an input bitset and this row is already excluded, skip it
            if (input && !input->test(r)) {
                continue;
            }

            // Look up column stats for this segment
            auto stats_it = stats_index.find(std::make_pair(start_row, end_row));
            if (stats_it == stats_index.end()) {
                // No stats for this segment, include it
                res->set_bit(r, true);
                continue;
            }

            size_t stats_row = stats_it->second;

            // Check if we can prune this segment based on any predicate
            bool should_prune = can_prune_segment(preds, [&](const std::string& col_name) -> std::optional<ColumnStatValues> {
                auto col_it = stats->column_min_max_indices.find(col_name);
                if (col_it == stats->column_min_max_indices.end()) {
                    return std::nullopt;
                }

                auto min_val = get_double_value_at(stats->segment, col_it->second.first, stats_row);
                auto max_val = get_double_value_at(stats->segment, col_it->second.second, stats_row);

                if (!min_val.has_value() || !max_val.has_value()) {
                    return std::nullopt;
                }

                return ColumnStatValues{min_val, max_val};
            });

            if (should_prune) {
                ARCTICDB_DEBUG(log::version(),
                    "Column stats pruning: Skipping segment {} (rows {}-{})",
                    r, start_row, end_row);
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

        ARCTICDB_DEBUG(log::version(),
            "Column stats filter pruned {} of {} segments",
            pruned_count, isr.size());

        return res;
    };
}

std::optional<pipelines::FilterQuery<pipelines::index::IndexSegmentReader>> try_create_column_stats_filter_for_clauses(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item,
    const std::vector<std::shared_ptr<Clause>>& clauses
) {
    // Look for FilterClause in the clauses by checking for expression context
    for (const auto& clause : clauses) {
        // Use the interface method to check if this clause has a filter expression context
        const auto* expr_context = clause->filter_expression_context();
        if (!expr_context) {
            continue;
        }

        auto predicates = extract_pruneable_predicates(*expr_context);
        if (predicates.empty()) {
            ARCTICDB_DEBUG(log::version(), "No pruneable predicates found in FilterClause");
            continue;
        }

        ARCTICDB_DEBUG(log::version(), "Found {} pruneable predicates in FilterClause", predicates.size());

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
            ARCTICDB_DEBUG(log::version(),
                "Column stats exist but don't contain relevant columns for predicates");
            return std::nullopt;
        }

        // Create and return the filter
        return create_column_stats_filter(
            std::make_shared<ColumnStatsData>(std::move(*column_stats)),
            std::move(predicates)
        );
    }

    return std::nullopt;
}

} // namespace arcticdb
