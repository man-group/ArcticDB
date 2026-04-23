/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/lazy_read_helpers.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

void apply_truncation(
        SegmentInMemory& segment, const pipelines::RowRange& slice_row_range, const FilterRange& row_filter
) {
    util::variant_match(
            row_filter,
            [&segment](const entity::IndexRange& index_filter) {
                // Timestamp-based truncation (date_range).
                const auto& time_filter = static_cast<const TimestampRange&>(index_filter);
                const auto num_rows = segment.row_count();
                if (num_rows == 0) {
                    return;
                }
                auto index_column = segment.column_ptr(0);
                auto first_ts = *index_column->scalar_at<timestamp>(0);
                auto last_ts = *index_column->scalar_at<timestamp>(num_rows - 1);

                if ((time_filter.first > first_ts && time_filter.first <= last_ts) ||
                    (time_filter.second >= first_ts && time_filter.second < last_ts)) {
                    auto start_row = index_column->search_sorted<timestamp>(time_filter.first, false);
                    auto end_row = index_column->search_sorted<timestamp>(time_filter.second, true);
                    segment = segment.truncate(start_row, end_row, false);
                } else if (time_filter.first > last_ts) {
                    segment = segment.truncate(0, 0, false);
                }
            },
            [&segment, &slice_row_range](const pipelines::RowRange& rr_filter) {
                // Row-based truncation (row_range / LIMIT).
                const auto num_rows = segment.row_count();
                if (num_rows == 0) {
                    return;
                }
                auto seg_start = static_cast<int64_t>(slice_row_range.first);
                auto filter_start = static_cast<int64_t>(rr_filter.first);
                auto filter_end = static_cast<int64_t>(rr_filter.second);

                auto local_start = std::max(int64_t{0}, filter_start - seg_start);
                auto local_end = std::min(static_cast<int64_t>(num_rows), filter_end - seg_start);

                if (local_start > 0 || local_end < static_cast<int64_t>(num_rows)) {
                    segment = segment.truncate(
                            static_cast<size_t>(local_start),
                            static_cast<size_t>(std::max(local_end, int64_t{0})),
                            false
                    );
                }
            },
            [](const std::monostate&) {
                // No filter — nothing to truncate
            }
    );
}

bool apply_filter_clause(
        SegmentInMemory& segment, const std::shared_ptr<ExpressionContext>& expression_context,
        const std::string& filter_root_node_name
) {
    if (!expression_context) {
        return true;
    }
    if (segment.row_count() == 0) {
        return false;
    }

    ExpressionName root_node_name(filter_root_node_name);
    ProcessingUnit proc(std::move(segment));
    proc.set_expression_context(expression_context);
    auto variant_data = proc.get(root_node_name);

    bool has_rows = false;
    util::variant_match(
            variant_data,
            [&proc, &has_rows](util::BitSet& bitset) {
                if (bitset.count() > 0) {
                    proc.apply_filter(std::move(bitset), PipelineOptimisation::SPEED);
                    has_rows = true;
                }
            },
            [](EmptyResult) {},
            [&has_rows](FullResult) { has_rows = true; },
            [](const auto&) { util::raise_rte("Expected bitset from filter clause in lazy iterator"); }
    );

    if (has_rows) {
        segment = std::move(*proc.segments_->at(0));
    }
    return has_rows;
}

size_t estimate_segment_bytes(const pipelines::SliceAndKey& sk, const StreamDescriptor& descriptor) {
    // Estimate from slice metadata: rows × columns × 8 bytes (conservative average type size).
    // This is intentionally rough — it's used for backpressure, not exact accounting.
    auto row_count = sk.slice_.row_range.diff();
    auto col_count = descriptor.field_count();
    constexpr size_t avg_bytes_per_value = 8;
    return row_count * col_count * avg_bytes_per_value;
}

} // namespace arcticdb
