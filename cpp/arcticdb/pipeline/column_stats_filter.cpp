/*
 Copyright 2026 Man Group Operations Limited

 Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

 As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
 be governed by the Apache License, version 2.0.
 */
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/processing/query_planner.hpp>

#include <iterator>
#include <unordered_set>

namespace arcticdb {

bool is_column_stats_enabled() { return ConfigsMap::instance()->get_int("ColumnStats.UseForQueries", 0) == 1; }

bool ColumnStatsQueryMetadata::should_try_column_stats_read() const {
    return is_column_stats_enabled() && !filter_expressions.empty();
}

StatsVariantData evaluate_ast_node_against_stats(
        const VariantNode& node, const ExpressionContext& expression_context, const StatsRowVector& stats_rows
) {
    return util::variant_match(
            node,
            [&](const ColumnName& column_name) -> StatsVariantData {
                std::vector<ColumnStatsValues> result;
                result.reserve(stats_rows.size());
                for (const auto& row : stats_rows) {
                    if (!row) {
                        result.emplace_back(std::nullopt, std::nullopt);
                        continue;
                    }
                    if (auto it = row->stats_for_column.find(column_name.value); it != row->stats_for_column.end()) {
                        result.push_back(it->second);
                    } else {
                        result.emplace_back(std::nullopt, std::nullopt);
                    }
                }
                util::check(
                        result.size() == stats_rows.size(), "Expected the result to have the same size as stats_rows"
                );
                return result;
            },
            [&](const ValueName& value_name) -> StatsVariantData {
                return expression_context.values_.get_value(value_name.value);
            },
            [&](const ExpressionName& expression_name) -> StatsVariantData {
                auto expr = expression_context.expression_nodes_.get_value(expression_name.value);
                return compute_stats(expression_context, *expr, stats_rows);
            },
            [&](const ValueSetName& value_set_name) -> StatsVariantData {
                return expression_context.value_sets_.get_value(value_set_name.value);
            },
            [&](const auto&) -> StatsVariantData { return std::vector(stats_rows.size(), StatsComparison::UNKNOWN); }
    );
}

StatsVariantData dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
) {
    switch (operation) {
    case OperationType::GT:
        return column_stats_detail::visit_binary_comparator_stats<GreaterThanOperator>(left, right);
    case OperationType::GE:
        return column_stats_detail::visit_binary_comparator_stats<GreaterThanEqualsOperator>(left, right);
    case OperationType::LT:
        return column_stats_detail::visit_binary_comparator_stats<LessThanOperator>(left, right);
    case OperationType::LE:
        return column_stats_detail::visit_binary_comparator_stats<LessThanEqualsOperator>(left, right);
    case OperationType::EQ:
        return column_stats_detail::visit_binary_comparator_stats<EqualsOperator>(left, right);
    case OperationType::NE:
        return column_stats_detail::visit_binary_comparator_stats<NotEqualsOperator>(left, right);
    case OperationType::AND:
    case OperationType::OR:
    case OperationType::XOR:
        return column_stats_detail::visit_binary_boolean_stats(left, right, operation);
    case OperationType::ISIN:
    case OperationType::ISNOTIN:
        return column_stats_detail::visit_binary_membership_stats(left, right, operation);
    default: {
        // Not yet implemented: ADD SUB MUL DIV (binary operators) Monday: 11292578954
        size_t sz =
                std::max(column_stats_detail::stats_variant_size(left), column_stats_detail::stats_variant_size(right));
        return std::vector(sz, StatsComparison::UNKNOWN);
    }
    }
}

StatsVariantData dispatch_unary_stats(const StatsVariantData& left, OperationType operation) {
    switch (operation) {
    case OperationType::NOT:
    case OperationType::IDENTITY:
        return column_stats_detail::visit_unary_boolean_stats(left, operation);
    default:
        ARCTICDB_DEBUG(log::version(), "Unsupported unary operator for stats {}", operation);
        return util::variant_match(
                left,
                [](const std::vector<StatsComparison>& comparisons) -> StatsVariantData {
                    return std::vector(comparisons.size(), StatsComparison::UNKNOWN);
                },
                [](const std::vector<ColumnStatsValues>& values) -> StatsVariantData {
                    return std::vector(values.size(), StatsComparison::UNKNOWN);
                },
                [](const std::shared_ptr<Value>&) -> StatsVariantData {
                    util::raise_rte("Do not expect a Value in dispatch_unary_stats!");
                },
                [](const std::shared_ptr<ValueSet>&) -> StatsVariantData {
                    util::raise_rte("Do not expect a ValueSet in dispatch_unary_stats!");
                }
        );
    }
}

StatsVariantData compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const StatsRowVector& stats_rows
) {
    if (is_binary_operation(node.operation_type_)) {
        auto left = evaluate_ast_node_against_stats(node.left_, expression_context, stats_rows);
        auto right = evaluate_ast_node_against_stats(node.right_, expression_context, stats_rows);
        return dispatch_binary_stats(left, right, node.operation_type_);
    }
    if (is_unary_operation(node.operation_type_)) {
        auto left = evaluate_ast_node_against_stats(node.left_, expression_context, stats_rows);
        return dispatch_unary_stats(left, node.operation_type_);
    }
    return std::vector(stats_rows.size(), StatsComparison::UNKNOWN);
}

namespace {

struct StatsColumnEntry {
    size_t segment_col_idx;
    arcticc::pb2::column_stats_pb2::ColumnStatsType stat_type;
    DataType data_type;
};

} // anonymous namespace

ColumnStatsData::ColumnStatsData(
        SegmentInMemory&& segment, const TimeseriesDescriptor& tsd,
        std::optional<std::pair<timestamp, timestamp>> date_range
) {
    using namespace arcticc::pb2::column_stats_pb2;
    if (segment.row_count() == 0) {
        return;
    }

    ColumnStatsHeader header;
    auto* metadata = segment.metadata();
    util::check(metadata != nullptr, "Column stats segment has no metadata");
    bool unpacked = metadata->UnpackTo(&header);
    util::check(unpacked, "Could not unpack ColumnStatsHeader from column stats segment metadata");
    validate_column_stats_header_version(header);

    segment.init_column_map();
    const auto& fields = segment.descriptor().fields();
    const auto segment_row_count = segment.row_count();

    const auto& start_index_col = segment.column(start_index_column_offset);
    const auto& end_index_col = segment.column(end_index_column_offset);
    if (start_index_col.is_sparse() || end_index_col.is_sparse() ||
        static_cast<size_t>(start_index_col.row_count()) != segment_row_count ||
        static_cast<size_t>(end_index_col.row_count()) != segment_row_count) {
        log::version().warn("Saw column stats row without start_index or end_index, discarding all column stats");
        return;
    }

    // For each user column with stats: resolve its segment column indices by name. Column lookup
    // (rather than the in-header offset) tolerates partial decode where unrelated columns may have
    // been dropped.
    std::vector<std::pair<std::string, std::vector<StatsColumnEntry>>> entries_by_column;
    entries_by_column.reserve(header.stats_by_column().size());
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        std::string col_name{tsd.fields().at(data_col_offset).name()};
        std::vector<StatsColumnEntry> entries;
        for (const auto& entry : entry_list.entries()) {
            if (entry.type() != MIN_V1 && entry.type() != MAX_V1) {
                log::version().warn(
                        "Unknown column stats type {} for column {}, skipping", static_cast<int>(entry.type()), col_name
                );
                continue;
            }
            const auto field_name = to_segment_column_name(col_name, entry.type());
            const auto col_index = segment.column_index(field_name);
            if (!col_index.has_value()) {
                // Column was filtered out at decode time, or never present in this segment.
                continue;
            }
            entries.push_back({static_cast<size_t>(*col_index), entry.type(), fields.at(*col_index).type().data_type()}
            );
        }
        if (!entries.empty()) {
            entries_by_column.emplace_back(std::move(col_name), std::move(entries));
        }
    }

    // Build rows for the contiguous kept range [first_kept, first_kept + rows_.size()). Because
    // both start_index and end_index are monotonically nondecreasing, the date-range filter only
    // strips a prefix and a suffix; there are no interior gaps.
    size_t first_kept = segment_row_count;
    rows_.reserve(segment_row_count);
    using TsTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
    auto start_it = start_index_col.begin<TsTDT>();
    auto end_it = end_index_col.begin<TsTDT>();
    timestamp prev_start = 0;
    timestamp prev_end = 0;
    for (size_t r = 0; r < segment_row_count; ++r, ++start_it, ++end_it) {
        const timestamp start_index_ts = *start_it;
        const timestamp end_index_ts = *end_it;
        if (r > 0) {
            util::check(
                    start_index_ts >= prev_start && end_index_ts >= prev_end,
                    "Column stats segment start_index/end_index must be monotonically increasing "
                    "(violated at row {})",
                    r
            );
        }
        prev_start = start_index_ts;
        prev_end = end_index_ts;
        if (date_range.has_value()) {
            if (start_index_ts > date_range->second) {
                break;
            }
            if (end_index_ts < date_range->first) {
                continue;
            }
        }
        if (first_kept == segment_row_count) {
            first_kept = r;
        }
        ColumnStatsRow stats_row;
        stats_row.start_index = start_index_ts;
        stats_row.end_index = end_index_ts;
        // Pre-populate so the column_absent post-pass marks columns whose source segment had no
        // values for any kept row.
        for (const auto& [col_name, _] : entries_by_column) {
            stats_row.stats_for_column.emplace(col_name, ColumnStatsValues{});
        }
        rows_.push_back(std::move(stats_row));
    }
    if (rows_.empty()) {
        return;
    }

    // Populate stats column-wise: one pass per (column, MIN/MAX) entry rather than per row.
    const size_t kept_count = rows_.size();
    for (const auto& [col_name, entries] : entries_by_column) {
        for (const auto& entry : entries) {
            const auto& column = segment.column(static_cast<position_t>(entry.segment_col_idx));
            details::visit_type(entry.data_type, [&](auto tag) {
                using type_info = ScalarTypeInfo<decltype(tag)>;
                if constexpr (is_numeric_type(type_info::data_type) || is_time_type(type_info::data_type) ||
                              is_bool_type(type_info::data_type)) {
                    using RawType = typename type_info::RawType;
                    auto write = [&](size_t kept_idx, RawType raw) {
                        auto& stats = rows_[kept_idx].stats_for_column.at(col_name);
                        Value v{raw, type_info::data_type};
                        if (entry.stat_type == MIN_V1) {
                            stats.min = std::move(v);
                        } else {
                            stats.max = std::move(v);
                        }
                    };
                    if (!column.is_sparse() && static_cast<size_t>(column.row_count()) == segment_row_count) {
                        auto it = column.begin<typename type_info::TDT>();
                        std::advance(it, static_cast<ssize_t>(first_kept));
                        for (size_t kept_idx = 0; kept_idx < kept_count; ++kept_idx, ++it) {
                            write(kept_idx, *it);
                        }
                    } else {
                        for (size_t kept_idx = 0; kept_idx < kept_count; ++kept_idx) {
                            const size_t src_row = first_kept + kept_idx;
                            if (auto opt = column.scalar_at<RawType>(static_cast<position_t>(src_row));
                                opt.has_value()) {
                                write(kept_idx, *opt);
                            }
                        }
                    }
                }
            });
        }
    }

    // If a column's MIN and MAX are both absent (sparse bitmap), the column was not present in
    // the data segment for that row.
    for (auto& row : rows_) {
        for (auto& [col_name, stats] : row.stats_for_column) {
            util::check(
                    stats.min.has_value() == stats.max.has_value(),
                    "MIN and MAX should both be present or both be absent, col_name={}",
                    col_name
            );
            if (!stats.min) {
                stats.column_absent = true;
            }
        }
    }

    index_to_row_.reserve(rows_.size());
    for (size_t r = 0; r < rows_.size(); ++r) {
        auto key = std::make_pair(rows_[r].start_index, rows_[r].end_index);
        if (auto [_, inserted] = index_to_row_.emplace(key, r); !inserted) {
            // Duplicate (start_index, end_index) — can happen with timestamp indices when
            // multiple segments span the same time range.
            duplicate_keys_.insert(key);
        }
    }
    for (const auto& key : duplicate_keys_) {
        log::version().debug("Duplicate key detected in column stats - dropping {}", key);
        index_to_row_.erase(key);
    }
}

const ColumnStatsRow* ColumnStatsData::find_stats(timestamp start_index, timestamp end_index) const {
    if (auto it = index_to_row_.find({start_index, end_index}); it != index_to_row_.end()) {
        return &rows_[it->second];
    }
    return nullptr;
}

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        ColumnStatsData&& column_stats_data, ExpressionContext&& expression_context
) {
    return [column_stats_data = std::move(column_stats_data), expression_context = std::move(expression_context)](
                   const index::IndexSegmentReader& isr, std::unique_ptr<util::BitSet>&& input
           ) mutable {
        using namespace pipelines::index;

        std::unique_ptr<util::BitSet> res;
        if (input) {
            res = std::move(input);
        } else {
            res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));
            res->invert();
        }

        auto start_index_col = isr.column(Fields::start_index).begin<stream::TimeseriesIndex::TypeDescTag>();
        auto end_index_col = isr.column(Fields::end_index).begin<stream::TimeseriesIndex::TypeDescTag>();

        StatsRowVector stats_rows;
        stats_rows.reserve(isr.size());
        [[maybe_unused]] size_t total_count = 0; // for debug logging only, unused in release build
        for (size_t row = 0; row < isr.size(); ++row) {
            if (!res->get_bit(row)) {
                // Don't bother - we already know we don't need to look at the segment
                stats_rows.push_back(nullptr);
                continue;
            }
            total_count++;
            timestamp start_idx = *(start_index_col + row);
            timestamp end_idx = *(end_index_col + row);
            stats_rows.push_back(column_stats_data.find_stats(start_idx, end_idx));
        }
        util::check(stats_rows.size() == isr.size(), "Expected stats_rows.size() == isr.size()");

        // Evaluate the AST
        StatsVariantData result =
                evaluate_ast_node_against_stats(expression_context.root_node_name_, expression_context, stats_rows);
        util::check(
                std::holds_alternative<std::vector<StatsComparison>>(result),
                "evaluate_ast_node_against_stats should evaluate to a vector<StatsComparison>"
        );

        // Convert to BitSet
        size_t pruned_count = 0;
        const auto& comparisons = std::get<std::vector<StatsComparison>>(result);
        util::check(comparisons.size() == isr.size(), "Expected comparisons.size() == isr.size()");
        for (size_t row = 0; row < isr.size(); ++row) {
            if (comparisons.at(row) == StatsComparison::NONE_MATCH) {
                res->set_bit(row, false);
                pruned_count++;
            }
        }

        log::version().debug("Column stats filter pruned {} of {} segments", pruned_count, total_count);
        return res;
    };
}

ColumnStatsQueryMetadata column_stats_query_metadata(const std::vector<std::shared_ptr<Clause>>& clauses) {
    // The clauses eligible for column stats use:
    // - FilterClauses contribute filter expressions and columns of interest
    // - DateRangeClauses contribute their range
    // - RowRangeClauses are skipped
    // - Anything else (Resample / GroupBy / Project) ends the prefix because those clauses
    // transform the data so stats computed on the original segments are no longer valid.
    ColumnStatsQueryMetadata result;
    for (const auto& clause : clauses) {
        auto& clause_type = folly::poly_type(*clause);
        if (clause_type == typeid(DateRangeClause)) {
            const auto& dr = folly::poly_cast<DateRangeClause>(*clause);
            if (!result.date_range.has_value()) {
                result.date_range = std::make_pair(dr.start(), dr.end());
            } else {
                result.date_range->first = std::max(result.date_range->first, dr.start());
                result.date_range->second = std::min(result.date_range->second, dr.end());
            }
            continue;
        }
        if (clause_type == typeid(RowRangeClause)) {
            continue;
        }
        if (clause_type != typeid(FilterClause)) {
            break;
        }
        const auto& filter = folly::poly_cast<FilterClause>(*clause);
        result.filter_expressions.emplace_back(filter.expression_context_);
        util::check(
                filter.clause_info().input_columns_.has_value(),
                "FilterClause is missing input_columns_ — Python bindings should always populate this"
        );
        for (const auto& col : *filter.clause_info().input_columns_) {
            result.columns_of_interest.insert(col);
        }
    }
    return result;
}

SegmentInMemory partial_decode_column_stats_segment(
        Segment& column_stats_segment, const TimeseriesDescriptor& tsd,
        const std::unordered_set<std::string>& columns_of_interest
) {
    using namespace arcticc::pb2::column_stats_pb2;
    ScopedTimer timer{"partial_decode_column_stats_segment", [](auto msg) { log::version().info(msg); }};

    auto maybe_metadata = decode_metadata_from_segment(column_stats_segment);
    util::check(maybe_metadata.has_value(), "Column stats segment has no metadata");
    ColumnStatsHeader header;
    bool unpacked = maybe_metadata->UnpackTo(&header);
    util::check(unpacked, "Could not unpack ColumnStatsHeader from column stats segment metadata");
    validate_column_stats_header_version(header);

    std::unordered_set<std::string> retain_field_names;
    retain_field_names.insert(start_index_column_name);
    retain_field_names.insert(end_index_column_name);
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        std::string col_name{tsd.fields().at(data_col_offset).name()};
        if (!columns_of_interest.contains(col_name)) {
            continue;
        }
        for (const auto& entry : entry_list.entries()) {
            retain_field_names.insert(to_segment_column_name(col_name, entry.type()));
        }
    }

    // Preserve the order so start_index lands at offset 0 and end_index at offset 1, matching the
    // start_index_column_offset / end_index_column_offset constants used downstream.
    StreamDescriptor partial_desc;
    partial_desc.set_index(column_stats_segment.descriptor().index());
    for (const auto& field : column_stats_segment.descriptor().fields()) {
        if (retain_field_names.contains(std::string{field.name()})) {
            partial_desc.add_field(field);
        }
    }
    util::check(
            partial_desc.fields().size() >= 2 && partial_desc.fields(0).name() == start_index_column_name &&
                    partial_desc.fields(1).name() == end_index_column_name,
            "Expected start_index/end_index at the front of the column stats segment"
    );

    SegmentInMemory partial(std::move(partial_desc), 0, AllocationType::DYNAMIC);
    decode_into_memory_segment(
            column_stats_segment, column_stats_segment.header(), partial, column_stats_segment.descriptor()
    );
    return partial;
}

namespace {

FilterQuery<index::IndexSegmentReader> build_filter_from_column_stats_data(
        ColumnStatsData&& column_stats, std::vector<std::shared_ptr<ExpressionContext>>&& filter_expressions
) {
    util::check(!filter_expressions.empty(), "Expected at least one filter expression");
    ARCTICDB_DEBUG(log::version(), "AND-ing expression contexts from filters");
    ExpressionContext overall_context = and_filter_expression_contexts(filter_expressions);
    ARCTICDB_DEBUG(log::version(), "Creating column stats filter");
    return create_column_stats_filter(std::move(column_stats), std::move(overall_context));
}

} // namespace

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        storage::KeySegmentPair&& column_stats_compressed, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
) {
    SegmentInMemory partial_segment = partial_decode_column_stats_segment(
            *column_stats_compressed.segment_ptr(), tsd, query_metadata.columns_of_interest
    );
    ColumnStatsData column_stats{std::move(partial_segment), tsd, query_metadata.date_range};
    return build_filter_from_column_stats_data(std::move(column_stats), std::move(query_metadata.filter_expressions));
}

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const TimeseriesDescriptor& tsd,
        ColumnStatsQueryMetadata&& query_metadata
) {
    ColumnStatsData column_stats{std::move(column_stats_segment), tsd, query_metadata.date_range};
    return build_filter_from_column_stats_data(std::move(column_stats), std::move(query_metadata.filter_expressions));
}

} // namespace arcticdb
