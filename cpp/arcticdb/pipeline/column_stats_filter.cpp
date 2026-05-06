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
        const VariantNode& node, const ExpressionContext& expression_context, const StatsRowIndices& row_indices,
        const ColumnStatsData& column_stats
) {
    return util::variant_match(
            node,
            [&](const ColumnName& column_name) -> StatsVariantData {
                if (auto slot = column_stats.slot_for_column(column_name.value); slot.has_value()) {
                    return column_stats.values_at_slot(*slot, row_indices);
                }
                return std::vector<ColumnStatsValues>(row_indices.size());
            },
            [&](const ValueName& value_name) -> StatsVariantData {
                return expression_context.values_.get_value(value_name.value);
            },
            [&](const ExpressionName& expression_name) -> StatsVariantData {
                auto expr = expression_context.expression_nodes_.get_value(expression_name.value);
                return compute_stats(expression_context, *expr, row_indices, column_stats);
            },
            [&](const ValueSetName& value_set_name) -> StatsVariantData {
                return expression_context.value_sets_.get_value(value_set_name.value);
            },
            [&](const auto&) -> StatsVariantData { return std::vector(row_indices.size(), StatsComparison::UNKNOWN); }
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
        const ExpressionContext& expression_context, const ExpressionNode& node, const StatsRowIndices& row_indices,
        const ColumnStatsData& column_stats
) {
    if (is_binary_operation(node.operation_type_)) {
        auto left = evaluate_ast_node_against_stats(node.left_, expression_context, row_indices, column_stats);
        auto right = evaluate_ast_node_against_stats(node.right_, expression_context, row_indices, column_stats);
        return dispatch_binary_stats(left, right, node.operation_type_);
    }
    if (is_unary_operation(node.operation_type_)) {
        auto left = evaluate_ast_node_against_stats(node.left_, expression_context, row_indices, column_stats);
        return dispatch_unary_stats(left, node.operation_type_);
    }
    return std::vector(row_indices.size(), StatsComparison::UNKNOWN);
}

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

    // For each column in the read query with any stats we register a slot and remember the segment column indices to
    // read from.
    std::vector<StatsMetadataForColumn> stats_metadata;
    stats_metadata.reserve(header.stats_by_column().size());
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        StatsMetadataForColumn stats_metadata_for_column;
        stats_metadata_for_column.col_name = std::string{tsd.fields().at(data_col_offset).name()};
        for (const auto& entry : entry_list.entries()) {
            if (entry.type() != MIN_V1 && entry.type() != MAX_V1) {
                log::version().warn(
                        "Unknown column stats type {} for column {}, skipping",
                        static_cast<int>(entry.type()),
                        stats_metadata_for_column.col_name
                );
                continue;
            }
            const auto field_name = to_segment_column_name(stats_metadata_for_column.col_name, entry.type());
            const auto col_index = segment.column_index(field_name);
            if (!col_index.has_value()) {
                // Column was filtered out at decode time, or never present in this segment.
                continue;
            }
            const auto entry_data_type = fields.at(*col_index).type().data_type();
            if (stats_metadata_for_column.data_type == DataType::UNKNOWN) {
                stats_metadata_for_column.data_type = entry_data_type;
            } else {
                util::check(
                        stats_metadata_for_column.data_type == entry_data_type,
                        "MIN/MAX stats columns for {} disagree on data type",
                        stats_metadata_for_column.col_name
                );
            }
            stats_metadata_for_column.entries.push_back({*col_index, entry.type()});
        }
        if (!stats_metadata_for_column.entries.empty()) {
            stats_metadata.emplace_back(std::move(stats_metadata_for_column));
        }
    }

    // Construct start_indices and end_indices with date range pruning.
    // Also construct the interval [first_kept, last_kept_excl) to quickly skip stats for row ranges
    // we are not interested in when we read the statistics themselves.
    size_t first_kept = segment_row_count;
    size_t last_kept_excl = 0;
    start_indices_.reserve(segment_row_count);
    end_indices_.reserve(segment_row_count);
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
        last_kept_excl = r + 1;
        start_indices_.push_back(start_index_ts);
        end_indices_.push_back(end_index_ts);
    }
    num_rows_ = start_indices_.size();
    if (num_rows_ == 0) {
        return;
    }

    num_slots_ = stats_metadata.size();
    slots_.resize(num_slots_);
    for (auto& slot_data : slots_) {
        slot_data.mins.resize(num_rows_);
        slot_data.maxes.resize(num_rows_);
    }

    for (size_t slot = 0; slot < num_slots_; ++slot) {
        auto& stats_metadata_for_column = stats_metadata.at(slot);
        col_name_to_slot_.emplace(stats_metadata_for_column.col_name, slot);
        auto& slot_data = slots_.at(slot);

        details::visit_type(stats_metadata_for_column.data_type, [&]<typename T>(T) {
            using type_info = ScalarTypeInfo<T>;
            if constexpr (is_numeric_type(type_info::data_type) || is_time_type(type_info::data_type) ||
                          is_bool_type(type_info::data_type)) {
                using RawType = type_info::RawType;

                for (const auto& entry : stats_metadata_for_column.entries) {
                    const auto& column = segment.column(static_cast<position_t>(entry.segment_col_idx));
                    const bool is_min = entry.stat_type == MIN_V1;
                    auto& dest = is_min ? slot_data.mins : slot_data.maxes;

                    if (!column.is_sparse() && static_cast<size_t>(column.row_count()) == segment_row_count) {
                        // Dense
                        auto it = column.begin<typename type_info::TDT>();
                        std::advance(it, static_cast<ssize_t>(first_kept));
                        for (size_t r = first_kept; r < last_kept_excl; ++r, ++it) {
                            const size_t kept = r - first_kept;
                            dest.at(kept) = Value{*it, type_info::data_type};
                        }
                    } else {
                        // Sparse
                        for (size_t r = first_kept; r < last_kept_excl; ++r) {
                            if (auto opt_val = column.scalar_at<RawType>(static_cast<position_t>(r));
                                opt_val.has_value()) {
                                const size_t kept = r - first_kept;
                                dest.at(kept) = Value{*opt_val, type_info::data_type};
                            }
                        }
                    }
                }
            }
        });
    }

    index_to_row_.reserve(num_rows_);
    std::unordered_set<std::pair<timestamp, timestamp>, util::PairHasher> duplicate_keys;
    for (size_t r = 0; r < num_rows_; ++r) {
        auto key = std::make_pair(start_indices_.at(r), end_indices_.at(r));
        if (auto [_, inserted] = index_to_row_.emplace(key, r); !inserted) {
            // Duplicate (start_index, end_index) — can happen with timestamp indices when multiple
            // segments span the same time range.
            duplicate_keys.insert(key);
        }
    }
    for (const auto& key : duplicate_keys) {
        log::version().debug("Duplicate key detected in column stats - dropping {}", key);
        index_to_row_.erase(key);
    }
}

std::optional<size_t> ColumnStatsData::find_row(timestamp start_index, timestamp end_index) const {
    if (auto it = index_to_row_.find({start_index, end_index}); it != index_to_row_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<size_t> ColumnStatsData::slot_for_column(const std::string& col_name) const {
    if (auto it = col_name_to_slot_.find(col_name); it != col_name_to_slot_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<ColumnStatsValues> ColumnStatsData::values_at_slot(
        size_t slot, const std::vector<std::optional<size_t>>& row_indices
) const {
    std::vector<ColumnStatsValues> result(row_indices.size());
    if (slot >= num_slots_ || num_rows_ == 0) {
        return result;
    }

    const auto& slot_data = slots_.at(slot);
    for (size_t i = 0; i < row_indices.size(); ++i) {
        const auto& maybe_row = row_indices.at(i);
        if (!maybe_row.has_value()) {
            continue;
        }
        const size_t r = *maybe_row;
        const bool min_set = slot_data.mins.at(r).has_value();
        const bool max_set = slot_data.maxes.at(r).has_value();
        util::check(min_set == max_set, "MIN and MAX should both be present or both be absent");
        auto& result_entry = result.at(i);
        if (min_set) {
            result_entry.min = slot_data.mins.at(r);
            result_entry.max = slot_data.maxes.at(r);
        } else {
            result_entry.column_absent = true;
        }
    }
    return result;
}

ColumnStatsValues ColumnStatsData::stats_for(size_t slot, size_t row) const {
    auto v = values_at_slot(slot, {std::optional<size_t>{row}});
    return v.empty() ? ColumnStatsValues{} : std::move(v.at(0));
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

        StatsRowIndices row_indices;
        row_indices.reserve(isr.size());
        [[maybe_unused]] size_t total_count = 0; // for debug logging only, unused in release build
        for (size_t row = 0; row < isr.size(); ++row) {
            if (!res->get_bit(row)) {
                // Don't bother - we already know we don't need to look at the segment
                row_indices.emplace_back(std::nullopt);
                continue;
            }
            total_count++;
            timestamp start_idx = *(start_index_col + row);
            timestamp end_idx = *(end_index_col + row);
            row_indices.emplace_back(column_stats_data.find_row(start_idx, end_idx));
        }
        util::check(row_indices.size() == isr.size(), "Expected row_indices.size() == isr.size()");

        // Evaluate the AST
        StatsVariantData result = evaluate_ast_node_against_stats(
                expression_context.root_node_name_, expression_context, row_indices, column_stats_data
        );
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
            const auto& date_range_clause = folly::poly_cast<DateRangeClause>(*clause);
            if (!result.date_range.has_value()) {
                result.date_range = std::make_pair(date_range_clause.start(), date_range_clause.end());
            } else {
                result.date_range->first = std::max(result.date_range->first, date_range_clause.start());
                result.date_range->second = std::min(result.date_range->second, date_range_clause.end());
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
