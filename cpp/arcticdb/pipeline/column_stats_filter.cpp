/*
 Copyright 2026 Man Group Operations Limited

 Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

 As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
 be governed by the Apache License, version 2.0.
 */
#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>

#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/processing/query_planner.hpp>

namespace arcticdb {

namespace {

std::optional<Value> extract_value_from_column(
        const SegmentInMemory::iterator& it, size_t col_idx, DataType data_type
) {
    return details::visit_type(data_type, [&it, col_idx](auto tag) -> std::optional<Value> {
        using type_info = ScalarTypeInfo<decltype(tag)>;
        // Only numeric types are supported at the moment.
        if constexpr (is_numeric_type(type_info::data_type) || is_time_type(type_info::data_type) ||
                      is_bool_type(type_info::data_type)) {
            auto opt_val = it->scalar_at<typename type_info::RawType>(col_idx);
            if (opt_val.has_value()) {
                return Value{*opt_val, type_info::data_type};
            }
        }
        return std::nullopt;
    });
}

} // anonymous namespace

bool is_column_stats_enabled() { return ConfigsMap::instance()->get_int("ColumnStats.UseForQueries", 0) == 1; }

bool should_try_column_stats_read(const ReadQuery& read_query) {
    if (!is_column_stats_enabled()) {
        return false;
    }
    if (read_query.clauses_.empty()) {
        return false;
    }
    for (const auto& clause : read_query.clauses_) {
        auto& clause_type = folly::poly_type(*clause);
        if (clause_type == typeid(DateRangeClause) || clause_type == typeid(RowRangeClause)) {
            continue;
        }
        if (clause_type == typeid(FilterClause)) {
            return true;
        }
        break;
    }
    return false;
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
    default: {
        // Not yet implemented: ADD SUB MUL DIV (binary operators) Monday: 11292578954
        // ISIN ISNOTIN (binary membership) Monday: 11292567032
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

ColumnStatsData::ColumnStatsData(SegmentInMemory&& segment, const TimeseriesDescriptor& tsd) {
    using namespace arcticc::pb2::descriptors_pb2;
    if (segment.row_count() == 0) {
        return;
    }

    // Build reverse lookup: stats_seg_offset -> (column_name, stat_type)
    ColumnStatsHeader header;
    auto* metadata = segment.metadata();
    util::check(metadata != nullptr, "Column stats segment has no metadata");
    bool unpacked = metadata->UnpackTo(&header);
    util::check(unpacked, "Could not unpack ColumnStatsHeader from column stats segment metadata");

    std::unordered_map<size_t, std::pair<std::string, ColumnStatsType>> stats_at_column_index;
    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        std::string col_name{tsd.fields().at(data_col_offset).name()};
        for (const auto& entry : entry_list.entries()) {
            stats_at_column_index.emplace(entry.stats_seg_offset(), std::make_pair(col_name, entry.type()));
        }
    }

    // Future aseaton consider iterating column-wise rather than row-wise?
    const auto& fields = segment.descriptor().fields();
    rows_.reserve(segment.row_count());
    for (auto it = segment.begin(); it != segment.end(); ++it) {
        ColumnStatsRow stats_row;

        // The index values in the column stats segment are just rowcounts if the symbol is string-indexed
        auto start_index = it->scalar_at<timestamp>(start_index_column_offset);
        auto end_index = it->scalar_at<timestamp>(end_index_column_offset);

        if (!start_index || !end_index) {
            log::version().warn("Saw column stats row without start_index or end_index, discarding all column stats");
            rows_.clear();
            index_to_row_.clear();
            return;
        }

        stats_row.start_index = *start_index;
        stats_row.end_index = *end_index;

        for (size_t col_idx = end_index_column_offset + 1; col_idx < fields.size(); ++col_idx) {
            const auto& field = fields[col_idx];
            auto lookup_it = stats_at_column_index.find(col_idx);
            if (lookup_it == stats_at_column_index.end()) {
                continue;
            }
            const auto& [col_name, stat_type] = lookup_it->second;
            auto value = extract_value_from_column(it, col_idx, field.type().data_type());

            auto& stats = stats_row.stats_for_column[col_name];
            switch (stat_type) {
            case COLUMN_STATS_TYPE_MIN_V1:
                stats.min = value;
                break;
            case COLUMN_STATS_TYPE_MAX_V1:
                stats.max = value;
                break;
            default:
                log::version().warn(
                        "Unknown column stats type {} at offset {}, skipping", static_cast<int>(stat_type), col_idx
                );
                break;
            }
        }

        auto key = std::make_pair(stats_row.start_index, stats_row.end_index);
        if (auto [_, inserted] = index_to_row_.emplace(key, rows_.size()); !inserted) {
            // Duplicate (start_index, end_index). This can happen with timestamp indices where
            // multiple segments span the same time range.
            duplicate_keys_.insert(key);
        }
        rows_.push_back(std::move(stats_row));
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

bool ColumnStatsData::empty() const { return rows_.empty(); }

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
            if (const ColumnStatsRow* stats = column_stats_data.find_stats(start_idx, end_idx)) {
                stats_rows.push_back(stats);
            } else {
                stats_rows.push_back(nullptr);
            }
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

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const TimeseriesDescriptor& tsd,
        const std::vector<std::shared_ptr<Clause>>& clauses
) {
    std::vector<std::shared_ptr<ExpressionContext>> filter_expressions;

    for (const auto& clause : clauses) {
        auto& clause_type = folly::poly_type(*clause);

        if (clause_type == typeid(DateRangeClause) || clause_type == typeid(RowRangeClause)) {
            continue;
        }

        // Resample, GroupBy, and Projection clauses transform the data so column stats
        // computed on the original segments are no longer valid for any subsequent filters.
        if (clause_type != typeid(FilterClause)) {
            break;
        }

        FilterClause& filter = folly::poly_cast<FilterClause>(*clause);
        filter_expressions.emplace_back(filter.expression_context_);
    }

    util::check(!filter_expressions.empty(), "Expected at least one filter expression");

    ColumnStatsData column_stats{std::move(column_stats_segment), tsd};

    ARCTICDB_DEBUG(log::version(), "AND-ing expression contexts from filters");
    ExpressionContext overall_context = and_filter_expression_contexts(filter_expressions);

    ARCTICDB_DEBUG(log::version(), "Creating column stats filter");
    return create_column_stats_filter(std::move(column_stats), std::move(overall_context));
}

} // namespace arcticdb
