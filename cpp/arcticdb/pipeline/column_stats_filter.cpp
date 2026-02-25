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
#include <processing/query_planner.hpp>

namespace arcticdb {

namespace {

std::optional<Value> extract_value_from_column(
        const SegmentInMemory::iterator& it, size_t col_idx, DataType data_type
) {
    return details::visit_type(data_type, [&it, col_idx](auto tag) -> std::optional<Value> {
        using type_info = ScalarTypeInfo<decltype(tag)>;
        // Only numeric types are supported at the moment.
        if constexpr (is_numeric_type(type_info::data_type) || is_time_type(type_info::data_type)) {
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

StatsVariantData resolve_stats_node(
        const VariantNode& node, const ExpressionContext& expression_context, const ColumnStatsRow& stats
) {
    return util::variant_match(
            node,
            [&](const ColumnName& column_name) -> StatsVariantData {
                auto it = stats.stats_for_column.find(column_name.value);
                if (it != stats.stats_for_column.end()) {
                    return it->second;
                }
                return StatsComparison::UNKNOWN;
            },
            [&](const ValueName& value_name) -> StatsVariantData {
                return expression_context.values_.get_value(value_name.value);
            },
            [&](const ExpressionName& expression_name) -> StatsVariantData {
                auto expr = expression_context.expression_nodes_.get_value(expression_name.value);
                return compute_stats(expression_context, *expr, stats);
            },
            [](const auto&) -> StatsVariantData { return StatsComparison::UNKNOWN; }
    );
}

StatsComparison dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
) {
    switch (operation) {
    case OperationType::GT:
        return column_stats_detail::visit_binary_comparator_stats(left, right, GreaterThanOperator{});
    case OperationType::GE:
        return column_stats_detail::visit_binary_comparator_stats(left, right, GreaterThanEqualsOperator{});
    case OperationType::LT:
        return column_stats_detail::visit_binary_comparator_stats(left, right, LessThanOperator{});
    case OperationType::LE:
        return column_stats_detail::visit_binary_comparator_stats(left, right, LessThanEqualsOperator{});
    case OperationType::EQ:
        return column_stats_detail::visit_binary_comparator_stats(left, right, EqualsOperator{});
    case OperationType::NE:
        return column_stats_detail::visit_binary_comparator_stats(left, right, NotEqualsOperator{});
    case OperationType::AND:
    case OperationType::OR:
    case OperationType::XOR:
        return column_stats_detail::visit_binary_boolean_stats(left, right, operation);
    default:
        // Not yet implemented: ADD SUB MUL DIV (binary operators) ISIN ISNOTIN (binary membership)
        return StatsComparison::UNKNOWN;
    }
}

StatsComparison dispatch_unary_stats(const StatsVariantData& left, OperationType operation) {
    if (!std::holds_alternative<StatsComparison>(left)) {
        return StatsComparison::UNKNOWN; // TODO aseaton not implemented
    }
    auto left_transformed = std::get<StatsComparison>(left);
    if (left_transformed == StatsComparison::UNKNOWN) {
        return StatsComparison::UNKNOWN;
    }
    switch (operation) {
    case OperationType::NOT:
        return left_transformed == StatsComparison::ALL_MATCH ? StatsComparison::NONE_MATCH
                                                              : StatsComparison::ALL_MATCH;
    default:
        // Not implemented: ABS, ISNULL, NOTNULL, IDENTITY, NEG
        ARCTICDB_DEBUG(log::version(), "Unsupported unary operator for stats {}", operation);
        return StatsComparison::UNKNOWN;
    }
}

StatsComparison compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const ColumnStatsRow& stats
) {
    if (is_binary_operation(node.operation_type_)) {
        auto left = resolve_stats_node(node.left_, expression_context, stats);
        auto right = resolve_stats_node(node.right_, expression_context, stats);
        return dispatch_binary_stats(left, right, node.operation_type_);
    }
    if (is_unary_operation(node.operation_type_)) {
        auto left = resolve_stats_node(node.left_, expression_context, stats);
        return dispatch_unary_stats(left, node.operation_type_);
    }
    // Not yet implemented: unary and ternary operators
    return StatsComparison::UNKNOWN;
}

bool evaluate_expression_against_stats(const ExpressionContext& expression_context, const ColumnStatsRow& stats) {
    auto result = resolve_stats_node(expression_context.root_node_name_, expression_context, stats);
    if (std::holds_alternative<StatsComparison>(result)) {
        return std::get<StatsComparison>(result) != StatsComparison::NONE_MATCH;
    }
    return true;
}

ColumnStatsData::ColumnStatsData(SegmentInMemory&& segment) {
    if (segment.row_count() == 0) {
        return;
    }

    std::unordered_map<size_t, std::pair<std::string, ColumnStatElement>> stats_at_column_index;

    const auto& fields = segment.descriptor().fields();
    for (size_t i = end_index_column_offset + 1; i < fields.size(); ++i) {
        const auto& field = fields[i];
        std::string_view name = field.name();
        auto parsed = from_segment_column_name_to_internal(name);
        stats_at_column_index.emplace(i, std::move(parsed));
    }

    // Future aseaton consider iterating column-wise rather than row-wise?
    rows_.reserve(segment.row_count());
    for (auto it = segment.begin(); it != segment.end(); ++it) {
        ColumnStatsRow stats_row;

        // The index values in the column stats segment are just rowcounts if the symbol is string-indexed
        auto start_index = it->scalar_at<timestamp>(start_index_column_offset);
        auto end_index = it->scalar_at<timestamp>(end_index_column_offset);

        if (!start_index || !end_index) {
            log::version().warn("Saw column stats row without start_index or end_index");
            continue;
        }

        stats_row.start_index = *start_index;
        stats_row.end_index = *end_index;

        for (size_t col_idx = end_index_column_offset + 1; col_idx < fields.size(); ++col_idx) {
            const auto& field = fields[col_idx];

            const auto& stats_type = stats_at_column_index.at(col_idx);
            auto value = extract_value_from_column(it, col_idx, field.type().data_type());

            auto& stats = stats_row.stats_for_column[stats_type.first];
            switch (stats_type.second) {
            case ColumnStatElement::MIN:
                stats.min = value;
                break;
            case ColumnStatElement::MAX:
                stats.max = value;
                break;
            }
        }

        index_to_row_[{stats_row.start_index, stats_row.end_index}] = it->row_id_;
        rows_.push_back(std::move(stats_row));
    }
}

const ColumnStatsRow* ColumnStatsData::find_stats(timestamp start_index, timestamp end_index) const {
    auto it = index_to_row_.find({start_index, end_index});
    if (it != index_to_row_.end()) {
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

        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));

        if (column_stats_data.empty()) {
            // No column stats, keep all segments
            ARCTICDB_DEBUG(log::version(), "Empty column stats - keeping all segments");
            if (input) {
                return std::move(input);
            }
            res->set_range(0, isr.size());
            return res;
        }

        auto start_index_col = isr.column(Fields::start_index).begin<stream::TimeseriesIndex::TypeDescTag>();
        auto end_index_col = isr.column(Fields::end_index).begin<stream::TimeseriesIndex::TypeDescTag>();

        size_t pruned_count = 0;
        size_t total_count = 0;

        for (size_t row = 0; row < isr.size(); ++row) {
            // Check if this row is already filtered out by a previous filter
            if (input && !input->get_bit(row)) {
                continue;
            }

            total_count++;

            timestamp start_idx = *(start_index_col + row);
            timestamp end_idx = *(end_index_col + row);

            ARCTICDB_DEBUG(log::version(), "Looking up stats {} {}", start_idx, end_idx);
            const ColumnStatsRow* stats = column_stats_data.find_stats(start_idx, end_idx);

            if (!stats) {
                ARCTICDB_DEBUG(log::version(), "No stats for this index row - keep it");
                res->set_bit(row, true);
                continue;
            }

            ARCTICDB_DEBUG(log::version(), "Evaluating against stats");
            bool keep = evaluate_expression_against_stats(expression_context, *stats);
            if (keep) {
                res->set_bit(row, true);
            } else {
                pruned_count++;
            }
        }

        ARCTICDB_DEBUG(log::version(), "Column stats filter pruned {} of {} segments", pruned_count, total_count);

        return res;
    };
}

FilterQuery<index::IndexSegmentReader> create_column_stats_filter(
        SegmentInMemory&& column_stats_segment, const std::vector<std::shared_ptr<Clause>>& clauses
) {
    util::check(is_column_stats_enabled(), "Column stats not feature flagged on");
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

    util::check(filter_expressions.size() > 0, "Expected at least one filter expression");

    ColumnStatsData column_stats{std::move(column_stats_segment)};

    ARCTICDB_DEBUG(log::version(), "AND-ing expression contexts from filters");
    ExpressionContext overall_context = and_filter_expression_contexts(filter_expressions);

    ARCTICDB_DEBUG(log::version(), "Creating column stats filter");
    return create_column_stats_filter(std::move(column_stats), std::move(overall_context));
}

} // namespace arcticdb
