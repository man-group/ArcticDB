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
                    auto& column_stats_row = row->get();
                    auto it = column_stats_row.stats_for_column.find(column_name.value);
                    if (it != column_stats_row.stats_for_column.end()) {
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
        // Not yet implemented: ADD SUB MUL DIV (binary operators) ISIN ISNOTIN (binary membership)
        size_t sz =
                std::max(column_stats_detail::stats_variant_size(left), column_stats_detail::stats_variant_size(right));
        return std::vector(sz, StatsComparison::UNKNOWN);
    }
    }
}

StatsVariantData dispatch_unary_stats(const StatsVariantData& left, OperationType operation) {
    if (!std::holds_alternative<std::vector<StatsComparison>>(left)) {
        return left; // TODO aseaton not implemented
    }
    const auto& stats_comparisons = std::get<std::vector<StatsComparison>>(left);
    switch (operation) {
    case OperationType::NOT: {
        std::vector<StatsComparison> result;
        result.reserve(stats_comparisons.size());
        for (StatsComparison comparison : stats_comparisons) {
            switch (comparison) {
            case StatsComparison::UNKNOWN:
                result.push_back(StatsComparison::UNKNOWN);
                break;
            case StatsComparison::ALL_MATCH:
                result.push_back(StatsComparison::NONE_MATCH);
                break;
            case StatsComparison::NONE_MATCH:
                result.push_back(StatsComparison::ALL_MATCH);
                break;
            default:
                util::raise_rte("Unexpected StatsComparison", comparison);
            }
        }
        util::check(result.size() == stats_comparisons.size(), "Expected result.size() == stats_comparison.size()");
        return result;
    }
    default:
        // Not implemented: ABS, ISNULL, NOTNULL, IDENTITY, NEG
        ARCTICDB_DEBUG(log::version(), "Unsupported unary operator for stats {}", operation);
        return std::vector(stats_comparisons.size(), StatsComparison::UNKNOWN);
    }
}

StatsVariantData compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const StatsRowVector& stats_rows
) {
    if (is_binary_operation(node.operation_type_)) {
        auto left = resolve_stats_node(node.left_, expression_context, stats_rows);
        auto right = resolve_stats_node(node.right_, expression_context, stats_rows);
        return dispatch_binary_stats(left, right, node.operation_type_);
    }
    if (is_unary_operation(node.operation_type_)) {
        auto left = resolve_stats_node(node.left_, expression_context, stats_rows);
        return dispatch_unary_stats(left, node.operation_type_);
    }
    return std::vector(stats_rows.size(), StatsComparison::UNKNOWN);
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
        res->invert();
        if (input) {
            *res &= *input;
        }

        if (column_stats_data.empty()) {
            // No column stats, keep all segments
            ARCTICDB_DEBUG(log::version(), "Empty column stats - keeping all segments");
            return std::move(input);
        }

        auto start_index_col = isr.column(Fields::start_index).begin<stream::TimeseriesIndex::TypeDescTag>();
        auto end_index_col = isr.column(Fields::end_index).begin<stream::TimeseriesIndex::TypeDescTag>();

        StatsRowVector stats_rows;
        stats_rows.reserve(isr.size());
        size_t total_count = 0;
        for (size_t row = 0; row < isr.size(); ++row) {
            if (input && !input->get_bit(row)) {
                // Don't bother - we already know we don't need to look at the segment
                stats_rows.emplace_back(std::nullopt);
                continue;
            }
            total_count++;
            timestamp start_idx = *(start_index_col + row);
            timestamp end_idx = *(end_index_col + row);
            if (const ColumnStatsRow* stats = column_stats_data.find_stats(start_idx, end_idx)) {
                stats_rows.emplace_back(std::cref(*stats));
            } else {
                stats_rows.emplace_back(std::nullopt);
            }
        }
        util::check(stats_rows.size() == isr.size(), "Expected stats_rows.size() == isr.size()");

        // Evaluate the AST
        StatsVariantData result =
                resolve_stats_node(expression_context.root_node_name_, expression_context, stats_rows);
        util::check(
                std::holds_alternative<std::vector<StatsComparison>>(result),
                "resolve_stats_node should evaluate to a vector<StatsComparison>"
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

    util::check(!filter_expressions.empty(), "Expected at least one filter expression");

    ColumnStatsData column_stats{std::move(column_stats_segment)};

    ARCTICDB_DEBUG(log::version(), "AND-ing expression contexts from filters");
    ExpressionContext overall_context = and_filter_expression_contexts(filter_expressions);

    ARCTICDB_DEBUG(log::version(), "Creating column stats filter");
    return create_column_stats_filter(std::move(column_stats), std::move(overall_context));
}

} // namespace arcticdb
