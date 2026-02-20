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
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
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
    default:
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

std::optional<ColumnStatsData> try_load_column_stats(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item
) {
    auto column_stats_key = version_store::index_key_to_column_stats_key(versioned_item.key_);

    try {
        auto column_stats_segment = store->read_sync(column_stats_key).second;
        return ColumnStatsData{std::move(column_stats_segment)};
    } catch (const storage::KeyNotFoundException&) {
        ARCTICDB_DEBUG(log::version(), "No column stats available for segment pruning");
        return std::nullopt;
    }
}

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

std::optional<FilterQuery<index::IndexSegmentReader>> try_create_column_stats_filter_for_clauses(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::vector<std::shared_ptr<Clause>>& clauses
) {
    if (ConfigsMap::instance()->get_int("ColumnStats.UseForQueries", 0) != 1) {
        // Feature-flagged off by default
        ARCTICDB_DEBUG(log::version(), "Not using column stats for query - feature flagged off");
        return std::nullopt;
    }

    std::vector<std::shared_ptr<ExpressionContext>> filter_expressions;

    for (const auto& clause : clauses) {
        auto& clause_type = folly::poly_type(*clause);

        if (clause_type == typeid(DateRangeClause) || clause_type == typeid(RowRangeClause)) {
            continue;
        }

        // Resample, GroupBy, and Projection clauses transform the data so column stats
        // computed on the original segments are no longer valid for any subsequent filters.
        if (clause_type != typeid(FilterClause)) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Found clause that modifies data {}, not applying any more column stats",
                    clause_type.name()
            );
            break;
        }

        FilterClause& filter = folly::poly_cast<FilterClause>(*clause);
        filter_expressions.emplace_back(filter.expression_context_);
    }

    if (filter_expressions.empty()) {
        ARCTICDB_DEBUG(log::version(), "No filter expressions - not pruning");
        return std::nullopt;
    }

    ARCTICDB_DEBUG(log::version(), "Loading column stats");
    auto column_stats = try_load_column_stats(store, versioned_item);
    if (!column_stats.has_value()) {
        ARCTICDB_DEBUG(log::version(), "No column stats available for pruning");
        return std::nullopt;
    }

    ARCTICDB_DEBUG(log::version(), "AND-ing expression contexts from filters");
    ExpressionContext overall_context = and_filter_expression_contexts(filter_expressions);

    ARCTICDB_DEBUG(log::version(), "Creating column stats filter");
    return create_column_stats_filter({std::move(*column_stats)}, std::move(overall_context));
}

} // namespace arcticdb
