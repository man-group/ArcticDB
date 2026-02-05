/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/column_stats_filter.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

namespace {

// Column stats column names are in the format "vX.Y_MIN(col_name)" or "vX.Y_MAX(col_name)"
// Extract the column name and operation (MIN/MAX) from the column name
struct ParsedColumnStatName {
    std::string column_name;
    bool is_min;  // true for MIN, false for MAX
};

// Parse column stat name without regex for performance
// Format: vX.Y_MIN(col_name) or vX.Y_MAX(col_name)
std::optional<ParsedColumnStatName> parse_column_stat_name(std::string_view name) {
    // Find the underscore after the version prefix
    auto underscore_pos = name.find('_');
    if (underscore_pos == std::string_view::npos) {
        return std::nullopt;
    }

    // Check version prefix starts with 'v'
    if (name.empty() || name[0] != 'v') {
        return std::nullopt;
    }

    auto after_underscore = name.substr(underscore_pos + 1);

    // Check for MIN or MAX prefix
    bool is_min;
    std::string_view after_op;
    if (after_underscore.substr(0, 4) == "MIN(") {
        is_min = true;
        after_op = after_underscore.substr(4);
    } else if (after_underscore.substr(0, 4) == "MAX(") {
        is_min = false;
        after_op = after_underscore.substr(4);
    } else {
        return std::nullopt;
    }

    // Extract column name from between parentheses
    if (after_op.empty() || after_op.back() != ')') {
        return std::nullopt;
    }

    return ParsedColumnStatName{
        .column_name = std::string(after_op.substr(0, after_op.size() - 1)),
        .is_min = is_min
    };
}

// Extract a value from a segment column at a given row
// Returns std::nullopt if the value cannot be extracted
std::optional<Value> extract_value_from_column(const Column& column, size_t row, DataType data_type) {
    std::optional<Value> result;
    details::visit_type(data_type, [&column, row, &result](auto tag) {
        using TagType = decltype(tag);
        using RawType = typename TagType::raw_type;
        if constexpr (is_numeric_type(TagType::data_type) || is_time_type(TagType::data_type)) {
            auto opt_val = column.scalar_at<RawType>(row);
            if (opt_val.has_value()) {
                result = Value(*opt_val, TagType::data_type);
            }
        }
    });
    return result;
}

}  // anonymous namespace

// Returns true if the segment MIGHT contain matching data (should be kept)
// Returns false if the segment CANNOT contain matching data (can be pruned)
bool compare_value_with_stats(
    const Value& query_value,
    const std::optional<Value>& min_value,
    const std::optional<Value>& max_value,
    OperationType op
) {
    if (!min_value || !max_value) {
        // No stats available, cannot prune
        return true;
    }

    // Handle NaN values in stats - if min or max is NaN, we cannot prune
    // because NaN comparisons always return false and the segment might have valid values
    auto check_nan = [](const Value& val) {
        if (is_floating_point_type(val.data_type())) {
            return details::visit_type(val.data_type(), [&val](auto tag) {
                using TagType = decltype(tag);
                using RawType = typename TagType::raw_type;
                if constexpr (std::is_floating_point_v<RawType>) {
                    return std::isnan(val.get<RawType>());
                }
                return false;
            });
        }
        return false;
    };

    if (check_nan(*min_value) || check_nan(*max_value)) {
        // Stats contain NaN, cannot prune based on them
        return true;
    }

    // Get the data type for comparison - use the query value's type
    DataType compare_type = query_value.data_type();

    // Check if types are compatible - if not, we cannot safely compare
    // For numeric types, we need to handle type promotion
    if (!is_numeric_type(compare_type) && !is_time_type(compare_type)) {
        // Non-numeric types not supported for column stats filtering
        return true;
    }

    return details::visit_type(compare_type, [&](auto tag) {
        using TagType = decltype(tag);
        using RawType = typename TagType::raw_type;

        if constexpr (is_numeric_type(TagType::data_type) || is_time_type(TagType::data_type)) {
            RawType qval = query_value.get<RawType>();

            // Convert min/max values to the query type for comparison
            // If types don't match, we need to cast. For safety, if the stats type
            // differs significantly, we don't prune.
            RawType minval, maxval;

            auto extract_as = [](const Value& v, DataType expected_type, RawType& out) -> bool {
                return details::visit_type(v.data_type(), [&v, expected_type, &out](auto val_tag) {
                    using ValTagType = decltype(val_tag);
                    using ValRawType = typename ValTagType::raw_type;
                    if constexpr (is_numeric_type(ValTagType::data_type) || is_time_type(ValTagType::data_type)) {
                        // Safe cast between numeric types
                        out = static_cast<RawType>(v.get<ValRawType>());
                        return true;
                    }
                    return false;
                });
            };

            if (!extract_as(*min_value, compare_type, minval) ||
                !extract_as(*max_value, compare_type, maxval)) {
                // Could not convert stats values, cannot prune
                return true;
            }

            switch (op) {
            case OperationType::GT:
                // col > qval: keep if max > qval
                return GreaterThanOperator{}(maxval, qval);
            case OperationType::GE:
                // col >= qval: keep if max >= qval
                return GreaterThanEqualsOperator{}(maxval, qval);
            case OperationType::LT:
                // col < qval: keep if min < qval
                return LessThanOperator{}(minval, qval);
            case OperationType::LE:
                // col <= qval: keep if min <= qval
                return LessThanEqualsOperator{}(minval, qval);
            case OperationType::EQ:
                // col == qval: keep if min <= qval <= max
                return LessThanEqualsOperator{}(minval, qval) && LessThanEqualsOperator{}(qval, maxval);
            case OperationType::NE:
                // col != qval: keep unless the entire segment contains only qval (min == max == qval)
                // If min == max == qval, then all values are qval, so no values satisfy != qval
                if (EqualsOperator{}(minval, qval) && EqualsOperator{}(maxval, qval)) {
                    return false;  // Can prune - all values equal qval
                }
                return true;  // Cannot prune - segment contains values other than qval
            default:
                // For unsupported operations, don't prune
                return true;
            }
        }
        return true;
    });
}

bool evaluate_expression_node_against_stats(
    const ExpressionContext& expression_context,
    const ExpressionNode& node,
    const ColumnStatsRow& stats
);

bool evaluate_node_against_stats(
    const ExpressionContext& expression_context,
    const VariantNode& node,
    const ColumnStatsRow& stats
) {
    return util::variant_match(
        node,
        [&](const ColumnName&) -> bool {
            // A bare column name doesn't give us enough info to prune
            return true;
        },
        [&](const ValueName&) -> bool {
            // A bare value doesn't give us enough info to prune
            return true;
        },
        [&](const ValueSetName&) -> bool {
            // Value sets (isin/isnotin) - not supported for column stats filtering yet
            return true;
        },
        [&](const ExpressionName& expression_name) -> bool {
            auto expr = expression_context.expression_nodes_.get_value(expression_name.value);
            return evaluate_expression_node_against_stats(expression_context, *expr, stats);
        },
        [&](const RegexName&) -> bool {
            // Regex matching not supported for column stats filtering
            return true;
        },
        [&](std::monostate) -> bool {
            return true;
        }
    );
}

bool evaluate_expression_node_against_stats(
    const ExpressionContext& expression_context,
    const ExpressionNode& node,
    const ColumnStatsRow& stats
) {
    OperationType op = node.operation_type_;

    // Handle boolean operations (AND, OR, NOT)
    if (op == OperationType::AND) {
        bool left_result = evaluate_node_against_stats(expression_context, node.left_, stats);
        bool right_result = evaluate_node_against_stats(expression_context, node.right_, stats);
        // For AND: keep if both sides say keep
        return left_result && right_result;
    }

    if (op == OperationType::OR) {
        bool left_result = evaluate_node_against_stats(expression_context, node.left_, stats);
        bool right_result = evaluate_node_against_stats(expression_context, node.right_, stats);
        // For OR: keep if either side says keep
        return left_result || right_result;
    }

    if (op == OperationType::NOT || op == OperationType::IDENTITY) {
        bool left_result = evaluate_node_against_stats(expression_context, node.left_, stats);
        if (op == OperationType::NOT) {
            // For NOT: we cannot simply invert the result because the column stats filtering
            // is conservative. If we keep a segment (true), inverting would prune it,
            // but that's incorrect - NOT of "might contain matches" is still "might contain matches".
            // We can only prune if the child says "definitely all match" which we can't determine.
            // So we return true (keep) for NOT operations.
            return true;
        }
        return left_result;
    }

    // Handle comparison operations
    if (op == OperationType::GT || op == OperationType::GE ||
        op == OperationType::LT || op == OperationType::LE ||
        op == OperationType::EQ || op == OperationType::NE) {

        // Check if this is a column vs value comparison
        std::optional<std::string> column_name;
        std::shared_ptr<Value> query_value;
        bool reversed = false;  // true if the value is on the left

        // Try left = column, right = value
        if (std::holds_alternative<ColumnName>(node.left_)) {
            column_name = std::get<ColumnName>(node.left_).value;
            if (std::holds_alternative<ValueName>(node.right_)) {
                query_value = expression_context.values_.get_value(std::get<ValueName>(node.right_).value);
            }
        }
        // Try left = value, right = column
        else if (std::holds_alternative<ValueName>(node.left_)) {
            query_value = expression_context.values_.get_value(std::get<ValueName>(node.left_).value);
            if (std::holds_alternative<ColumnName>(node.right_)) {
                column_name = std::get<ColumnName>(node.right_).value;
                reversed = true;
            }
        }

        if (!column_name || !query_value) {
            // Not a simple column vs value comparison, cannot optimize
            return true;
        }

        // Check if we have stats for this column
        auto it = stats.column_min_max.find(*column_name);
        if (it == stats.column_min_max.end()) {
            // No stats for this column, cannot prune
            return true;
        }

        const auto& [min_value, max_value] = it->second;

        // If reversed (value op column), we need to flip the operation
        OperationType effective_op = op;
        if (reversed) {
            switch (op) {
            case OperationType::GT: effective_op = OperationType::LT; break;
            case OperationType::GE: effective_op = OperationType::LE; break;
            case OperationType::LT: effective_op = OperationType::GT; break;
            case OperationType::LE: effective_op = OperationType::GE; break;
            default: break;  // EQ and NE are symmetric
            }
        }

        return compare_value_with_stats(*query_value, min_value, max_value, effective_op);
    }

    // For any other operation type, we cannot prune
    return true;
}

ColumnStatsData::ColumnStatsData(SegmentInMemory&& segment) {
    if (segment.row_count() == 0) {
        return;
    }

    // Find the start_index and end_index columns
    std::optional<size_t> start_index_col_idx;
    std::optional<size_t> end_index_col_idx;

    // Map from column name to column index
    std::unordered_map<std::string, size_t> column_indices;

    const auto& fields = segment.descriptor().fields();
    for (size_t i = 0; i < static_cast<size_t>(fields.size()); ++i) {
        const auto& field = fields[i];
        std::string_view name = field.name();

        if (name == start_index_column_name) {
            start_index_col_idx = i;
        } else if (name == end_index_column_name) {
            end_index_col_idx = i;
        } else {
            // Try to parse as a stats column
            auto parsed = parse_column_stat_name(name);
            if (parsed) {
                columns_with_stats_.insert(parsed->column_name);
                column_indices[std::string(name)] = i;
            }
        }
    }

    if (!start_index_col_idx || !end_index_col_idx) {
        log::version().warn("Column stats segment missing start_index or end_index columns");
        return;
    }

    // Extract all rows
    rows_.reserve(segment.row_count());
    for (size_t row = 0; row < segment.row_count(); ++row) {
        ColumnStatsRow stats_row;

        // Extract start_index and end_index
        auto start_val = segment.column(*start_index_col_idx).scalar_at<timestamp>(row);
        auto end_val = segment.column(*end_index_col_idx).scalar_at<timestamp>(row);

        if (!start_val || !end_val) {
            continue;
        }

        stats_row.start_index = *start_val;
        stats_row.end_index = *end_val;

        // Extract MIN/MAX values for each column with stats
        for (size_t col_idx = 0; col_idx < static_cast<size_t>(fields.size()); ++col_idx) {
            const auto& field = fields[col_idx];
            std::string_view name = field.name();

            auto parsed = parse_column_stat_name(name);
            if (!parsed) {
                continue;
            }

            auto value = extract_value_from_column(
                segment.column(col_idx),
                row,
                field.type().data_type()
            );

            auto& min_max = stats_row.column_min_max[parsed->column_name];
            if (parsed->is_min) {
                min_max.first = value;
            } else {
                min_max.second = value;
            }
        }

        // Add to lookup map
        index_to_row_[{stats_row.start_index, stats_row.end_index}] = rows_.size();
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

bool ColumnStatsData::has_stats_for_column(const std::string& column_name) const {
    return columns_with_stats_.contains(column_name);
}

bool evaluate_expression_against_stats(
    const ExpressionContext& expression_context,
    const ExpressionName& root_node_name,
    const ColumnStatsRow& stats
) {
    auto root_node = expression_context.expression_nodes_.get_value(root_node_name.value);
    return evaluate_expression_node_against_stats(expression_context, *root_node, stats);
}

pipelines::FilterQuery<pipelines::index::IndexSegmentReader> create_column_stats_filter(
    std::shared_ptr<ColumnStatsData> column_stats_data,
    const std::vector<std::shared_ptr<Clause>>& clauses
) {
    // Collect expression contexts from leading FilterClauses
    std::vector<std::pair<std::shared_ptr<ExpressionContext>, ExpressionName>> filter_expressions;

    for (const auto& clause : clauses) {
        // Skip DateRangeClause and RowRangeClause - these don't affect column stats filtering
        if (folly::poly_type(*clause) == typeid(DateRangeClause) ||
            folly::poly_type(*clause) == typeid(RowRangeClause)) {
            continue;
        }
        // Check if this clause is a FilterClause
        if (folly::poly_type(*clause) != typeid(FilterClause)) {
            // Not a FilterClause (and not DateRange/RowRange), stop processing
            break;
        }
        const auto& filter_clause = folly::poly_cast<FilterClause>(*clause);
        filter_expressions.emplace_back(
            filter_clause.expression_context_,
            filter_clause.root_node_name_
        );
    }

    if (filter_expressions.empty()) {
        // No filter clauses to optimize, return a no-op filter
        return [](const pipelines::index::IndexSegmentReader& isr, std::unique_ptr<util::BitSet>&& input) {
            if (input) {
                return std::move(input);
            }
            auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));
            res->set_range(0, isr.size());
            return res;
        };
    }

    return [column_stats_data = std::move(column_stats_data),
            filter_expressions = std::move(filter_expressions)](
        const pipelines::index::IndexSegmentReader& isr,
        std::unique_ptr<util::BitSet>&& input
    ) mutable {
        using namespace pipelines::index;

        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));

        if (column_stats_data->empty()) {
            // No column stats, keep all segments
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

            // Find the column stats for this row
            const ColumnStatsRow* stats = column_stats_data->find_stats(start_idx, end_idx);

            if (!stats) {
                // No stats for this row, keep it
                res->set_bit(row, true);
                continue;
            }

            // Evaluate all filter expressions against the stats
            bool keep = true;
            for (const auto& [expr_ctx, root_name] : filter_expressions) {
                if (!evaluate_expression_against_stats(*expr_ctx, root_name, *stats)) {
                    keep = false;
                    break;
                }
            }

            if (keep) {
                res->set_bit(row, true);
            } else {
                pruned_count++;
            }
        }

        ARCTICDB_DEBUG(
            log::version(),
            "Column stats filter pruned {} of {} segments",
            pruned_count,
            total_count
        );

        return res;
    };
}

}  // namespace arcticdb
