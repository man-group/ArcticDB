/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/segment_filter_analysis.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

namespace {

bool is_pruneable_comparison_op(OperationType op) {
    switch (op) {
        case OperationType::LT:
        case OperationType::LE:
        case OperationType::GT:
        case OperationType::GE:
        case OperationType::EQ:
            return true;
        default:
            return false;
    }
}

std::optional<PruneablePredicate> try_extract_predicate_from_node(
    const ExpressionNode& node,
    const ExpressionContext& context
) {
    if (!is_pruneable_comparison_op(node.operation_type_)) {
        return std::nullopt;
    }

    // Check if this is a simple predicate: col OP value or value OP col
    std::optional<std::string> column_name;
    std::shared_ptr<Value> value;
    bool column_on_left = false;

    // Check left side for column name
    if (std::holds_alternative<ColumnName>(node.left_)) {
        column_name = std::get<ColumnName>(node.left_).value;
        column_on_left = true;
    }

    // Check right side for column name (if left wasn't a column)
    if (!column_name.has_value() && std::holds_alternative<ColumnName>(node.right_)) {
        column_name = std::get<ColumnName>(node.right_).value;
        column_on_left = false;
    }

    // If no column name found, this isn't a simple predicate
    if (!column_name.has_value()) {
        return std::nullopt;
    }

    // Now look for the value on the opposite side
    const auto& value_side = column_on_left ? node.right_ : node.left_;

    if (std::holds_alternative<ValueName>(value_side)) {
        const auto& value_name = std::get<ValueName>(value_side).value;
        try {
            value = context.values_.get_value(value_name);
        } catch (const std::out_of_range&) {
            // Value not found in context
            return std::nullopt;
        }
    } else {
        // Not a simple column vs value comparison
        return std::nullopt;
    }

    if (!value) {
        return std::nullopt;
    }

    return PruneablePredicate{
        .column_name = *column_name,
        .op = node.operation_type_,
        .value = value,
        .column_on_left = column_on_left
    };
}

void extract_predicates_recursive(
    const VariantNode& node_ref,
    const ExpressionContext& context,
    std::vector<PruneablePredicate>& predicates
) {
    if (std::holds_alternative<ExpressionName>(node_ref)) {
        const auto& expr_name = std::get<ExpressionName>(node_ref).value;
        std::shared_ptr<ExpressionNode> node;
        try {
            node = context.expression_nodes_.get_value(expr_name);
        } catch (const std::out_of_range&) {
            return;
        }

        if (!node) {
            return;
        }

        // Try to extract a predicate from this node
        auto predicate = try_extract_predicate_from_node(*node, context);
        if (predicate.has_value()) {
            predicates.push_back(std::move(*predicate));
        }

        // For AND operations, recurse into both sides (both must be true, so we can prune if either allows)
        // For now, we only handle top-level simple predicates - not nested AND/OR
        // This is a simplification for the initial implementation
    }
}

} // anonymous namespace

std::vector<PruneablePredicate> extract_pruneable_predicates(const ExpressionContext& expression_context) {
    std::vector<PruneablePredicate> predicates;

    // Start from the root node
    extract_predicates_recursive(expression_context.root_node_name_, expression_context, predicates);

    ARCTICDB_DEBUG(log::version(), "Extracted {} pruneable predicates from expression", predicates.size());
    return predicates;
}

std::optional<double> value_to_double(const Value& value) {
    switch (value.data_type()) {
        case DataType::UINT8:
            return static_cast<double>(value.get<uint8_t>());
        case DataType::UINT16:
            return static_cast<double>(value.get<uint16_t>());
        case DataType::UINT32:
            return static_cast<double>(value.get<uint32_t>());
        case DataType::UINT64:
            return static_cast<double>(value.get<uint64_t>());
        case DataType::INT8:
            return static_cast<double>(value.get<int8_t>());
        case DataType::INT16:
            return static_cast<double>(value.get<int16_t>());
        case DataType::INT32:
            return static_cast<double>(value.get<int32_t>());
        case DataType::INT64:
            return static_cast<double>(value.get<int64_t>());
        case DataType::FLOAT32:
            return static_cast<double>(value.get<float>());
        case DataType::FLOAT64:
            return value.get<double>();
        case DataType::NANOSECONDS_UTC64:
            return static_cast<double>(value.get<int64_t>());
        default:
            // String and other types not supported for comparison
            return std::nullopt;
    }
}

bool can_prune_segment_with_predicate(
    const PruneablePredicate& predicate,
    const ColumnStatValues& stats
) {
    if (!stats.has_values()) {
        // No stats available, cannot prune
        return false;
    }

    auto predicate_value = value_to_double(*predicate.value);
    if (!predicate_value.has_value()) {
        // Cannot convert predicate value to double, cannot prune
        return false;
    }

    const double val = *predicate_value;
    const double min = *stats.min_value;
    const double max = *stats.max_value;

    // Determine the effective operation based on column position
    // If column is on left: col OP value -> use op directly
    // If column is on right: value OP col -> flip the operation
    OperationType effective_op = predicate.op;
    if (!predicate.column_on_left) {
        // Flip the operation: "5 < col" becomes "col > 5"
        switch (predicate.op) {
            case OperationType::LT: effective_op = OperationType::GT; break;
            case OperationType::LE: effective_op = OperationType::GE; break;
            case OperationType::GT: effective_op = OperationType::LT; break;
            case OperationType::GE: effective_op = OperationType::LE; break;
            case OperationType::EQ: effective_op = OperationType::EQ; break;  // EQ is symmetric
            default: return false;
        }
    }

    // Now check if we can prune based on the effective operation
    // We're checking: can ANY value in [min, max] satisfy "col effective_op val"?
    // If no value can satisfy it, we can prune.
    switch (effective_op) {
        case OperationType::GT:
            // col > val: prune if max <= val (no value in segment can be > val)
            return max <= val;

        case OperationType::GE:
            // col >= val: prune if max < val (no value in segment can be >= val)
            return max < val;

        case OperationType::LT:
            // col < val: prune if min >= val (no value in segment can be < val)
            return min >= val;

        case OperationType::LE:
            // col <= val: prune if min > val (no value in segment can be <= val)
            return min > val;

        case OperationType::EQ:
            // col == val: prune if val < min or val > max
            return val < min || val > max;

        default:
            return false;
    }
}

bool can_prune_segment(
    const std::vector<PruneablePredicate>& predicates,
    const std::function<std::optional<ColumnStatValues>(const std::string&)>& get_column_stats
) {
    // For multiple predicates at the top level, they are implicitly ANDed.
    // We can prune if ANY predicate allows pruning (since all must be true).
    for (const auto& predicate : predicates) {
        auto stats = get_column_stats(predicate.column_name);
        if (!stats.has_value()) {
            // No stats for this column, skip this predicate
            continue;
        }

        if (can_prune_segment_with_predicate(predicate, *stats)) {
            ARCTICDB_DEBUG(log::version(),
                "Column stats pruning: predicate on column '{}' allows pruning (min={}, max={})",
                predicate.column_name, stats->min_value.value_or(0.0), stats->max_value.value_or(0.0));
            return true;
        }
    }

    return false;
}

} // namespace arcticdb
