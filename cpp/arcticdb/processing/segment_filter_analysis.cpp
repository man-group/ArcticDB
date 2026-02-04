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
        const ExpressionNode& node, const ExpressionContext& context
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
            .column_name = *column_name, .op = node.operation_type_, .value = value, .column_on_left = column_on_left
    };
}

void extract_predicates_recursive(
        const VariantNode& node_ref, const ExpressionContext& context, std::vector<PruneablePredicate>& predicates
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

        // For AND operations, recurse into both sides
        // Since both conditions must be true, we can prune if either predicate allows pruning
        if (node->operation_type_ == OperationType::AND) {
            extract_predicates_recursive(node->left_, context, predicates);
            extract_predicates_recursive(node->right_, context, predicates);
        }
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

namespace {

// Helper to compare two values of the same type
template<typename T>
int compare_typed(T left, T right) {
    if (left < right)
        return -1;
    if (left > right)
        return 1;
    return 0;
}

// Convert a Value to double for comparison purposes
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
        return std::nullopt;
    }
}

} // anonymous namespace

std::optional<int> compare_values(const Value& left, const Value& right) {
    // Compare using native types when both values have the same type
    if (left.data_type() == right.data_type()) {
        return details::visit_type(left.data_type(), [&left, &right]<typename TypeTag>(TypeTag) -> std::optional<int> {
            using RawType = typename TypeTag::raw_type;
            if constexpr (std::is_arithmetic_v<RawType>) {
                auto left_val = left.get<RawType>();
                auto right_val = right.get<RawType>();
                return compare_typed(left_val, right_val);
            }
            return std::nullopt;
        });
    }

    // Fall back to double comparison for different types
    auto left_double = value_to_double(left);
    auto right_double = value_to_double(right);
    if (!left_double.has_value() || !right_double.has_value()) {
        return std::nullopt;
    }
    return compare_typed(*left_double, *right_double);
}

bool can_prune_segment_with_predicate(const PruneablePredicate& predicate, const ColumnStatValues& stats) {
    if (!stats.has_values()) {
        // No stats available, cannot prune
        return false;
    }

    const Value& predicate_val = *predicate.value;
    const Value& min_val = *stats.min_value;
    const Value& max_val = *stats.max_value;

    // Compare predicate value against min and max using native types
    auto cmp_max = compare_values(predicate_val, max_val); // predicate_val vs max
    auto cmp_min = compare_values(predicate_val, min_val); // predicate_val vs min

    if (!cmp_max.has_value() || !cmp_min.has_value()) {
        // Cannot compare values (incompatible types), cannot prune
        return false;
    }

    // Determine the effective operation based on column position
    // If column is on left: col OP value -> use op directly
    // If column is on right: value OP col -> flip the operation
    OperationType effective_op = predicate.op;
    if (!predicate.column_on_left) {
        // Flip the operation: "5 < col" becomes "col > 5"
        switch (predicate.op) {
        case OperationType::LT:
            effective_op = OperationType::GT;
            break;
        case OperationType::LE:
            effective_op = OperationType::GE;
            break;
        case OperationType::GT:
            effective_op = OperationType::LT;
            break;
        case OperationType::GE:
            effective_op = OperationType::LE;
            break;
        case OperationType::EQ:
            effective_op = OperationType::EQ;
            break; // EQ is symmetric
        default:
            return false;
        }
    }

    // Now check if we can prune based on the effective operation
    // We're checking: can ANY value in [min, max] satisfy "col effective_op val"?
    // If no value can satisfy it, we can prune.
    //
    // cmp_max: negative means predicate_val < max, 0 means equal, positive means predicate_val > max
    // cmp_min: negative means predicate_val < min, 0 means equal, positive means predicate_val > min
    switch (effective_op) {
    case OperationType::GT:
        // col > val: prune if max <= val (no value in segment can be > val)
        // prune if predicate_val >= max (cmp_max >= 0)
        return *cmp_max >= 0;

    case OperationType::GE:
        // col >= val: prune if max < val (no value in segment can be >= val)
        // prune if predicate_val > max (cmp_max > 0)
        return *cmp_max > 0;

    case OperationType::LT:
        // col < val: prune if min >= val (no value in segment can be < val)
        // prune if predicate_val <= min (cmp_min <= 0)
        return *cmp_min <= 0;

    case OperationType::LE:
        // col <= val: prune if min > val (no value in segment can be <= val)
        // prune if predicate_val < min (cmp_min < 0)
        return *cmp_min < 0;

    case OperationType::EQ:
        // col == val: prune if val < min or val > max
        // prune if predicate_val < min (cmp_min < 0) or predicate_val > max (cmp_max > 0)
        return *cmp_min < 0 || *cmp_max > 0;

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
            ARCTICDB_DEBUG(
                    log::version(),
                    "Column stats pruning: predicate on column '{}' allows pruning",
                    predicate.column_name
            );
            return true;
        }
    }

    return false;
}

} // namespace arcticdb
