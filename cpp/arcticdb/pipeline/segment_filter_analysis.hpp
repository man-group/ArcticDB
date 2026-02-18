/*
 Copyright 2026 Man Group Operations Limited

 Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

 As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
 be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <string>
#include <vector>
#include <optional>

namespace arcticdb {

/**
 * Represents a simple predicate that can be evaluated against column min/max stats.
 *
 * For example, `col > 5` would be:
 *   column_name = "col"
 *   op = OperationType::GT
 *   value = Value(5)
 *   column_on_left = true
 *
 * For `5 < col`, it would be the same logically but:
 *   column_on_left = false (which means we need to flip the comparison)
 */
struct SimplePredicate {
    std::string column_name;
    OperationType op; // LT, LE, GT, GE, EQ
    std::shared_ptr<Value> value;
    bool column_on_left; // True if predicate is "col OP value", false if "value OP col"
};

/**
 * Extract predicates from a FilterClause's ExpressionContext that can be used
 * for segment pruning based on column min/max statistics.
 *
 * Only extracts simple predicates of the form:
 *   - col > value
 *   - col >= value
 *   - col < value
 *   - col <= value
 *   - col == value
 *
 * Returns an empty vector if no pruneable predicates are found.
 */
std::vector<SimplePredicate> extract_simple_predicates(const ExpressionContext& expression_context);

/**
 * Statistics for a single segment/column combination.
 * Stores min/max values in their native types to avoid precision loss.
 */
struct ColumnStatValues {
    std::optional<Value> min_value;
    std::optional<Value> max_value;

    bool has_values() const { return min_value.has_value() && max_value.has_value(); }
};

/**
 * Check if a segment can definitely be pruned (i.e., no rows can possibly match)
 * based on the given predicate and column statistics.
 *
 * Returns true if the segment should be skipped, false if it might contain matching rows.
 */
bool can_prune_segment_with_predicate(const SimplePredicate& predicate, const ColumnStatValues& stats);

/**
 * Check if a segment can be pruned based on multiple predicates.
 * Returns true if ANY predicate allows pruning (since they are implicitly ANDed at the top level).
 */
bool can_prune_segment(
        const std::vector<SimplePredicate>& predicates,
        const std::function<std::optional<ColumnStatValues>(const std::string&)>& get_column_stats
);

/**
 * Compare two numeric Value objects.
 * Returns: negative if left < right, 0 if equal, positive if left > right
 * Returns std::nullopt if the values cannot be compared (e.g., incompatible types).
 */
std::optional<int> compare_values(const Value& left, const Value& right);

} // namespace arcticdb
