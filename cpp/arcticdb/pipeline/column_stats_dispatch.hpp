/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/column_stats_filter.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/entity/type_conversion.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/variant.hpp>

#include <memory>
#include <variant>

namespace arcticdb {

using StatsVariantData = std::variant<
        // A result from applying column stats, like q["a"] > 5 -> StatsComparison::NONE_MATCH
        std::vector<StatsComparison>,
        // A fixed value, like 5 in q["a"] > 5
        std::shared_ptr<Value>,
        // Stats associated with a column, like q["a"] -> ColumnStatsValues for that column
        std::vector<ColumnStatsValues>,
        // A value set from an isin/isnotin expression
        std::shared_ptr<ValueSet>>;

using StatsRowVector = std::vector<std::optional<std::reference_wrapper<const ColumnStatsRow>>>;

StatsVariantData evaluate_ast_node_against_stats(
        const VariantNode& node, const ExpressionContext& expression_context, const StatsRowVector& stats_rows
);

StatsVariantData dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsVariantData compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const StatsRowVector& stats_rows
);

namespace column_stats_detail {

size_t stats_variant_size(const StatsVariantData& v);

bool value_is_nan(const Value& val);

template<typename Func>
struct FlippedComparator;

template<>
struct FlippedComparator<GreaterThanOperator> {
    using type = LessThanOperator;
};
template<>
struct FlippedComparator<GreaterThanEqualsOperator> {
    using type = LessThanEqualsOperator;
};
template<>
struct FlippedComparator<LessThanOperator> {
    using type = GreaterThanOperator;
};
template<>
struct FlippedComparator<LessThanEqualsOperator> {
    using type = GreaterThanEqualsOperator;
};
template<>
struct FlippedComparator<EqualsOperator> {
    using type = EqualsOperator;
};
template<>
struct FlippedComparator<NotEqualsOperator> {
    using type = NotEqualsOperator;
};

template<typename Func>
StatsComparison stats_comparator(const ColumnStatsValues& stats_lhs, const Value& val_rhs, Func&& func) {
    if (!stats_lhs.min || !stats_lhs.max) {
        return StatsComparison::UNKNOWN;
    }

    if (value_is_nan(*stats_lhs.min) || value_is_nan(*stats_lhs.max)) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats_lhs.min->data_type(), [&](auto stats_tag) -> StatsComparison {
        using StatsTag = std::remove_reference_t<decltype(stats_tag)>;

        return details::visit_type(val_rhs.data_type(), [&](auto val_tag) -> StatsComparison {
            using ValTag = std::remove_reference_t<decltype(val_tag)>;

            // bools are disabled in this part of the grammar at the moment, Monday: 11292565671
            if constexpr (requires {
                              StatsTag::data_type;
                              ValTag::data_type;
                          }) {
                if constexpr ((is_numeric_type(StatsTag::data_type) || is_time_type(StatsTag::data_type)) &&
                              (is_numeric_type(ValTag::data_type) || is_time_type(ValTag::data_type))) {
                    using StatsRawType = StatsTag::raw_type;
                    using ValRawType = ValTag::raw_type;
                    using comp = Comparable<StatsRawType, ValRawType>;
                    auto min_val = static_cast<comp::left_type>(stats_lhs.min->get<StatsRawType>());
                    auto max_val = static_cast<comp::left_type>(stats_lhs.max->get<StatsRawType>());
                    auto query_value = static_cast<comp::right_type>(val_rhs.get<ValRawType>());
                    return func(ValueRange<typename comp::left_type>{min_val, max_val}, query_value);
                }
            }

            return StatsComparison::UNKNOWN;
        });
    });
}

template<typename Func>
std::vector<StatsComparison> visit_binary_comparator_stats(
        const StatsVariantData& left, const StatsVariantData& right
) {
    using FuncType = std::remove_reference_t<Func>;
    using FlippedType = FlippedComparator<FuncType>::type;
    return std::visit(
            util::overload{
                    [](const std::vector<ColumnStatsValues>& l,
                       const std::shared_ptr<Value>& r) -> std::vector<StatsComparison> {
                        std::vector<StatsComparison> result;
                        result.reserve(l.size());
                        for (const auto& column_stats_values : l) {
                            result.emplace_back(stats_comparator(column_stats_values, *r, FuncType{}));
                        }
                        return result;
                    },
                    [](const std::shared_ptr<Value>& l,
                       const std::vector<ColumnStatsValues>& r) -> std::vector<StatsComparison> {
                        std::vector<StatsComparison> result;
                        result.reserve(r.size());
                        for (const auto& column_stats_values : r) {
                            result.emplace_back(stats_comparator(column_stats_values, *l, FlippedType{}));
                        }
                        return result;
                    },
                    [&left, &right](const auto&, const auto&) -> std::vector<StatsComparison> {
                        size_t sz = std::max(stats_variant_size(left), stats_variant_size(right));
                        return std::vector(sz, StatsComparison::UNKNOWN);
                    }
            },
            left,
            right
    );
}

StatsComparison stats_membership_comparator(
        const ColumnStatsValues& stats, const ValueSet& value_set, OperationType op
);

std::vector<StatsComparison> visit_binary_membership_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsComparison binary_boolean_stats(StatsComparison left, StatsComparison right, OperationType operation);

std::vector<StatsComparison> visit_binary_boolean_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsComparison unary_boolean_stats(const StatsComparison& stats_values, OperationType operation);

StatsComparison unary_boolean_stats(const ColumnStatsValues& stats_values, OperationType operation);

StatsVariantData visit_unary_boolean_stats(const StatsVariantData& left, OperationType operation);

} // namespace column_stats_detail

} // namespace arcticdb
