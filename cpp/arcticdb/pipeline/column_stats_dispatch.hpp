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
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/operation_types.hpp>
#include <arcticdb/entity/type_conversion.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/variant.hpp>

#include <memory>
#include <variant>

namespace arcticdb {

using StatsVariantData = std::variant<StatsComparison, std::shared_ptr<Value>, ColumnStatsValues>;

StatsVariantData resolve_stats_node(
        const VariantNode& node, const ExpressionContext& expression_context, const ColumnStatsRow& stats
);

StatsComparison dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsComparison compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const ColumnStatsRow& stats
);

namespace column_stats_detail {

inline bool value_is_nan(const Value& val) {
    if (is_floating_point_type(val.data_type())) {
        return details::visit_type(val.data_type(), [&val](auto tag) {
            using RawType = typename decltype(tag)::raw_type;
            if constexpr (std::is_floating_point_v<RawType>) {
                return std::isnan(val.get<RawType>());
            }
            return false;
        });
    }
    return false;
}

inline OperationType flip_comparison(const OperationType op) {
    switch (op) {
    case OperationType::GT:
        return OperationType::LT;
    case OperationType::GE:
        return OperationType::LE;
    case OperationType::LT:
        return OperationType::GT;
    case OperationType::LE:
        return OperationType::GE;
    default:
        return op;
    }
}

inline StatsComparison stats_comparator(const ColumnStatsValues& stats, const Value& val, OperationType op) {
    if (!stats.min || !stats.max) {
        return StatsComparison::UNKNOWN;
    }

    if (value_is_nan(*stats.min) || value_is_nan(*stats.max)) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats.min->data_type(), [&](auto stats_tag) -> StatsComparison {
        using StatsRawType = typename decltype(stats_tag)::raw_type;

        return details::visit_type(val.data_type(), [&](auto val_tag) -> StatsComparison {
            using ValRawType = typename decltype(val_tag)::raw_type;

            if constexpr ((is_numeric_type(decltype(stats_tag)::data_type) ||
                           is_time_type(decltype(stats_tag)::data_type)) &&
                          (is_numeric_type(decltype(val_tag)::data_type) || is_time_type(decltype(val_tag)::data_type)
                          )) {
                using comp = Comparable<StatsRawType, ValRawType>;
                auto minval = static_cast<typename comp::left_type>(stats.min->get<StatsRawType>());
                auto maxval = static_cast<typename comp::left_type>(stats.max->get<StatsRawType>());
                auto qval = static_cast<typename comp::right_type>(val.get<ValRawType>());

                switch (op) {
                case OperationType::GT:
                    if (GreaterThanOperator{}(minval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (!GreaterThanOperator{}(maxval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                case OperationType::GE:
                    if (GreaterThanEqualsOperator{}(minval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (!GreaterThanEqualsOperator{}(maxval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                case OperationType::LT:
                    if (LessThanOperator{}(maxval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (!LessThanOperator{}(minval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                case OperationType::LE:
                    if (LessThanEqualsOperator{}(maxval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (!LessThanEqualsOperator{}(minval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                case OperationType::EQ:
                    if (EqualsOperator{}(minval, qval) && EqualsOperator{}(maxval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (LessThanOperator{}(maxval, qval) || GreaterThanOperator{}(minval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                case OperationType::NE:
                    if (LessThanOperator{}(maxval, qval) || GreaterThanOperator{}(minval, qval))
                        return StatsComparison::ALL_MATCH;
                    if (EqualsOperator{}(minval, qval) && EqualsOperator{}(maxval, qval))
                        return StatsComparison::NONE_MATCH;
                    return StatsComparison::UNKNOWN;
                default:
                    return StatsComparison::UNKNOWN;
                }
            }

            return StatsComparison::UNKNOWN;
        });
    });
}

inline StatsComparison visit_binary_comparator_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType op
) {
    return std::visit(
            util::overload{
                    [op](const ColumnStatsValues& l, const std::shared_ptr<Value>& r) -> StatsComparison {
                        return stats_comparator(l, *r, op);
                    },
                    [op](const std::shared_ptr<Value>& l, const ColumnStatsValues& r) -> StatsComparison {
                        return stats_comparator(r, *l, flip_comparison(op));
                    },
                    [](const auto&, const auto&) -> StatsComparison { return StatsComparison::UNKNOWN; }
            },
            left,
            right
    );
}

} // namespace column_stats_detail

} // namespace arcticdb