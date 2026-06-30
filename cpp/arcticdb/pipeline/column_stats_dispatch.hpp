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
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
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

// One entry per index segment row: the matching ColumnStatsData row index, or nullopt when no
// stats row matches (or when the row is already pruned). Stats values are read out
// downstream via ColumnStatsData::values_for_column.
using StatsRowIndices = std::vector<std::optional<size_t>>;

StatsVariantData evaluate_ast_node_against_stats(
        const ExpressionNode& node, const StatsRowIndices& row_indices, const ColumnStatsData& column_stats
);

StatsVariantData dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

namespace column_stats_detail {

size_t stats_variant_size(const StatsVariantData& v);

template<DataType StatsType, DataType ValType>
void check_no_mixed_bool_comparison(const Value& stats_val, const Value& query_val) {
    constexpr bool stats_is_bool = is_bool_type(StatsType);
    constexpr bool val_is_bool = is_bool_type(ValType);
    if constexpr (stats_is_bool && !val_is_bool) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Invalid comparison: cannot compare bool column ({}) with non-bool value ({})",
                get_user_friendly_type_string(stats_val.descriptor()),
                get_user_friendly_type_string(query_val.descriptor())
        );
    } else if constexpr (!stats_is_bool && val_is_bool) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Invalid comparison: cannot compare non-bool column ({}) with bool value ({})",
                get_user_friendly_type_string(stats_val.descriptor()),
                get_user_friendly_type_string(query_val.descriptor())
        );
    }
}

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
    if (stats_lhs.column_absent) {
        return StatsComparison::NONE_MATCH;
    }
    if (!stats_lhs.min) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats_lhs.min->data_type(), [&](auto stats_tag) -> StatsComparison {
        using StatsTag = std::remove_reference_t<decltype(stats_tag)>;

        return details::visit_type(val_rhs.data_type(), [&](auto val_tag) -> StatsComparison {
            using ValTag = std::remove_reference_t<decltype(val_tag)>;

            // Monday: 8065794446 we should disallow comparing time types to non-time numeric types
            // This is also wrong downstream in the rest of the processing pipeline
            constexpr bool stats_supported = is_numeric_type(StatsTag::data_type) ||
                                             is_time_type(StatsTag::data_type) || is_bool_type(StatsTag::data_type);
            constexpr bool val_supported = is_numeric_type(ValTag::data_type) || is_time_type(ValTag::data_type) ||
                                           is_bool_type(ValTag::data_type);
            // Mixed bool/non-bool comparisons are rejected at runtime by check_no_mixed_bool_comparison.
            // MSVC C4804 treats `numeric > bool` as an error.
            constexpr bool bool_compatible = is_bool_type(StatsTag::data_type) == is_bool_type(ValTag::data_type);
            check_no_mixed_bool_comparison<StatsTag::data_type, ValTag::data_type>(*stats_lhs.min, val_rhs);
            if constexpr (stats_supported && val_supported && bool_compatible) {
                using StatsRawType = StatsTag::raw_type;
                using ValRawType = ValTag::raw_type;
                using comp = Comparable<StatsRawType, ValRawType>;
                auto min_val = static_cast<comp::left_type>(stats_lhs.min->get<StatsRawType>());
                auto max_val = static_cast<comp::left_type>(stats_lhs.max->get<StatsRawType>());
                auto query_value = static_cast<comp::right_type>(val_rhs.get<ValRawType>());
                if constexpr (is_time_type(StatsTag::data_type)) {
                    using F = std::remove_reference_t<Func>;
                    constexpr auto nat_result = std::is_same_v<F, NotEqualsOperator> ? StatsComparison::ALL_MATCH
                                                                                     : StatsComparison::NONE_MATCH;
                    if (auto r = check_time_stats_for_nat(
                                static_cast<timestamp>(min_val), static_cast<timestamp>(query_value), nat_result
                        )) {
                        return *r;
                    }
                }
                return func(ValueRange<typename comp::left_type>{min_val, max_val}, query_value);
            }

            return StatsComparison::UNKNOWN;
        });
    });
}

template<typename Func>
StatsComparison stats_comparator(const ColumnStatsValues& stats_lhs, const ColumnStatsValues& stats_rhs, Func&& func) {
    if (stats_lhs.column_absent || stats_rhs.column_absent) {
        return StatsComparison::NONE_MATCH;
    }
    if (!stats_lhs.min || !stats_rhs.min) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats_lhs.min->data_type(), [&](auto lhs_tag) -> StatsComparison {
        using LhsTag = std::remove_reference_t<decltype(lhs_tag)>;

        return details::visit_type(stats_rhs.min->data_type(), [&](auto rhs_tag) -> StatsComparison {
            using RhsTag = std::remove_reference_t<decltype(rhs_tag)>;

            if constexpr ((is_numeric_type(LhsTag::data_type) || is_time_type(LhsTag::data_type)) &&
                          (is_numeric_type(RhsTag::data_type) || is_time_type(RhsTag::data_type))) {
                using LhsRawType = LhsTag::raw_type;
                using RhsRawType = RhsTag::raw_type;
                using comp = Comparable<LhsRawType, RhsRawType>;
                auto lhs_min = static_cast<typename comp::left_type>(stats_lhs.min->get<LhsRawType>());
                auto lhs_max = static_cast<typename comp::left_type>(stats_lhs.max->get<LhsRawType>());
                auto rhs_min = static_cast<typename comp::right_type>(stats_rhs.min->get<RhsRawType>());
                auto rhs_max = static_cast<typename comp::right_type>(stats_rhs.max->get<RhsRawType>());
                if constexpr (is_time_type(LhsTag::data_type) || is_time_type(RhsTag::data_type)) {
                    using F = std::remove_reference_t<Func>;
                    constexpr auto nat_result = std::is_same_v<F, NotEqualsOperator> ? StatsComparison::ALL_MATCH
                                                                                     : StatsComparison::NONE_MATCH;
                    if constexpr (is_time_type(LhsTag::data_type)) {
                        if (static_cast<timestamp>(lhs_min) == NaT) {
                            return nat_result;
                        }
                    }
                    if constexpr (is_time_type(RhsTag::data_type)) {
                        if (static_cast<timestamp>(rhs_min) == NaT) {
                            return nat_result;
                        }
                    }
                }
                return func(
                        ValueRange<typename comp::left_type>{lhs_min, lhs_max},
                        ValueRange<typename comp::right_type>{rhs_min, rhs_max}
                );
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
                    [](const std::vector<ColumnStatsValues>& l,
                       const std::vector<ColumnStatsValues>& r) -> std::vector<StatsComparison> {
                        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                                l.size() == r.size(),
                                "Column stats vectors must have the same size, got {} and {}",
                                l.size(),
                                r.size()
                        );
                        std::vector<StatsComparison> result;
                        result.reserve(l.size());
                        for (size_t i = 0; i < l.size(); ++i) {
                            result.emplace_back(stats_comparator(l.at(i), r.at(i), FuncType{}));
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

StatsComparison to_stats_comparison_for_boolean(const ColumnStatsValues& csv);
StatsComparison to_stats_comparison_for_boolean(const Value& val);

StatsComparison stats_membership_comparator(const ColumnStatsValues& stats, ValueSet& value_set, OperationType op);

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
