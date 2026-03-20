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
        std::vector<ColumnStatsValues>>;

using StatsRowVector = std::vector<const ColumnStatsRow*>;

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
    if (!stats_lhs.min || !stats_lhs.max) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats_lhs.min->data_type(), [&](auto stats_tag) -> StatsComparison {
        using StatsTag = std::remove_reference_t<decltype(stats_tag)>;

        return details::visit_type(val_rhs.data_type(), [&](auto val_tag) -> StatsComparison {
            using ValTag = std::remove_reference_t<decltype(val_tag)>;

            constexpr bool stats_supported = is_numeric_type(StatsTag::data_type) ||
                                             is_time_type(StatsTag::data_type) || is_bool_type(StatsTag::data_type);
            constexpr bool val_supported = is_numeric_type(ValTag::data_type) || is_time_type(ValTag::data_type) ||
                                           is_bool_type(ValTag::data_type);
            check_no_mixed_bool_comparison<StatsTag::data_type, ValTag::data_type>(*stats_lhs.min, val_rhs);
            if constexpr (stats_supported && val_supported) {
                using StatsRawType = StatsTag::raw_type;
                using ValRawType = ValTag::raw_type;
                using comp = Comparable<StatsRawType, ValRawType>;
                auto min_val = static_cast<comp::left_type>(stats_lhs.min->get<StatsRawType>());
                auto max_val = static_cast<comp::left_type>(stats_lhs.max->get<StatsRawType>());
                auto query_value = static_cast<comp::right_type>(val_rhs.get<ValRawType>());
                return func(ValueRange<typename comp::left_type>{min_val, max_val}, query_value);
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

StatsComparison to_stats_comparison_for_boolean(const ColumnStatsValues& csv);
StatsComparison to_stats_comparison_for_boolean(const Value& val);

StatsComparison binary_boolean_stats(StatsComparison left, StatsComparison right, OperationType operation);

std::vector<StatsComparison> visit_binary_boolean_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsComparison unary_boolean_stats(const StatsComparison& stats_values, OperationType operation);

StatsComparison unary_boolean_stats(const ColumnStatsValues& stats_values, OperationType operation);

StatsVariantData visit_unary_boolean_stats(const StatsVariantData& left, OperationType operation);

} // namespace column_stats_detail

} // namespace arcticdb
