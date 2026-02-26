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

using StatsVariantData = std::variant<
        // A result from applying column stats, like q["a"] > 5 -> StatsComparison::NONE_MATCH
        std::vector<StatsComparison>,
        // A fixed value, like 5 in q["a"] > 5
        std::shared_ptr<Value>,
        // Stats associated with a column, like q["a"] -> ColumnStatsValues for that column
        std::vector<ColumnStatsValues>>;

using StatsRowVector = std::vector<std::optional<std::reference_wrapper<const ColumnStatsRow>>>;

StatsVariantData resolve_stats_node(
        const VariantNode& node, const ExpressionContext& expression_context, const StatsRowVector& stats_rows
);

StatsVariantData dispatch_binary_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
);

StatsVariantData compute_stats(
        const ExpressionContext& expression_context, const ExpressionNode& node, const StatsRowVector& stats_rows
);

namespace column_stats_detail {

inline size_t stats_variant_size(const StatsVariantData& v) {
    return std::visit(
            util::overload{
                    [](const std::vector<StatsComparison>& vec) -> size_t { return vec.size(); },
                    [](const std::shared_ptr<Value>&) -> size_t { return 0; },
                    [](const std::vector<ColumnStatsValues>& vec) -> size_t { return vec.size(); }
            },
            v
    );
}

inline bool value_is_nan(const Value& val) {
    if (is_floating_point_type(val.data_type())) {
        return details::visit_type(val.data_type(), [&val](auto tag) {
            using RawType = decltype(tag)::raw_type;
            if constexpr (std::is_floating_point_v<RawType>) {
                return std::isnan(val.get<RawType>());
            }
            return false;
        });
    }
    return false;
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

    if (value_is_nan(*stats_lhs.min) || value_is_nan(*stats_lhs.max)) {
        return StatsComparison::UNKNOWN;
    }

    return details::visit_type(stats_lhs.min->data_type(), [&](auto stats_tag) -> StatsComparison {
        using StatsTag = std::remove_reference_t<decltype(stats_tag)>;

        return details::visit_type(val_rhs.data_type(), [&](auto val_tag) -> StatsComparison {
            using ValTag = std::remove_reference_t<decltype(val_tag)>;

            if constexpr (requires {
                              StatsTag::data_type;
                              ValTag::data_type;
                          }) {
                if constexpr ((is_numeric_type(StatsTag::data_type) || is_time_type(StatsTag::data_type)) &&
                              (is_numeric_type(ValTag::data_type) || is_time_type(ValTag::data_type))) {
                    using StatsRawType = typename StatsTag::raw_type;
                    using ValRawType = typename ValTag::raw_type;
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

StatsComparison binary_boolean_stats(StatsComparison left, StatsComparison right, OperationType operation);

inline std::vector<StatsComparison> visit_binary_boolean_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
) {
    // TODO aseaton remaining cases
    // StatsComparison & Value -> Only if Value is a bool
    // StatsComparison & ColumnStatsValues -> Only if ColumnStatsValues are a bool
    // Value & Value -> Only if Value is a bool
    // ColumnStatsValues & ColumnStatsValues -> Only if a bool
    // Value & ColumnStatsValues -> Only if a bool
    return std::visit(
            util::overload{
                    [operation](const std::vector<StatsComparison>& l, const std::vector<StatsComparison>& r)
                            -> std::vector<StatsComparison> {
                        util::check(
                                l.size() == r.size(),
                                "Mismatched vector sizes in visit_binary_boolean_stats: {} vs {}",
                                l.size(),
                                r.size()
                        );
                        std::vector<StatsComparison> result;
                        result.reserve(l.size());
                        for (size_t i = 0; i < l.size(); ++i) {
                            result.push_back(binary_boolean_stats(l.at(i), r.at(i), operation));
                        }
                        return result;
                    },
                    [&](const auto&, const auto&) -> std::vector<StatsComparison> {
                        size_t sz = std::max(stats_variant_size(left), stats_variant_size(right));
                        log::version().warn("Unsupported case in visit_binary_boolean_stats");
                        return std::vector(sz, StatsComparison::UNKNOWN);
                    }
            },
            left,
            right
    );
}

} // namespace column_stats_detail

} // namespace arcticdb
