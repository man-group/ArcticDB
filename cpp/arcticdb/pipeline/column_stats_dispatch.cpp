/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/column_stats_dispatch.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb::column_stats_detail {

namespace {

bool either_unknown(StatsComparison left, StatsComparison right) {
    return left == StatsComparison::UNKNOWN || right == StatsComparison::UNKNOWN;
}

} // namespace

size_t stats_variant_size(const StatsVariantData& v) {
    return std::visit(
            util::overload{
                    [](const std::vector<StatsComparison>& vec) -> size_t { return vec.size(); },
                    [](const std::shared_ptr<Value>&) -> size_t { return 0; },
                    [](const std::vector<ColumnStatsValues>& vec) -> size_t { return vec.size(); }
            },
            v
    );
}

bool value_is_nan(const Value& val) {
    if (is_floating_point_type(val.data_type())) {
        return details::visit_type(val.data_type(), [&val]<typename TagType>(TagType) -> bool {
            using RawType = TagType::raw_type;
            if constexpr (std::is_floating_point_v<RawType>) {
                return std::isnan(val.get<RawType>());
            }
            return false;
        });
    }
    return false;
}

StatsComparison to_stats_comparison_for_boolean(const ColumnStatsValues& csv) {
    return unary_boolean_stats(csv, OperationType::IDENTITY);
}

StatsComparison to_stats_comparison_for_boolean(const Value& val) {
    util::check(
            is_bool_type(val.data_type()),
            "Expected bool type in to_stats_comparison_for_boolean, got {}",
            get_user_friendly_type_string(val.descriptor())
    );
    return val.get<bool>() ? StatsComparison::ALL_MATCH : StatsComparison::NONE_MATCH;
}

StatsComparison binary_boolean_stats(StatsComparison left, StatsComparison right, OperationType operation) {
    switch (operation) {
    case OperationType::AND:
        if (left == StatsComparison::NONE_MATCH || right == StatsComparison::NONE_MATCH)
            return StatsComparison::NONE_MATCH;
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        // left and right must both be ALL_MATCH
        return StatsComparison::ALL_MATCH;
    case OperationType::OR:
        if (left == StatsComparison::ALL_MATCH || right == StatsComparison::ALL_MATCH)
            return StatsComparison::ALL_MATCH;
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        // left and right must both be NONE_MATCH
        return StatsComparison::NONE_MATCH;
    case OperationType::XOR:
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        return (is_match(left) ^ is_match(right)) ? StatsComparison::ALL_MATCH : StatsComparison::NONE_MATCH;
    default:
        util::raise_rte("Unsupported operation in binary_boolean_stats - expected AND OR or XOR");
    }
}

namespace {

std::vector<StatsComparison> pairwise_binary_boolean(
        const std::vector<StatsComparison>& l, const std::vector<StatsComparison>& r, OperationType operation
) {
    util::check(
            l.size() == r.size(), "Mismatched vector sizes in pairwise_binary_boolean: {} vs {}", l.size(), r.size()
    );
    std::vector<StatsComparison> result;
    result.reserve(l.size());
    for (size_t i = 0; i < l.size(); ++i) {
        result.push_back(binary_boolean_stats(l[i], r[i], operation));
    }
    return result;
}

std::vector<StatsComparison> convert_csv_vector(const std::vector<ColumnStatsValues>& csvs) {
    std::vector<StatsComparison> result;
    result.reserve(csvs.size());
    for (const auto& csv : csvs) {
        result.push_back(to_stats_comparison_for_boolean(csv));
    }
    return result;
}

std::vector<StatsComparison> broadcast_value(StatsComparison val, size_t size) { return std::vector(size, val); }

} // namespace

std::vector<StatsComparison> visit_binary_boolean_stats(
        const StatsVariantData& left, const StatsVariantData& right, OperationType operation
) {
    return std::visit(
            util::overload{
                    // StatsComparison x StatsComparison
                    [operation](const std::vector<StatsComparison>& l, const std::vector<StatsComparison>& r)
                            -> std::vector<StatsComparison> { return pairwise_binary_boolean(l, r, operation); },
                    // StatsComparison x ColumnStatsValues
                    [operation](const std::vector<StatsComparison>& l, const std::vector<ColumnStatsValues>& r)
                            -> std::vector<StatsComparison> {
                        return pairwise_binary_boolean(l, convert_csv_vector(r), operation);
                    },
                    // ColumnStatsValues x StatsComparison
                    [operation](const std::vector<ColumnStatsValues>& l, const std::vector<StatsComparison>& r)
                            -> std::vector<StatsComparison> {
                        return pairwise_binary_boolean(convert_csv_vector(l), r, operation);
                    },
                    // ColumnStatsValues x ColumnStatsValues
                    [operation](const std::vector<ColumnStatsValues>& l, const std::vector<ColumnStatsValues>& r)
                            -> std::vector<StatsComparison> {
                        return pairwise_binary_boolean(convert_csv_vector(l), convert_csv_vector(r), operation);
                    },
                    // StatsComparison x Value
                    [operation](const std::vector<StatsComparison>& l, const std::shared_ptr<Value>& r)
                            -> std::vector<StatsComparison> {
                        return pairwise_binary_boolean(
                                l, broadcast_value(to_stats_comparison_for_boolean(*r), l.size()), operation
                        );
                    },
                    // Value x StatsComparison
                    [operation](const std::shared_ptr<Value>& l, const std::vector<StatsComparison>& r)
                            -> std::vector<StatsComparison> {
                        return pairwise_binary_boolean(
                                broadcast_value(to_stats_comparison_for_boolean(*l), r.size()), r, operation
                        );
                    },
                    // ColumnStatsValues x Value
                    [operation](const std::vector<ColumnStatsValues>& l, const std::shared_ptr<Value>& r)
                            -> std::vector<StatsComparison> {
                        auto converted_l = convert_csv_vector(l);
                        return pairwise_binary_boolean(
                                converted_l, broadcast_value(to_stats_comparison_for_boolean(*r), l.size()), operation
                        );
                    },
                    // Value x ColumnStatsValues
                    [operation](const std::shared_ptr<Value>& l, const std::vector<ColumnStatsValues>& r)
                            -> std::vector<StatsComparison> {
                        auto converted_r = convert_csv_vector(r);
                        return pairwise_binary_boolean(
                                broadcast_value(to_stats_comparison_for_boolean(*l), r.size()), converted_r, operation
                        );
                    },
                    // Value x Value
                    [](const std::shared_ptr<Value>&, const std::shared_ptr<Value>&) -> std::vector<StatsComparison> {
                        return {};
                    }
            },
            left,
            right
    );
}

StatsComparison unary_boolean_stats(const StatsComparison& stats_comparison, OperationType operation) {
    if (stats_comparison == StatsComparison::UNKNOWN) {
        return StatsComparison::UNKNOWN;
    }

    switch (operation) {
    case OperationType::IDENTITY:
        return stats_comparison;
    case OperationType::NOT:
        return stats_comparison == StatsComparison::ALL_MATCH ? StatsComparison::NONE_MATCH
                                                              : StatsComparison::ALL_MATCH;
    default:
        util::raise_rte("Unexpected operator in unary_boolean_stats {}", int(operation));
    }
}

StatsComparison unary_boolean_stats(const ColumnStatsValues& stats_values, OperationType operation) {
    if (!stats_values.min || !stats_values.max) {
        return StatsComparison::UNKNOWN;
    }
    util::check(
            is_bool_type(stats_values.min->data_type()),
            "Expect bool_types for stats in unary_boolean_stats got {}",
            get_user_friendly_type_string(stats_values.min->descriptor())
    );
    util::check(
            is_bool_type(stats_values.max->data_type()),
            "Expect bool_types for stats in unary_boolean_stats got {}",
            get_user_friendly_type_string(stats_values.max->descriptor())
    );
    bool min_val = stats_values.min->get<bool>();
    bool max_val = stats_values.max->get<bool>();
    util::check(max_val >= min_val, "Should never have min_val=true and max_val=false");
    // Cases:
    // min_val  max_val  IDENTITY   NEG
    // false    true     UNKNOWN    UNKNOWN
    // false    false    NONE_MATCH ALL_MATCH
    // true     true     ALL_MATCH  NONE_MATCH
    switch (operation) {
    case OperationType::IDENTITY:
        if (!min_val && max_val)
            return StatsComparison::UNKNOWN;
        if (!min_val || !max_val)
            return StatsComparison::NONE_MATCH;
        util::check(min_val && max_val, "Should always be true here");
        return StatsComparison::ALL_MATCH;
    case OperationType::NOT:
        if (!min_val && max_val)
            return StatsComparison::UNKNOWN;
        if (!min_val || !max_val)
            return StatsComparison::ALL_MATCH;
        util::check(min_val && max_val, "Should always be true here");
        return StatsComparison::NONE_MATCH;
    default:
        util::raise_rte("Unexpected operator in unary_boolean_stats {}", int(operation));
    }
}

StatsVariantData visit_unary_boolean_stats(const StatsVariantData& left, OperationType operation) {
    return util::variant_match(
            left,
            [operation](const std::vector<ColumnStatsValues>& l) -> std::vector<StatsComparison> {
                std::vector<StatsComparison> result;
                result.reserve(l.size());
                for (const auto& column_stats_values : l) {
                    result.emplace_back(unary_boolean_stats(column_stats_values, operation));
                }
                return result;
            },
            [operation](const std::vector<StatsComparison>& l) -> std::vector<StatsComparison> {
                std::vector<StatsComparison> result;
                result.reserve(l.size());
                for (const auto& comparisons : l) {
                    result.emplace_back(unary_boolean_stats(comparisons, operation));
                }
                return result;
            },
            [](const std::shared_ptr<Value>&) -> std::vector<StatsComparison> {
                util::raise_rte("Value should never be provided to visit_unary_boolean_stats");
            }
    );
}

} // namespace arcticdb::column_stats_detail
