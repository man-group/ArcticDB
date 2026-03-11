/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/column_stats_dispatch.hpp>

namespace arcticdb::column_stats_detail {

namespace {

bool either_unknown(StatsComparison left, StatsComparison right) {
    return left == StatsComparison::UNKNOWN || right == StatsComparison::UNKNOWN;
}

} // namespace

StatsComparison binary_boolean_stats(StatsComparison left, StatsComparison right, OperationType operation) {
    switch (operation) {
    case OperationType::AND:
        if (left == StatsComparison::NONE_MATCH || right == StatsComparison::NONE_MATCH)
            return StatsComparison::NONE_MATCH;
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        return (is_match(left) && is_match(right)) ? StatsComparison::ALL_MATCH : StatsComparison::NONE_MATCH;
    case OperationType::OR:
        if (left == StatsComparison::ALL_MATCH || right == StatsComparison::ALL_MATCH)
            return StatsComparison::ALL_MATCH;
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        return (is_match(left) || is_match(right)) ? StatsComparison::ALL_MATCH : StatsComparison::NONE_MATCH;
    case OperationType::XOR:
        if (either_unknown(left, right))
            return StatsComparison::UNKNOWN;
        return (is_match(left) ^ is_match(right)) ? StatsComparison::ALL_MATCH : StatsComparison::NONE_MATCH;
    default:
        util::raise_rte("Unsupported operation in binary_boolean_stats - expected AND OR or XOR");
    }
}

} // namespace arcticdb::column_stats_detail