/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>
#include <arcticdb/pipeline/column_stats_filter.hpp>

#include <cmath>
#include <limits>

using namespace arcticdb;
using namespace arcticdb::column_stats_detail;

template<typename LhsType, typename RhsType, typename Func>
StatsComparison typed_range_stats_comparator(
        LhsType lhs_min, LhsType lhs_max, RhsType rhs_min, RhsType rhs_max, Func&& func
) {
    ColumnStatsValues lhs{construct_value(lhs_min), construct_value(lhs_max)};
    ColumnStatsValues rhs{construct_value(rhs_min), construct_value(rhs_max)};
    return stats_comparator(lhs, rhs, std::forward<Func>(func));
}

template<typename LhsType, typename RhsType>
StatsComparison dispatch_range_vs_range(
        LhsType lhs_min, LhsType lhs_max, RhsType rhs_min, RhsType rhs_max, OperationType op
) {
    std::vector<ColumnStatsValues> lhs{{construct_value(lhs_min), construct_value(lhs_max)}};
    std::vector<ColumnStatsValues> rhs{{construct_value(rhs_min), construct_value(rhs_max)}};
    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(lhs, rhs, op));
    EXPECT_EQ(result.size(), 1);
    return result.at(0);
}

TEST(MixedTypeRangeVsRange, Int32LhsDoubleRhs) {
    // [1, 10] < [10.5, 20.0] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{1}, int32_t{10}, 10.5, 20.0, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [1, 10] < [5.0, 8.0] -> UNKNOWN (overlapping ranges)
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{1}, int32_t{10}, 5.0, 8.0, LessThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // [1, 10] < [0.5, 0.9] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{1}, int32_t{10}, 0.5, 0.9, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [5, 5] == [5.0, 5.0] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{5}, int32_t{5}, 5.0, 5.0, EqualsOperator{}), StatsComparison::ALL_MATCH
    );
    // [5, 5] == [5.5, 5.5] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{5}, int32_t{5}, 5.5, 5.5, EqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [1, 10] != [20.0, 30.0] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int32_t{1}, int32_t{10}, 20.0, 30.0, NotEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
}

TEST(MixedTypeRangeVsRange, FloatLhsInt64Rhs) {
    // [1.0f, 10.0f] > [11, 20] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(1.0f, 10.0f, int64_t{11}, int64_t{20}, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [1.0f, 10.0f] > [-5, -1] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(1.0f, 10.0f, int64_t{-5}, int64_t{-1}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [1.0f, 10.0f] >= [10, 10] -> UNKNOWN (overlapping at boundary)
    ASSERT_EQ(
            typed_range_stats_comparator(1.0f, 10.0f, int64_t{10}, int64_t{10}, GreaterThanEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeRangeVsRange, DoubleLhsInt32Rhs) {
    // [1.0, 10.0] <= [10, 20] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(1.0, 10.0, int32_t{10}, int32_t{20}, LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [1.0, 10.0] <= [11, 20] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(1.0, 10.0, int32_t{11}, int32_t{20}, LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [15.0, 20.0] <= [1, 10] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(15.0, 20.0, int32_t{1}, int32_t{10}, LessThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [1.0, 10.0] <= [5, 15] -> UNKNOWN (ranges overlap)
    ASSERT_EQ(
            typed_range_stats_comparator(1.0, 10.0, int32_t{5}, int32_t{15}, LessThanEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
}

// uint32_t lhs range, int64_t rhs range
TEST(MixedTypeRangeVsRange, UInt32LhsInt64Rhs) {
    // [0, 100] < [-10, -1] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(uint32_t{0}, uint32_t{100}, int64_t{-10}, int64_t{-1}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [0, 100] > [-10, -1] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(uint32_t{0}, uint32_t{100}, int64_t{-10}, int64_t{-1}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [0, 100] > [-10, 50] -> UNKNOWN (ranges overlap)
    ASSERT_EQ(
            typed_range_stats_comparator(uint32_t{0}, uint32_t{100}, int64_t{-10}, int64_t{50}, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
}

// int64_t lhs range, uint64_t rhs range (signed/unsigned boundary)
TEST(MixedTypeRangeVsRange, Int64LhsUInt64Rhs) {
    // [-5, 5] < [6, 10] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{-5}, int64_t{5}, uint64_t{6}, uint64_t{10}, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [-5, 5] < [3, 10] -> UNKNOWN (overlapping)
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{-5}, int64_t{5}, uint64_t{3}, uint64_t{10}, LessThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // [10, 20] > [0, 5] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{10}, int64_t{20}, uint64_t{0}, uint64_t{5}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [-100, -1] < [0, 10] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{-100}, int64_t{-1}, uint64_t{0}, uint64_t{10}, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [10, 20] < [0, 5] -> NONE_MATCH (min_lhs=10 >= max_rhs=5)
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{10}, int64_t{20}, uint64_t{0}, uint64_t{5}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
}

// uint64_t lhs range, int64_t rhs range
TEST(MixedTypeRangeVsRange, UInt64LhsInt64Rhs) {
    // [0, 5] < [-10, -1] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t{0}, uint64_t{5}, int64_t{-10}, int64_t{-1}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [0, 5] > [10, 20] -> NONE_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t{0}, uint64_t{5}, int64_t{10}, int64_t{20}, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [10, 20] == [10, 20] -> UNKNOWN
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t{10}, uint64_t{20}, int64_t{10}, int64_t{20}, EqualsOperator{}),
            StatsComparison::UNKNOWN
    );
    // [5, 5] == [5, 5] -> ALL_MATCH
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t{5}, uint64_t{5}, int64_t{5}, int64_t{5}, EqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
}

// int64_t lhs range, uint64_t rhs range where rhs has MSB set
TEST(MixedTypeRangeVsRange, Int64LhsUInt64MsbRhs) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Any int64 range is less than a uint64 range with MSB set
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{-100}, int64_t{100}, MSB, MSB, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // No int64 is greater than a uint64 with MSB set
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, GreaterThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // No int64 can equal a uint64 with MSB set
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, EqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, MSB, U64_MAX, NotEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // int64 lhs vs uint64 rhs spanning the MSB boundary
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX - 1), MSB, LessThanOperator{}),
            StatsComparison::UNKNOWN
    );
    ASSERT_EQ(
            typed_range_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX - 1), MSB, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // int64 point at INT64_MAX vs uint64 point at INT64_MAX
    ASSERT_EQ(
            typed_range_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), uint64_t(I64_MAX), EqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), uint64_t(I64_MAX), NotEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
}

TEST(MixedTypeRangeVsRange, UInt64MsbLhsInt64Rhs) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto I64_MIN = std::numeric_limits<int64_t>::min();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // uint64 with MSB set is always greater than any int64
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, int64_t{0}, I64_MAX, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, MSB, I64_MIN, I64_MAX, GreaterThanOperator{}), StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, int64_t{0}, I64_MAX, GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // uint64 with MSB set is never less than any int64
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, int64_t{0}, I64_MAX, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, int64_t{0}, I64_MAX, LessThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // No uint64 with MSB set can equal any int64
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, I64_MIN, I64_MAX, EqualsOperator{}), StatsComparison::NONE_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(MSB, U64_MAX, I64_MIN, I64_MAX, NotEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // uint64 range spanning MSB boundary vs int64 range
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t(I64_MAX - 1), MSB, int64_t{0}, I64_MAX, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t(I64_MAX - 1), MSB, int64_t{0}, I64_MAX, LessThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // uint64 range spanning MSB boundary vs negative int64 range
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t(I64_MAX - 1), MSB, int64_t{-100}, int64_t{-1}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_range_stats_comparator(uint64_t(I64_MAX - 1), MSB, int64_t{-100}, int64_t{-1}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
}

class RangeVsRangeInfinityTest
    : public ::testing::TestWithParam<std::tuple<double, double, double, double, OperationType, StatsComparison>> {};

TEST_P(RangeVsRangeInfinityTest, AllCases) {
    auto [lhs_min, lhs_max, rhs_min, rhs_max, op, expected] = GetParam();
    ASSERT_EQ(dispatch_range_vs_range(lhs_min, lhs_max, rhs_min, rhs_max, op), expected);
}

// Finite lhs, rhs contains +inf
INSTANTIATE_TEST_SUITE_P(
        FiniteLhsPositiveInfRhs, RangeVsRangeInfinityTest,
        ::testing::Values(
                // [1, 10] < [100, +inf] -> ALL_MATCH
                std::make_tuple(
                        1.0, 10.0, 100.0, std::numeric_limits<double>::infinity(), OperationType::LT,
                        StatsComparison::ALL_MATCH
                ),
                // [1, 10] < [5, +inf] -> UNKNOWN
                std::make_tuple(
                        1.0, 10.0, 5.0, std::numeric_limits<double>::infinity(), OperationType::LT,
                        StatsComparison::UNKNOWN
                ),
                // [1, 10] > [5, +inf] -> UNKNOWN
                std::make_tuple(
                        1.0, 10.0, 5.0, std::numeric_limits<double>::infinity(), OperationType::GT,
                        StatsComparison::UNKNOWN
                ),
                // [1, 10] > [100, +inf] -> NONE_MATCH
                std::make_tuple(
                        1.0, 10.0, 100.0, std::numeric_limits<double>::infinity(), OperationType::GT,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] == [100, +inf] -> NONE_MATCH
                std::make_tuple(
                        1.0, 10.0, 100.0, std::numeric_limits<double>::infinity(), OperationType::EQ,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] != [100, +inf] -> ALL_MATCH
                std::make_tuple(
                        1.0, 10.0, 100.0, std::numeric_limits<double>::infinity(), OperationType::NE,
                        StatsComparison::ALL_MATCH
                )
        )
);

// Finite lhs, rhs contains -inf
INSTANTIATE_TEST_SUITE_P(
        FiniteLhsNegativeInfRhs, RangeVsRangeInfinityTest,
        ::testing::Values(
                // [1, 10] > [-inf, -5] -> ALL_MATCH
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), -5.0, OperationType::GT,
                        StatsComparison::ALL_MATCH
                ),
                // [1, 10] > [-inf, 5] -> UNKNOWN
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), 5.0, OperationType::GT,
                        StatsComparison::UNKNOWN
                ),
                // [1, 10] < [-inf, -5] -> NONE_MATCH
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), -5.0, OperationType::LT,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] == [-inf, -5] -> NONE_MATCH
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), -5.0, OperationType::EQ,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] != [-inf, -5] -> ALL_MATCH
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), -5.0, OperationType::NE,
                        StatsComparison::ALL_MATCH
                )
        )
);

// Both ranges contain infinity
INSTANTIATE_TEST_SUITE_P(
        BothRangesInfinity, RangeVsRangeInfinityTest,
        ::testing::Values(
                // [+inf, +inf] == [+inf, +inf] -> ALL_MATCH
                std::make_tuple(
                        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        OperationType::EQ, StatsComparison::ALL_MATCH
                ),
                // [+inf, +inf] < [+inf, +inf] -> NONE_MATCH
                std::make_tuple(
                        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        OperationType::LT, StatsComparison::NONE_MATCH
                ),
                // [-inf, -inf] < [+inf, +inf] -> ALL_MATCH
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        OperationType::LT, StatsComparison::ALL_MATCH
                ),
                // [-inf, +inf] < [-inf, +inf] -> UNKNOWN
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        OperationType::LT, StatsComparison::UNKNOWN
                ),
                // [-inf, 0] < [1, +inf] -> ALL_MATCH
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 0.0, 1.0, std::numeric_limits<double>::infinity(),
                        OperationType::LT, StatsComparison::ALL_MATCH
                ),
                // [-inf, 0] > [1, +inf] -> NONE_MATCH
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 0.0, 1.0, std::numeric_limits<double>::infinity(),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                // [-inf, 20] > [10, +inf] -> UNKNOWN
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 20.0, 10.0, std::numeric_limits<double>::infinity(),
                        OperationType::GT, StatsComparison::UNKNOWN
                )
        )
);

// One range is NaN, other is finite
INSTANTIATE_TEST_SUITE_P(
        NaNRangeVsFiniteRange, RangeVsRangeInfinityTest,
        ::testing::Values(
                // [NaN, NaN] < [1, 10] -> NONE_MATCH
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::LT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::LE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                // [NaN, NaN] != [1, 10] -> ALL_MATCH
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(), 1.0, 10.0,
                        OperationType::NE, StatsComparison::ALL_MATCH
                ),
                // [1, 10] < [NaN, NaN] -> NONE_MATCH
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::LT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::LE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                // [1, 10] != [NaN, NaN] -> ALL_MATCH
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::NE, StatsComparison::ALL_MATCH
                ),
                // [NaN, NaN] == [NaN, NaN] -> NONE_MATCH (NaN != NaN)
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                // [NaN, NaN] != [NaN, NaN] -> ALL_MATCH
                std::make_tuple(
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                        OperationType::NE, StatsComparison::ALL_MATCH
                )
        )
);

class Float32NaNRangeVsFloat64RangeTest : public ::testing::TestWithParam<std::tuple<OperationType, StatsComparison>> {
};

TEST_P(Float32NaNRangeVsFloat64RangeTest, AllOps) {
    auto [op, expected] = GetParam();
    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    std::vector<ColumnStatsValues> lhs{{construct_value(nan_f), construct_value(nan_f)}};
    std::vector<ColumnStatsValues> rhs{{construct_value(5.0), construct_value(10.0)}}; // FLOAT64

    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(lhs, rhs, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), expected);
}

// Float32 NaN lhs, Float64 finite rhs
INSTANTIATE_TEST_SUITE_P(
        Float32NaNLhsFloat64Rhs, Float32NaNRangeVsFloat64RangeTest,
        ::testing::Values(
                std::make_tuple(OperationType::LT, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::LE, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::GE, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::EQ, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::NE, StatsComparison::ALL_MATCH)
        )
);

class Float64RangeVsFloat32NaNRangeTest : public ::testing::TestWithParam<std::tuple<OperationType, StatsComparison>> {
};

TEST_P(Float64RangeVsFloat32NaNRangeTest, AllOps) {
    auto [op, expected] = GetParam();
    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    std::vector<ColumnStatsValues> lhs{{construct_value(5.0), construct_value(10.0)}}; // FLOAT64
    std::vector<ColumnStatsValues> rhs{{construct_value(nan_f), construct_value(nan_f)}};

    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(lhs, rhs, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), expected);
}

// Float64 finite lhs, Float32 NaN rhs
INSTANTIATE_TEST_SUITE_P(
        Float64LhsFloat32NaNRhs, Float64RangeVsFloat32NaNRangeTest,
        ::testing::Values(
                std::make_tuple(OperationType::LT, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::LE, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::GE, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::EQ, StatsComparison::NONE_MATCH),
                std::make_tuple(OperationType::NE, StatsComparison::ALL_MATCH)
        )
);
