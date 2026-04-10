#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/pipeline/column_stats_filter.hpp>

#include <cmath>
#include <limits>

using namespace arcticdb;
using namespace arcticdb::column_stats_detail;

namespace {

template<typename T>
std::shared_ptr<ValueSet> make_numeric_value_set(std::initializer_list<T> values) {
    auto set = std::make_shared<std::unordered_set<T>>(values);
    return std::make_shared<ValueSet>(NumericSetType{std::move(set)});
}

template<typename T>
std::shared_ptr<ValueSet> make_empty_value_set() {
    auto set = std::make_shared<std::unordered_set<T>>();
    return std::make_shared<ValueSet>(NumericSetType{std::move(set)});
}

} // namespace

TEST(StatsMembershipComparator, EmptySetIsinNoneMatch) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{10})}
    };
    auto vs = make_empty_value_set<int64_t>();
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::NONE_MATCH);
}

TEST(StatsMembershipComparator, EmptySetIsnotinAllMatch) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{10})}
    };
    auto vs = make_empty_value_set<int64_t>();
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetBelowBlockRange) {
    // Block [10, 20], set {1, 5} -> set_max=5 < block_min=10 -> NONE_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{10})}, std::optional<Value>{construct_value(int64_t{20})}
    };
    auto vs = make_numeric_value_set<int64_t>({1, 5});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::NONE_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetAboveBlockRange) {
    // Block [1, 5], set {10, 20} -> set_min=10 > block_max=5 -> NONE_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{5})}
    };
    auto vs = make_numeric_value_set<int64_t>({10, 20});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::NONE_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetOverlapsBlockRange) {
    // Block [1, 10], set {5, 15} -> overlaps -> UNKNOWN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{10})}
    };
    auto vs = make_numeric_value_set<int64_t>({5, 15});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::UNKNOWN);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::UNKNOWN);
}

TEST(StatsMembershipComparator, SingleElementMatchesSingleValueBlock) {
    // Block [5, 5], set {5} -> ALL_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{5})}, std::optional<Value>{construct_value(int64_t{5})}
    };
    auto vs = make_numeric_value_set<int64_t>({5});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::ALL_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::NONE_MATCH);
}

TEST(StatsMembershipComparator, MixedTypesInt32StatsDoubleSet) {
    // Block [1, 10] as int32, set {15.0, 20.0} as double -> set_min=15 > block_max=10 -> NONE_MATCH
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int32_t{1})}, std::optional<Value>{construct_value(int32_t{10})}
    };
    auto vs = make_numeric_value_set<double>({15.0, 20.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::NONE_MATCH);
}

TEST(StatsMembershipComparator, NaNInStatsMin) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())},
            std::optional<Value>{construct_value(10.0)}
    };
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::UNKNOWN);
}

TEST(StatsMembershipComparator, NaNInStatsMax) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(1.0)},
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::UNKNOWN);
}

TEST(StatsMembershipComparator, MissingStats) {
    ColumnStatsValues csv{std::nullopt, std::nullopt};
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::UNKNOWN);
}

TEST(StatsMembershipComparator, SetContainedWithinBlockRange) {
    // Block [1, 100], set {3, 7} -> overlap -> UNKNOWN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{100})}
    };
    auto vs = make_numeric_value_set<int64_t>({3, 7});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), StatsComparison::UNKNOWN);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), StatsComparison::UNKNOWN);
}

// Per-element pruning tests: the set's min/max range overlaps the block range,
// but individual element iteration can still prune.
class StatsMembershipPerElementInt64Test
    : public ::testing::TestWithParam<
              std::tuple<int64_t, int64_t, std::vector<int64_t>, StatsComparison, StatsComparison>> {};

TEST_P(StatsMembershipPerElementInt64Test, IsinAndIsnotin) {
    auto [block_min, block_max, set_values, expected_isin, expected_isnotin] = GetParam();
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(block_min)}, std::optional<Value>{construct_value(block_max)}
    };
    auto vs = std::make_shared<ValueSet>(
            NumericSetType{std::make_shared<std::unordered_set<int64_t>>(set_values.begin(), set_values.end())}
    );
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), expected_isin);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), expected_isnotin);
}

INSTANTIATE_TEST_SUITE_P(
        RangeOverlapsAllOutside, StatsMembershipPerElementInt64Test,
        ::testing::Values(std::make_tuple(
                int64_t{5}, int64_t{10}, std::vector<int64_t>{1, 3, 15}, StatsComparison::NONE_MATCH,
                StatsComparison::ALL_MATCH
        ))
);

INSTANTIATE_TEST_SUITE_P(
        RangeOverlapsSomeInside, StatsMembershipPerElementInt64Test,
        ::testing::Values(std::make_tuple(
                int64_t{5}, int64_t{10}, std::vector<int64_t>{1, 7, 15}, StatsComparison::UNKNOWN,
                StatsComparison::UNKNOWN
        ))
);

INSTANTIATE_TEST_SUITE_P(
        SingleValueBlockHit, StatsMembershipPerElementInt64Test,
        ::testing::Values(std::make_tuple(
                int64_t{5}, int64_t{5}, std::vector<int64_t>{3, 5, 8}, StatsComparison::ALL_MATCH,
                StatsComparison::NONE_MATCH
        ))
);

INSTANTIATE_TEST_SUITE_P(
        SingleValueBlockMiss, StatsMembershipPerElementInt64Test,
        ::testing::Values(std::make_tuple(
                int64_t{5}, int64_t{5}, std::vector<int64_t>{3, 8, 12}, StatsComparison::NONE_MATCH,
                StatsComparison::ALL_MATCH
        ))
);

class StatsMembershipNaNTest
    : public ::testing::TestWithParam<
              std::tuple<double, double, std::vector<double>, StatsComparison, StatsComparison>> {};

TEST_P(StatsMembershipNaNTest, IsinAndIsnotin) {
    auto [block_min, block_max, set_values, expected_isin, expected_isnotin] = GetParam();
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(block_min)}, std::optional<Value>{construct_value(block_max)}
    };
    auto vs = std::make_shared<ValueSet>(
            NumericSetType{std::make_shared<std::unordered_set<double>>(set_values.begin(), set_values.end())}
    );
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), expected_isin);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), expected_isnotin);
}

INSTANTIATE_TEST_SUITE_P(
        NaNInSetNonNaNOutside, StatsMembershipNaNTest,
        ::testing::Values(std::make_tuple(
                1.0, 10.0, std::vector<double>{std::numeric_limits<double>::quiet_NaN(), 20.0},
                StatsComparison::NONE_MATCH, StatsComparison::ALL_MATCH
        ))
);

INSTANTIATE_TEST_SUITE_P(
        NaNInSetNonNaNInside, StatsMembershipNaNTest,
        ::testing::Values(std::make_tuple(
                1.0, 10.0, std::vector<double>{std::numeric_limits<double>::quiet_NaN(), 5.0}, StatsComparison::UNKNOWN,
                StatsComparison::UNKNOWN
        ))
);

INSTANTIATE_TEST_SUITE_P(
        BothStatsNaNWithNaNInSet, StatsMembershipNaNTest,
        ::testing::Values(std::make_tuple(
                std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                std::vector<double>{std::numeric_limits<double>::quiet_NaN(), 5.0}, StatsComparison::NONE_MATCH,
                StatsComparison::ALL_MATCH
        ))
);

INSTANTIATE_TEST_SUITE_P(
        BothStatsNaNNoNaNInSet, StatsMembershipNaNTest,
        ::testing::Values(std::make_tuple(
                std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN(),
                std::vector<double>{5.0, 20.0}, StatsComparison::NONE_MATCH, StatsComparison::ALL_MATCH
        ))
);

INSTANTIATE_TEST_SUITE_P(
        AllNaNValueSet, StatsMembershipNaNTest,
        ::testing::Values(std::make_tuple(
                1.0, 10.0, std::vector<double>{std::numeric_limits<double>::quiet_NaN()}, StatsComparison::NONE_MATCH,
                StatsComparison::ALL_MATCH
        ))
);
