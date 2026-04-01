#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>

#include <cmath>
#include <limits>

using namespace arcticdb;
using namespace arcticdb::column_stats_detail;
using SC = StatsComparison;

class BinaryBooleanStatsTest : public ::testing::TestWithParam<std::tuple<SC, SC, OperationType, SC>> {};

TEST_P(BinaryBooleanStatsTest, AllCombinations) {
    auto [left, right, op, expected] = GetParam();
    ASSERT_EQ(binary_boolean_stats(left, right, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        AND, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(SC::ALL_MATCH, SC::ALL_MATCH, OperationType::AND, SC::ALL_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::NONE_MATCH, OperationType::AND, SC::NONE_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::UNKNOWN, OperationType::AND, SC::UNKNOWN),
                std::make_tuple(SC::NONE_MATCH, SC::ALL_MATCH, OperationType::AND, SC::NONE_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::NONE_MATCH, OperationType::AND, SC::NONE_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::UNKNOWN, OperationType::AND, SC::NONE_MATCH),
                std::make_tuple(SC::UNKNOWN, SC::ALL_MATCH, OperationType::AND, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::NONE_MATCH, OperationType::AND, SC::NONE_MATCH),
                std::make_tuple(SC::UNKNOWN, SC::UNKNOWN, OperationType::AND, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        OR, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(SC::ALL_MATCH, SC::ALL_MATCH, OperationType::OR, SC::ALL_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::NONE_MATCH, OperationType::OR, SC::ALL_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::UNKNOWN, OperationType::OR, SC::ALL_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::ALL_MATCH, OperationType::OR, SC::ALL_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::NONE_MATCH, OperationType::OR, SC::NONE_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::UNKNOWN, OperationType::OR, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::ALL_MATCH, OperationType::OR, SC::ALL_MATCH),
                std::make_tuple(SC::UNKNOWN, SC::NONE_MATCH, OperationType::OR, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::UNKNOWN, OperationType::OR, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        XOR, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(SC::ALL_MATCH, SC::ALL_MATCH, OperationType::XOR, SC::NONE_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::NONE_MATCH, OperationType::XOR, SC::ALL_MATCH),
                std::make_tuple(SC::ALL_MATCH, SC::UNKNOWN, OperationType::XOR, SC::UNKNOWN),
                std::make_tuple(SC::NONE_MATCH, SC::ALL_MATCH, OperationType::XOR, SC::ALL_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::NONE_MATCH, OperationType::XOR, SC::NONE_MATCH),
                std::make_tuple(SC::NONE_MATCH, SC::UNKNOWN, OperationType::XOR, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::ALL_MATCH, OperationType::XOR, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::NONE_MATCH, OperationType::XOR, SC::UNKNOWN),
                std::make_tuple(SC::UNKNOWN, SC::UNKNOWN, OperationType::XOR, SC::UNKNOWN)
        )
);

// Tests for unary_boolean_stats(StatsComparison, OperationType)
class UnaryBooleanStatsFromComparisonTest : public ::testing::TestWithParam<std::tuple<SC, OperationType, SC>> {};

TEST_P(UnaryBooleanStatsFromComparisonTest, AllCombinations) {
    auto [input, op, expected] = GetParam();
    ASSERT_EQ(unary_boolean_stats(input, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        IDENTITY, UnaryBooleanStatsFromComparisonTest,
        ::testing::Values(
                std::make_tuple(SC::ALL_MATCH, OperationType::IDENTITY, SC::ALL_MATCH),
                std::make_tuple(SC::NONE_MATCH, OperationType::IDENTITY, SC::NONE_MATCH),
                std::make_tuple(SC::UNKNOWN, OperationType::IDENTITY, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        NOT, UnaryBooleanStatsFromComparisonTest,
        ::testing::Values(
                std::make_tuple(SC::ALL_MATCH, OperationType::NOT, SC::NONE_MATCH),
                std::make_tuple(SC::NONE_MATCH, OperationType::NOT, SC::ALL_MATCH),
                std::make_tuple(SC::UNKNOWN, OperationType::NOT, SC::UNKNOWN)
        )
);

// Tests for unary_boolean_stats(ColumnStatsValues, OperationType)
// Parameters: (min: optional<bool>, max: optional<bool>, op, expected)
class UnaryBooleanStatsFromColumnStatsTest
    : public ::testing::TestWithParam<std::tuple<std::optional<bool>, std::optional<bool>, OperationType, SC>> {};

TEST_P(UnaryBooleanStatsFromColumnStatsTest, AllCombinations) {
    auto [min_opt, max_opt, op, expected] = GetParam();
    ColumnStatsValues stats{
            min_opt.has_value() ? std::optional<Value>{construct_value(*min_opt)} : std::nullopt,
            max_opt.has_value() ? std::optional<Value>{construct_value(*max_opt)} : std::nullopt
    };
    ASSERT_EQ(unary_boolean_stats(stats, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        MissingStats, UnaryBooleanStatsFromColumnStatsTest,
        ::testing::Values(
                std::make_tuple(std::optional<bool>{}, std::optional<bool>{}, OperationType::IDENTITY, SC::UNKNOWN),
                std::make_tuple(std::optional<bool>{}, std::optional<bool>{}, OperationType::NOT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        IDENTITY, UnaryBooleanStatsFromColumnStatsTest,
        ::testing::Values(
                std::make_tuple(std::optional{false}, std::optional{true}, OperationType::IDENTITY, SC::UNKNOWN),
                std::make_tuple(std::optional{false}, std::optional{false}, OperationType::IDENTITY, SC::NONE_MATCH),
                std::make_tuple(std::optional{true}, std::optional{true}, OperationType::IDENTITY, SC::ALL_MATCH)
                // min=true, max=false case is impossible
        )
);

INSTANTIATE_TEST_SUITE_P(
        NOT, UnaryBooleanStatsFromColumnStatsTest,
        ::testing::Values(
                std::make_tuple(std::optional{false}, std::optional{true}, OperationType::NOT, SC::UNKNOWN),
                std::make_tuple(std::optional{false}, std::optional{false}, OperationType::NOT, SC::ALL_MATCH),
                std::make_tuple(std::optional{true}, std::optional{true}, OperationType::NOT, SC::NONE_MATCH)
                // min=true, max=false case is impossible
        )
);

// Parameters: (min, max, query_value, op, expected)
class BinaryComparisonTest
    : public ::testing::TestWithParam<
              std::tuple<std::optional<int64_t>, std::optional<int64_t>, int64_t, OperationType, SC>> {};

TEST_P(BinaryComparisonTest, AllCases) {
    auto [min, max, value, op, expected] = GetParam();
    std::optional<Value> min_value = min.has_value() ? std::make_optional(Value(*min, DataType::INT64)) : std::nullopt;
    std::optional<Value> max_value = max.has_value() ? std::make_optional(Value(*max, DataType::INT64)) : std::nullopt;
    std::vector<ColumnStatsValues> column_stats_values{{min_value, max_value}};
    std::shared_ptr<Value> value_ptr = std::make_shared<Value>(Value(value, DataType::INT64));
    auto result = dispatch_binary_stats(column_stats_values, value_ptr, op);
    auto visitation_result = std::get<std::vector<StatsComparison>>(result);
    ASSERT_EQ(visitation_result.size(), 1);
    ASSERT_EQ(visitation_result.at(0), expected);
}

INSTANTIATE_TEST_SUITE_P(
        LessThan, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: max < value
                std::make_tuple(1, 5, 10, OperationType::LT, SC::ALL_MATCH),
                std::make_tuple(1, 5, 6, OperationType::LT, SC::ALL_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::LT, SC::ALL_MATCH),
                std::make_tuple(0, 0, 1, OperationType::LT, SC::ALL_MATCH),
                // NONE_MATCH: min >= value
                std::make_tuple(5, 10, 5, OperationType::LT, SC::NONE_MATCH),
                std::make_tuple(5, 10, 3, OperationType::LT, SC::NONE_MATCH),
                std::make_tuple(5, 5, 5, OperationType::LT, SC::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::LT, SC::NONE_MATCH),
                // UNKNOWN: min < value <= max
                std::make_tuple(1, 10, 5, OperationType::LT, SC::UNKNOWN),
                std::make_tuple(-10, -5, -7, OperationType::LT, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::LT, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::LT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThan, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min > value
                std::make_tuple(10, 20, 5, OperationType::GT, SC::ALL_MATCH),
                std::make_tuple(6, 10, 5, OperationType::GT, SC::ALL_MATCH),
                std::make_tuple(-10, -5, -11, OperationType::GT, SC::ALL_MATCH),
                // NONE_MATCH: max <= value
                std::make_tuple(1, 5, 5, OperationType::GT, SC::NONE_MATCH),
                std::make_tuple(1, 5, 10, OperationType::GT, SC::NONE_MATCH),
                std::make_tuple(5, 5, 5, OperationType::GT, SC::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::GT, SC::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::GT, SC::NONE_MATCH),
                // UNKNOWN: min <= value < max
                std::make_tuple(1, 10, 5, OperationType::GT, SC::UNKNOWN),
                std::make_tuple(-10, -5, -7, OperationType::GT, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::GT, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::GT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        LessThanEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: max <= value
                std::make_tuple(1, 5, 5, OperationType::LE, SC::ALL_MATCH),
                std::make_tuple(1, 5, 10, OperationType::LE, SC::ALL_MATCH),
                std::make_tuple(5, 5, 5, OperationType::LE, SC::ALL_MATCH),
                std::make_tuple(0, 0, 0, OperationType::LE, SC::ALL_MATCH),
                // NONE_MATCH: min > value
                std::make_tuple(5, 10, 4, OperationType::LE, SC::NONE_MATCH),
                std::make_tuple(5, 5, 4, OperationType::LE, SC::NONE_MATCH),
                std::make_tuple(1, 10, 0, OperationType::LE, SC::NONE_MATCH),
                // UNKNOWN
                std::make_tuple(1, 10, 5, OperationType::LE, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::LE, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::LE, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThanEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min >= value
                std::make_tuple(5, 10, 5, OperationType::GE, SC::ALL_MATCH),
                std::make_tuple(5, 10, 3, OperationType::GE, SC::ALL_MATCH),
                std::make_tuple(5, 5, 5, OperationType::GE, SC::ALL_MATCH),
                // NONE_MATCH: max < value
                std::make_tuple(1, 5, 10, OperationType::GE, SC::NONE_MATCH),
                std::make_tuple(5, 5, 6, OperationType::GE, SC::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::GE, SC::NONE_MATCH),
                // UNKNOWN
                std::make_tuple(1, 10, 5, OperationType::GE, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::GE, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::GE, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        Equals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min == max == value
                std::make_tuple(5, 5, 5, OperationType::EQ, SC::ALL_MATCH),
                std::make_tuple(0, 0, 0, OperationType::EQ, SC::ALL_MATCH),
                std::make_tuple(-3, -3, -3, OperationType::EQ, SC::ALL_MATCH),
                // NONE_MATCH: value outside [min, max]
                std::make_tuple(1, 5, 6, OperationType::EQ, SC::NONE_MATCH),
                std::make_tuple(1, 5, 0, OperationType::EQ, SC::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::EQ, SC::NONE_MATCH),
                std::make_tuple(-10, -5, -11, OperationType::EQ, SC::NONE_MATCH),
                // UNKNOWN: value inside [min, max] but min != max
                std::make_tuple(1, 10, 5, OperationType::EQ, SC::UNKNOWN),
                std::make_tuple(1, 5, 1, OperationType::EQ, SC::UNKNOWN),
                std::make_tuple(1, 5, 5, OperationType::EQ, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::EQ, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::EQ, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        NotEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: value outside [min, max]
                std::make_tuple(1, 5, 6, OperationType::NE, SC::ALL_MATCH),
                std::make_tuple(1, 5, 0, OperationType::NE, SC::ALL_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::NE, SC::ALL_MATCH),
                std::make_tuple(0, 0, 1, OperationType::NE, SC::ALL_MATCH),
                // NONE_MATCH: min == max == value
                std::make_tuple(5, 5, 5, OperationType::NE, SC::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::NE, SC::NONE_MATCH),
                std::make_tuple(-3, -3, -3, OperationType::NE, SC::NONE_MATCH),
                // UNKNOWN: value inside [min, max] but min != max
                std::make_tuple(1, 10, 5, OperationType::NE, SC::UNKNOWN),
                std::make_tuple(1, 5, 3, OperationType::NE, SC::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::NE, SC::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::NE, SC::UNKNOWN)
        )
);

class StatsComparatorNaNTest : public ::testing::TestWithParam<OperationType> {};

TEST_P(StatsComparatorNaNTest, NaNInMinReturnsUnknown) {
    auto op = GetParam();
    std::vector<ColumnStatsValues> stats{
            {construct_value(std::numeric_limits<double>::quiet_NaN()), construct_value(10.0)}
    };
    auto query = std::make_shared<Value>(construct_value(5.0));
    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, query, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, NaNInMaxReturnsUnknown) {
    auto op = GetParam();
    std::vector<ColumnStatsValues> stats{
            {construct_value(1.0), construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    auto query = std::make_shared<Value>(construct_value(5.0));
    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, query, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, BothNaNReturnsUnknown) {
    auto op = GetParam();
    std::vector<ColumnStatsValues> stats{
            {construct_value(std::numeric_limits<double>::quiet_NaN()),
             construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    auto query = std::make_shared<Value>(construct_value(5.0));
    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, query, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), SC::UNKNOWN);
}

INSTANTIATE_TEST_SUITE_P(
        AllComparisonOps, StatsComparatorNaNTest,
        ::testing::Values(
                OperationType::LT, OperationType::LE, OperationType::GT, OperationType::GE, OperationType::EQ,
                OperationType::NE
        )
);

// Parameters: (min, max, query_value, op, expected)
class BinaryComparisonInfinityTest
    : public ::testing::TestWithParam<std::tuple<double, double, double, OperationType, SC>> {};

TEST_P(BinaryComparisonInfinityTest, AllCases) {
    auto [min, max, value, op, expected] = GetParam();
    std::vector<ColumnStatsValues> column_stats_values{{construct_value(min), construct_value(max)}};
    std::shared_ptr<Value> value_ptr = std::make_shared<Value>(construct_value(value));
    auto result = dispatch_binary_stats(column_stats_values, value_ptr, op);
    auto visitation_result = std::get<std::vector<StatsComparison>>(result);
    ASSERT_EQ(visitation_result.size(), 1);
    ASSERT_EQ(visitation_result.at(0), expected);
}

INSTANTIATE_TEST_SUITE_P(
        NegativeInfMin, BinaryComparisonInfinityTest,
        ::testing::Values(
                // [-inf, 10] with various operators and query value 5
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::LT, SC::UNKNOWN),
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::LE, SC::UNKNOWN),
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::GT, SC::UNKNOWN),
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::GE, SC::UNKNOWN),
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::EQ, SC::UNKNOWN),
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::NE, SC::UNKNOWN),
                // [-inf, 10] < 20 -> ALL_MATCH (max=10 < 20)
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 20.0, OperationType::LT, SC::ALL_MATCH),
                // [-inf, 10] > 20 -> NONE_MATCH (max=10 <= 20)
                std::make_tuple(-std::numeric_limits<double>::infinity(), 10.0, 20.0, OperationType::GT, SC::NONE_MATCH)
        )
);

INSTANTIATE_TEST_SUITE_P(
        PositiveInfMax, BinaryComparisonInfinityTest,
        ::testing::Values(
                // [10, +inf] with various operators and query value 5
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LT, SC::NONE_MATCH),
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LE, SC::NONE_MATCH),
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::GT, SC::ALL_MATCH),
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::GE, SC::ALL_MATCH),
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::EQ, SC::NONE_MATCH),
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::NE, SC::ALL_MATCH),
                // [10, +inf] < 5 -> NONE_MATCH (min=10 >= 5)
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LT, SC::NONE_MATCH),
                // [10, +inf] > 20 -> UNKNOWN (min=10 <= 20 but max=inf > 20)
                std::make_tuple(10.0, std::numeric_limits<double>::infinity(), 20.0, OperationType::GT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        BothInfinite, BinaryComparisonInfinityTest,
        ::testing::Values(
                // [-inf, +inf]: every comparison is UNKNOWN
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::LT, SC::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::LE, SC::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::GT, SC::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::GE, SC::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::EQ, SC::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::NE, SC::UNKNOWN
                )
        )
);

namespace {

template<typename StatsType, typename QueryType, typename Func>
SC typed_stats_comparator(StatsType min, StatsType max, QueryType query_val, Func&& func) {
    ColumnStatsValues csv{std::optional<Value>{construct_value(min)}, std::optional<Value>{construct_value(max)}};
    Value query = construct_value(query_val);
    return stats_comparator(csv, query, std::forward<Func>(func));
}

} // namespace

// int32_t stats, double query value
TEST(MixedTypeStatsComparator, Int32StatsDoubleQuery) {
    // [1, 10] > 5.5 => UNKNOWN (min=1 <= 5.5 but max=10 > 5.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 5.5, GreaterThanOperator{}), SC::UNKNOWN);
    // [1, 10] > 10.5 => NONE_MATCH (max=10 <= 10.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 10.5, GreaterThanOperator{}), SC::NONE_MATCH);
    // [1, 10] < 0.5 => NONE_MATCH (min=1 >= 0.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 0.5, LessThanOperator{}), SC::NONE_MATCH);
    // [1, 10] < 10.5 => ALL_MATCH (max=10 < 10.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 10.5, LessThanOperator{}), SC::ALL_MATCH);
    // [5, 5] == 5.0 => ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int32_t{5}, int32_t{5}, 5.0, EqualsOperator{}), SC::ALL_MATCH);
    // [5, 5] == 5.5 => NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int32_t{5}, int32_t{5}, 5.5, EqualsOperator{}), SC::NONE_MATCH);
}

// float stats, int64_t query value
TEST(MixedTypeStatsComparator, FloatStatsInt64Query) {
    // [1.0f, 10.0f] > 5 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{5}, GreaterThanOperator{}), SC::UNKNOWN);
    // [1.0f, 10.0f] > 11 => NONE_MATCH (max=10 <= 11)
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{11}, GreaterThanOperator{}), SC::NONE_MATCH);
    // [1.0f, 10.0f] < 0 => NONE_MATCH (min=1 >= 0)
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{0}, LessThanOperator{}), SC::NONE_MATCH);
    // [100.0f, 200.0f] > 50 => ALL_MATCH (min=100 > 50)
    ASSERT_EQ(typed_stats_comparator(100.0f, 200.0f, int64_t{50}, GreaterThanOperator{}), SC::ALL_MATCH);
}

// double stats, int32_t query value
TEST(MixedTypeStatsComparator, DoubleStatsInt32Query) {
    // [1.5, 10.5] > 10 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{10}, GreaterThanOperator{}), SC::UNKNOWN);
    // [1.5, 10.5] >= 1 => ALL_MATCH (min=1.5 >= 1)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{1}, GreaterThanEqualsOperator{}), SC::ALL_MATCH);
    // [1.5, 10.5] <= 11 => ALL_MATCH (max=10.5 <= 11)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{11}, LessThanEqualsOperator{}), SC::ALL_MATCH);
    // [1.5, 10.5] <= 1 => NONE_MATCH (min=1.5 > 1)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{1}, LessThanEqualsOperator{}), SC::NONE_MATCH);
}

// uint32_t stats, int64_t query value (signed/unsigned promotion)
TEST(MixedTypeStatsComparator, Uint32StatsInt64Query) {
    // [100, 200] > 150 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{150}, GreaterThanOperator{}), SC::UNKNOWN);
    // [100, 200] > 50 => ALL_MATCH (min=100 > 50)
    ASSERT_EQ(typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{50}, GreaterThanOperator{}), SC::ALL_MATCH);
    // [100, 200] < 50 => NONE_MATCH (min=100 >= 50)
    ASSERT_EQ(typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{50}, LessThanOperator{}), SC::NONE_MATCH);
    // [100, 200] > -1 => ALL_MATCH (min=100 > -1, tests signed/unsigned interaction)
    ASSERT_EQ(typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{-1}, GreaterThanOperator{}), SC::ALL_MATCH);
}

// int8_t stats, double query value (small integer to double)
TEST(MixedTypeStatsComparator, Int8StatsDoubleQuery) {
    // [-100, 100] > 50.5 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(int8_t{-100}, int8_t{100}, 50.5, GreaterThanOperator{}), SC::UNKNOWN);
    // [-100, -50] < -49.5 => ALL_MATCH (max=-50 < -49.5)
    ASSERT_EQ(typed_stats_comparator(int8_t{-100}, int8_t{-50}, -49.5, LessThanOperator{}), SC::ALL_MATCH);
    // [50, 100] != 25.5 => ALL_MATCH (25.5 < min=50)
    ASSERT_EQ(typed_stats_comparator(int8_t{50}, int8_t{100}, 25.5, NotEqualsOperator{}), SC::ALL_MATCH);
}

// uint64_t stats, double query value
TEST(MixedTypeStatsComparator, Uint64StatsDoubleQuery) {
    // [1000, 2000] > 1500.5 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint64_t{1000}, uint64_t{2000}, 1500.5, GreaterThanOperator{}), SC::UNKNOWN);
    // [1000, 2000] > 500.5 => ALL_MATCH (min=1000 > 500.5)
    ASSERT_EQ(typed_stats_comparator(uint64_t{1000}, uint64_t{2000}, 500.5, GreaterThanOperator{}), SC::ALL_MATCH);
}

// int64_t stats, uint64_t query value — exercises the signed/unsigned comparison overloads
TEST(MixedTypeStatsComparator, Int64StatsUint64Query_GreaterThan) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Query has MSB set: any int64 range is not greater → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, GreaterThanOperator{}), SC::NONE_MATCH);
    ASSERT_EQ(typed_stats_comparator(int64_t{-1}, int64_t{100}, U64_MAX, GreaterThanOperator{}), SC::NONE_MATCH);
    // Query = uint64(INT64_MAX): max possible int64 is INT64_MAX, not greater → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), GreaterThanOperator{}), SC::NONE_MATCH);
    // Query = uint64(INT64_MAX - 1): INT64_MAX > INT64_MAX-1 → ALL_MATCH when min is also INT64_MAX
    ASSERT_EQ(typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX - 1), GreaterThanOperator{}), SC::ALL_MATCH);
    // Range spanning the boundary
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX - 1), GreaterThanOperator{}), SC::UNKNOWN);
    // Query = 0: positive range is greater
    ASSERT_EQ(typed_stats_comparator(int64_t{1}, int64_t{10}, uint64_t{0}, GreaterThanOperator{}), SC::ALL_MATCH);
    // Query = 0: negative range is not greater
    ASSERT_EQ(typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, GreaterThanOperator{}), SC::NONE_MATCH);
    // Query = 0: range spanning zero
    ASSERT_EQ(typed_stats_comparator(int64_t{-5}, int64_t{5}, uint64_t{0}, GreaterThanOperator{}), SC::UNKNOWN);
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_LessThan) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: any int64 is less → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, LessThanOperator{}), SC::ALL_MATCH);
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, std::numeric_limits<uint64_t>::max(), LessThanOperator{}),
            SC::ALL_MATCH
    );
    // Query = uint64(INT64_MAX): range entirely below → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX - 1, uint64_t(I64_MAX), LessThanOperator{}), SC::ALL_MATCH);
    // Query = uint64(INT64_MAX): min = INT64_MAX, not less → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), LessThanOperator{}), SC::NONE_MATCH);
    // Query = 0: negative range entirely below → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, LessThanOperator{}), SC::ALL_MATCH);
    // Query = 0: range starting at 0 → min not less → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, LessThanOperator{}), SC::NONE_MATCH);
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_Equals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: no int64 can equal it → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, EqualsOperator{}), SC::NONE_MATCH);
    // Exact match at INT64_MAX boundary
    ASSERT_EQ(typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), EqualsOperator{}), SC::ALL_MATCH);
    // Range contains the query value but is not a point → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), EqualsOperator{}), SC::UNKNOWN);
    // Value outside range → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{11}, EqualsOperator{}), SC::NONE_MATCH);
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_NotEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: every int64 differs → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, NotEqualsOperator{}), SC::ALL_MATCH);
    // Point range equal to query → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), NotEqualsOperator{}), SC::NONE_MATCH);
    // Non-point range containing query → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), NotEqualsOperator{}), SC::UNKNOWN);
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_LessThanEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: any int64 ≤ it → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, LessThanEqualsOperator{}), SC::ALL_MATCH);
    // Query = uint64(INT64_MAX): max = INT64_MAX ≤ INT64_MAX → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), LessThanEqualsOperator{}), SC::ALL_MATCH);
    // Query = 0: positive range min > 0 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{1}, int64_t{10}, uint64_t{0}, LessThanEqualsOperator{}), SC::NONE_MATCH);
    // Query = 0: range starting at 0 → max may exceed → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, LessThanEqualsOperator{}), SC::UNKNOWN);
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_GreaterThanEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: no int64 ≥ it → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, GreaterThanEqualsOperator{}), SC::NONE_MATCH);
    // Query = uint64(INT64_MAX): only INT64_MAX itself qualifies
    ASSERT_EQ(typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), GreaterThanEqualsOperator{}), SC::ALL_MATCH);
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), GreaterThanEqualsOperator{}), SC::UNKNOWN);
    // Query = 0: range [0, 10], min ≥ 0 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, GreaterThanEqualsOperator{}), SC::ALL_MATCH);
    // Query = 0: negative range → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, GreaterThanEqualsOperator{}), SC::NONE_MATCH
    );
}

// uint64_t stats, int64_t query value — exercises the signed/unsigned comparison overloads
TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_GreaterThan) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto I64_MIN = std::numeric_limits<int64_t>::min();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Stats min has MSB set: always greater than any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, U64_MAX, I64_MAX, GreaterThanOperator{}), SC::ALL_MATCH);
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, int64_t{0}, GreaterThanOperator{}), SC::ALL_MATCH);
    // Query negative: any uint64 > negative → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, GreaterThanOperator{}), SC::ALL_MATCH);
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{0}, I64_MIN, GreaterThanOperator{}), SC::ALL_MATCH);
    // Query = INT64_MAX, stats just below: max = INT64_MAX-1 as uint64, not greater → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t(I64_MAX - 1), I64_MAX, GreaterThanOperator{}), SC::NONE_MATCH
    );
    // Query = INT64_MAX, stats spanning the MSB boundary → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, GreaterThanOperator{}), SC::UNKNOWN);
    // Query = 0, stats entirely in shared range: [0, 0] > 0 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{0}, int64_t{0}, GreaterThanOperator{}), SC::NONE_MATCH);
    // Query = 0: [1, 10] > 0 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{1}, uint64_t{10}, int64_t{0}, GreaterThanOperator{}), SC::ALL_MATCH);
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_LessThan) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Stats min has MSB set: uint64 with MSB is never less than any int64 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, U64_MAX, I64_MAX, LessThanOperator{}), SC::NONE_MATCH);
    // Query negative: no uint64 < negative → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, LessThanOperator{}), SC::NONE_MATCH);
    // Stats entirely below query in shared range: [0, 4] < 5 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{4}, int64_t{5}, LessThanOperator{}), SC::ALL_MATCH);
    // Stats spanning the MSB boundary, query = INT64_MAX → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, LessThanOperator{}), SC::UNKNOWN);
    // Point at INT64_MAX: not less → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, LessThanOperator{}), SC::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_Equals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: no int64 can equal → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, EqualsOperator{}), SC::NONE_MATCH);
    // Query negative: no uint64 equals negative → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, EqualsOperator{}), SC::NONE_MATCH);
    // Exact match in shared range
    ASSERT_EQ(typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, EqualsOperator{}), SC::ALL_MATCH);
    // Range contains query in shared range → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{50}, EqualsOperator{}), SC::UNKNOWN);
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_NotEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: every value differs from any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, NotEqualsOperator{}), SC::ALL_MATCH);
    // Query negative: every uint64 differs → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, NotEqualsOperator{}), SC::ALL_MATCH);
    // Point range equal to query → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, NotEqualsOperator{}), SC::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_LessThanEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: never ≤ any int64 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, LessThanEqualsOperator{}), SC::NONE_MATCH);
    // Query negative: no uint64 ≤ negative → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, LessThanEqualsOperator{}), SC::NONE_MATCH
    );
    // Max in shared range ≤ query → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{10}, I64_MAX, LessThanEqualsOperator{}), SC::ALL_MATCH);
    // Point at boundary: INT64_MAX ≤ INT64_MAX → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, LessThanEqualsOperator{}),
            SC::ALL_MATCH
    );
    // Spanning MSB boundary → UNKNOWN
    ASSERT_EQ(typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, LessThanEqualsOperator{}), SC::UNKNOWN);
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_GreaterThanEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto I64_MIN = std::numeric_limits<int64_t>::min();
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats min has MSB set: always ≥ any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, GreaterThanEqualsOperator{}), SC::ALL_MATCH);
    // Query negative: any uint64 ≥ negative → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{0}, int64_t{-1}, GreaterThanEqualsOperator{}), SC::ALL_MATCH
    );
    ASSERT_EQ(typed_stats_comparator(uint64_t{0}, uint64_t{0}, I64_MIN, GreaterThanEqualsOperator{}), SC::ALL_MATCH);
    // Point at boundary: INT64_MAX ≥ INT64_MAX → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, GreaterThanEqualsOperator{}),
            SC::ALL_MATCH
    );
    // Max below query in shared range → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{4}, int64_t{5}, GreaterThanEqualsOperator{}), SC::NONE_MATCH
    );
}

INSTANTIATE_TEST_SUITE_P(
        InfinityQueryValue, BinaryComparisonInfinityTest,
        ::testing::Values(
                // Finite range, query value is +inf
                // [1, 10] < +inf -> ALL_MATCH (max=10 < inf)
                std::make_tuple(1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::LT, SC::ALL_MATCH),
                // [1, 10] > +inf -> NONE_MATCH (max=10 <= inf)
                std::make_tuple(1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::GT, SC::NONE_MATCH),
                // [1, 10] == +inf -> NONE_MATCH (max=10 < inf)
                std::make_tuple(1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::EQ, SC::NONE_MATCH),
                // [1, 10] != +inf -> ALL_MATCH (max=10 < inf)
                std::make_tuple(1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::NE, SC::ALL_MATCH),
                // Finite range, query value is -inf
                // [1, 10] > -inf -> ALL_MATCH (min=1 > -inf)
                std::make_tuple(1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::GT, SC::ALL_MATCH),
                // [1, 10] < -inf -> NONE_MATCH (min=1 >= -inf)
                std::make_tuple(1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::LT, SC::NONE_MATCH),
                // [1, 10] == -inf -> NONE_MATCH (min=1 > -inf)
                std::make_tuple(1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::EQ, SC::NONE_MATCH),
                // [1, 10] != -inf -> ALL_MATCH (min=1 > -inf)
                std::make_tuple(1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::NE, SC::ALL_MATCH)
        )
);
