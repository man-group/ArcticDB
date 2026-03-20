#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>
#include <arcticdb/pipeline/value_set.hpp>

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
                std::make_tuple(std::optional<bool>{}, std::optional<bool>{}, OperationType::NOT, SC::UNKNOWN),
                std::make_tuple(std::optional{false}, std::optional<bool>{}, OperationType::IDENTITY, SC::UNKNOWN),
                std::make_tuple(std::optional{false}, std::optional<bool>{}, OperationType::NOT, SC::UNKNOWN),
                std::make_tuple(std::optional<bool>{}, std::optional{true}, OperationType::IDENTITY, SC::UNKNOWN),
                std::make_tuple(std::optional<bool>{}, std::optional{true}, OperationType::NOT, SC::UNKNOWN)
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

namespace {

template<typename T>
SC value_range_comparison(ValueRange<T> range, T value, OperationType op) {
    switch (op) {
    case OperationType::LT:
        return LessThanOperator{}(range, value);
    case OperationType::LE:
        return LessThanEqualsOperator{}(range, value);
    case OperationType::GT:
        return GreaterThanOperator{}(range, value);
    case OperationType::GE:
        return GreaterThanEqualsOperator{}(range, value);
    case OperationType::EQ:
        return EqualsOperator{}(range, value);
    case OperationType::NE:
        return NotEqualsOperator{}(range, value);
    default:
        util::raise_rte("Unsupported op in value_range_comparison");
    }
}
} // namespace

// Parameters: (min, max, query_value, op, expected)
class ValueRangeComparisonTest
    : public ::testing::TestWithParam<std::tuple<int64_t, int64_t, int64_t, OperationType, SC>> {};

TEST_P(ValueRangeComparisonTest, AllCases) {
    auto [min, max, value, op, expected] = GetParam();
    ASSERT_EQ(value_range_comparison(ValueRange<int64_t>{min, max}, value, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        LessThan, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::LT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThan, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::GT, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        LessThanEquals, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::LE, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThanEquals, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::GE, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        Equals, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::EQ, SC::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        NotEquals, ValueRangeComparisonTest,
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
                std::make_tuple(-5, 5, 0, OperationType::NE, SC::UNKNOWN)
        )
);

class StatsComparatorNaNTest : public ::testing::TestWithParam<OperationType> {};

TEST_P(StatsComparatorNaNTest, NaNInMinReturnsUnknown) {
    auto op = GetParam();
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())},
            std::optional<Value>{construct_value(10.0)}
    };
    Value query = construct_value(5.0);

    SC result;
    switch (op) {
    case OperationType::LT:
        result = stats_comparator(csv, query, LessThanOperator{});
        break;
    case OperationType::LE:
        result = stats_comparator(csv, query, LessThanEqualsOperator{});
        break;
    case OperationType::GT:
        result = stats_comparator(csv, query, GreaterThanOperator{});
        break;
    case OperationType::GE:
        result = stats_comparator(csv, query, GreaterThanEqualsOperator{});
        break;
    case OperationType::EQ:
        result = stats_comparator(csv, query, EqualsOperator{});
        break;
    case OperationType::NE:
        result = stats_comparator(csv, query, NotEqualsOperator{});
        break;
    default:
        FAIL() << "Unexpected op";
    }
    ASSERT_EQ(result, SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, NaNInMaxReturnsUnknown) {
    auto op = GetParam();
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(1.0)},
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    Value query = construct_value(5.0);

    SC result;
    switch (op) {
    case OperationType::LT:
        result = stats_comparator(csv, query, LessThanOperator{});
        break;
    case OperationType::LE:
        result = stats_comparator(csv, query, LessThanEqualsOperator{});
        break;
    case OperationType::GT:
        result = stats_comparator(csv, query, GreaterThanOperator{});
        break;
    case OperationType::GE:
        result = stats_comparator(csv, query, GreaterThanEqualsOperator{});
        break;
    case OperationType::EQ:
        result = stats_comparator(csv, query, EqualsOperator{});
        break;
    case OperationType::NE:
        result = stats_comparator(csv, query, NotEqualsOperator{});
        break;
    default:
        FAIL() << "Unexpected op";
    }
    ASSERT_EQ(result, SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, BothNaNReturnsUnknown) {
    auto op = GetParam();
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())},
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    Value query = construct_value(5.0);

    SC result;
    switch (op) {
    case OperationType::LT:
        result = stats_comparator(csv, query, LessThanOperator{});
        break;
    case OperationType::LE:
        result = stats_comparator(csv, query, LessThanEqualsOperator{});
        break;
    case OperationType::GT:
        result = stats_comparator(csv, query, GreaterThanOperator{});
        break;
    case OperationType::GE:
        result = stats_comparator(csv, query, GreaterThanEqualsOperator{});
        break;
    case OperationType::EQ:
        result = stats_comparator(csv, query, EqualsOperator{});
        break;
    case OperationType::NE:
        result = stats_comparator(csv, query, NotEqualsOperator{});
        break;
    default:
        FAIL() << "Unexpected op";
    }
    ASSERT_EQ(result, SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, MissingMinReturnsUnknown) {
    auto op = GetParam();
    ColumnStatsValues csv{std::nullopt, std::optional<Value>{construct_value(10.0)}};
    Value query = construct_value(5.0);

    SC result;
    switch (op) {
    case OperationType::LT:
        result = stats_comparator(csv, query, LessThanOperator{});
        break;
    case OperationType::LE:
        result = stats_comparator(csv, query, LessThanEqualsOperator{});
        break;
    case OperationType::GT:
        result = stats_comparator(csv, query, GreaterThanOperator{});
        break;
    case OperationType::GE:
        result = stats_comparator(csv, query, GreaterThanEqualsOperator{});
        break;
    case OperationType::EQ:
        result = stats_comparator(csv, query, EqualsOperator{});
        break;
    case OperationType::NE:
        result = stats_comparator(csv, query, NotEqualsOperator{});
        break;
    default:
        FAIL() << "Unexpected op";
    }
    ASSERT_EQ(result, SC::UNKNOWN);
}

TEST_P(StatsComparatorNaNTest, MissingMaxReturnsUnknown) {
    auto op = GetParam();
    ColumnStatsValues csv{std::optional<Value>{construct_value(1.0)}, std::nullopt};
    Value query = construct_value(5.0);

    SC result;
    switch (op) {
    case OperationType::LT:
        result = stats_comparator(csv, query, LessThanOperator{});
        break;
    case OperationType::LE:
        result = stats_comparator(csv, query, LessThanEqualsOperator{});
        break;
    case OperationType::GT:
        result = stats_comparator(csv, query, GreaterThanOperator{});
        break;
    case OperationType::GE:
        result = stats_comparator(csv, query, GreaterThanEqualsOperator{});
        break;
    case OperationType::EQ:
        result = stats_comparator(csv, query, EqualsOperator{});
        break;
    case OperationType::NE:
        result = stats_comparator(csv, query, NotEqualsOperator{});
        break;
    default:
        FAIL() << "Unexpected op";
    }
    ASSERT_EQ(result, SC::UNKNOWN);
}

INSTANTIATE_TEST_SUITE_P(
        AllComparisonOps, StatsComparatorNaNTest,
        ::testing::Values(
                OperationType::LT, OperationType::LE, OperationType::GT, OperationType::GE, OperationType::EQ,
                OperationType::NE
        )
);

// Parameters: (min, max, query_value, op, expected)
class ValueRangeInfinityTest : public ::testing::TestWithParam<std::tuple<double, double, double, OperationType, SC>> {
};

TEST_P(ValueRangeInfinityTest, AllCases) {
    auto [min, max, value, op, expected] = GetParam();
    ASSERT_EQ(value_range_comparison(ValueRange{min, max}, value, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        NegativeInfMin, ValueRangeInfinityTest,
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
        PositiveInfMax, ValueRangeInfinityTest,
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
        BothInfinite, ValueRangeInfinityTest,
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

INSTANTIATE_TEST_SUITE_P(
        InfinityQueryValue, ValueRangeInfinityTest,
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
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::NONE_MATCH);
}

TEST(StatsMembershipComparator, EmptySetIsnotinAllMatch) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{10})}
    };
    auto vs = make_empty_value_set<int64_t>();
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetBelowBlockRange) {
    // Block [10, 20], set {1, 5} -> set_max=5 < block_min=10 -> NONE_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{10})}, std::optional<Value>{construct_value(int64_t{20})}
    };
    auto vs = make_numeric_value_set<int64_t>({1, 5});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::NONE_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetAboveBlockRange) {
    // Block [1, 5], set {10, 20} -> set_min=10 > block_max=5 -> NONE_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{5})}
    };
    auto vs = make_numeric_value_set<int64_t>({10, 20});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::NONE_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::ALL_MATCH);
}

TEST(StatsMembershipComparator, SetOverlapsBlockRange) {
    // Block [1, 10], set {5, 15} -> overlaps -> UNKNOWN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{10})}
    };
    auto vs = make_numeric_value_set<int64_t>({5, 15});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::UNKNOWN);
}

TEST(StatsMembershipComparator, SingleElementMatchesSingleValueBlock) {
    // Block [5, 5], set {5} -> ALL_MATCH for ISIN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{5})}, std::optional<Value>{construct_value(int64_t{5})}
    };
    auto vs = make_numeric_value_set<int64_t>({5});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::ALL_MATCH);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::NONE_MATCH);
}

TEST(StatsMembershipComparator, MixedTypesInt32StatsDoubleSet) {
    // Block [1, 10] as int32, set {15.0, 20.0} as double -> set_min=15 > block_max=10 -> NONE_MATCH
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int32_t{1})}, std::optional<Value>{construct_value(int32_t{10})}
    };
    auto vs = make_numeric_value_set<double>({15.0, 20.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::NONE_MATCH);
}

TEST(StatsMembershipComparator, NaNInStatsMin) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())},
            std::optional<Value>{construct_value(10.0)}
    };
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
}

TEST(StatsMembershipComparator, NaNInStatsMax) {
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(1.0)},
            std::optional<Value>{construct_value(std::numeric_limits<double>::quiet_NaN())}
    };
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
}

TEST(StatsMembershipComparator, MissingStatsMin) {
    ColumnStatsValues csv{std::nullopt, std::optional<Value>{construct_value(10.0)}};
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
}

TEST(StatsMembershipComparator, MissingStatsMax) {
    ColumnStatsValues csv{std::optional<Value>{construct_value(1.0)}, std::nullopt};
    auto vs = make_numeric_value_set<double>({5.0});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
}

TEST(StatsMembershipComparator, SetContainedWithinBlockRange) {
    // Block [1, 100], set {3, 7} -> overlap -> UNKNOWN
    ColumnStatsValues csv{
            std::optional<Value>{construct_value(int64_t{1})}, std::optional<Value>{construct_value(int64_t{100})}
    };
    auto vs = make_numeric_value_set<int64_t>({3, 7});
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISIN), SC::UNKNOWN);
    ASSERT_EQ(stats_membership_comparator(csv, *vs, OperationType::ISNOTIN), SC::UNKNOWN);
}
