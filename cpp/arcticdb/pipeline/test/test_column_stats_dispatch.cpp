#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>
#include <arcticdb/util/constants.hpp>

#include <cmath>
#include <limits>

using namespace arcticdb;
using namespace arcticdb::column_stats_detail;

const Value nan_value = construct_value(std::numeric_limits<double>::quiet_NaN());

class BinaryBooleanStatsTest
    : public ::testing::TestWithParam<std::tuple<StatsComparison, StatsComparison, OperationType, StatsComparison>> {};

TEST_P(BinaryBooleanStatsTest, AllCombinations) {
    auto [left, right, op, expected] = GetParam();
    ASSERT_EQ(binary_boolean_stats(left, right, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        AND, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::ALL_MATCH, OperationType::AND,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::NONE_MATCH, OperationType::AND,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::UNKNOWN, OperationType::AND,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::ALL_MATCH, OperationType::AND,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::NONE_MATCH, OperationType::AND,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::UNKNOWN, OperationType::AND,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::ALL_MATCH, OperationType::AND,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::NONE_MATCH, OperationType::AND,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::UNKNOWN, OperationType::AND, StatsComparison::UNKNOWN
                )
        )
);

INSTANTIATE_TEST_SUITE_P(
        OR, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::ALL_MATCH, OperationType::OR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::NONE_MATCH, OperationType::OR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::UNKNOWN, OperationType::OR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::ALL_MATCH, OperationType::OR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::NONE_MATCH, OperationType::OR,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::UNKNOWN, OperationType::OR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::ALL_MATCH, OperationType::OR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::NONE_MATCH, OperationType::OR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::UNKNOWN, OperationType::OR, StatsComparison::UNKNOWN
                )
        )
);

INSTANTIATE_TEST_SUITE_P(
        XOR, BinaryBooleanStatsTest,
        ::testing::Values(
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::ALL_MATCH, OperationType::XOR,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::NONE_MATCH, OperationType::XOR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::ALL_MATCH, StatsComparison::UNKNOWN, OperationType::XOR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::ALL_MATCH, OperationType::XOR,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::NONE_MATCH, OperationType::XOR,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        StatsComparison::NONE_MATCH, StatsComparison::UNKNOWN, OperationType::XOR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::ALL_MATCH, OperationType::XOR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::NONE_MATCH, OperationType::XOR,
                        StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        StatsComparison::UNKNOWN, StatsComparison::UNKNOWN, OperationType::XOR, StatsComparison::UNKNOWN
                )
        )
);

// Tests for unary_boolean_stats(StatsComparison, OperationType)
class UnaryBooleanStatsFromComparisonTest
    : public ::testing::TestWithParam<std::tuple<StatsComparison, OperationType, StatsComparison>> {};

TEST_P(UnaryBooleanStatsFromComparisonTest, AllCombinations) {
    auto [input, op, expected] = GetParam();
    ASSERT_EQ(unary_boolean_stats(input, op), expected);
}

INSTANTIATE_TEST_SUITE_P(
        IDENTITY, UnaryBooleanStatsFromComparisonTest,
        ::testing::Values(
                std::make_tuple(StatsComparison::ALL_MATCH, OperationType::IDENTITY, StatsComparison::ALL_MATCH),
                std::make_tuple(StatsComparison::NONE_MATCH, OperationType::IDENTITY, StatsComparison::NONE_MATCH),
                std::make_tuple(StatsComparison::UNKNOWN, OperationType::IDENTITY, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        NOT, UnaryBooleanStatsFromComparisonTest,
        ::testing::Values(
                std::make_tuple(StatsComparison::ALL_MATCH, OperationType::NOT, StatsComparison::NONE_MATCH),
                std::make_tuple(StatsComparison::NONE_MATCH, OperationType::NOT, StatsComparison::ALL_MATCH),
                std::make_tuple(StatsComparison::UNKNOWN, OperationType::NOT, StatsComparison::UNKNOWN)
        )
);

// Tests for unary_boolean_stats(ColumnStatsValues, OperationType)
// Parameters: (min: optional<bool>, max: optional<bool>, op, expected)
class UnaryBooleanStatsFromColumnStatsTest
    : public ::testing::TestWithParam<
              std::tuple<std::optional<bool>, std::optional<bool>, OperationType, StatsComparison>> {};

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
                std::make_tuple(
                        std::optional<bool>{}, std::optional<bool>{}, OperationType::IDENTITY, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        std::optional<bool>{}, std::optional<bool>{}, OperationType::NOT, StatsComparison::UNKNOWN
                )
        )
);

INSTANTIATE_TEST_SUITE_P(
        IDENTITY, UnaryBooleanStatsFromColumnStatsTest,
        ::testing::Values(
                std::make_tuple(
                        std::optional{false}, std::optional{true}, OperationType::IDENTITY, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        std::optional{false}, std::optional{false}, OperationType::IDENTITY, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        std::optional{true}, std::optional{true}, OperationType::IDENTITY, StatsComparison::ALL_MATCH
                )
                // min=true, max=false case is impossible
        )
);

INSTANTIATE_TEST_SUITE_P(
        NOT, UnaryBooleanStatsFromColumnStatsTest,
        ::testing::Values(
                std::make_tuple(
                        std::optional{false}, std::optional{true}, OperationType::NOT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        std::optional{false}, std::optional{false}, OperationType::NOT, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        std::optional{true}, std::optional{true}, OperationType::NOT, StatsComparison::NONE_MATCH
                )
                // min=true, max=false case is impossible
        )
);

// Parameters: (min, max, query_value, op, expected)
class BinaryComparisonTest
    : public ::testing::TestWithParam<
              std::tuple<std::optional<int64_t>, std::optional<int64_t>, int64_t, OperationType, StatsComparison>> {};

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
                std::make_tuple(1, 5, 10, OperationType::LT, StatsComparison::ALL_MATCH),
                std::make_tuple(1, 5, 6, OperationType::LT, StatsComparison::ALL_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::LT, StatsComparison::ALL_MATCH),
                std::make_tuple(0, 0, 1, OperationType::LT, StatsComparison::ALL_MATCH),
                // NONE_MATCH: min >= value
                std::make_tuple(5, 10, 5, OperationType::LT, StatsComparison::NONE_MATCH),
                std::make_tuple(5, 10, 3, OperationType::LT, StatsComparison::NONE_MATCH),
                std::make_tuple(5, 5, 5, OperationType::LT, StatsComparison::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::LT, StatsComparison::NONE_MATCH),
                // UNKNOWN: min < value <= max
                std::make_tuple(1, 10, 5, OperationType::LT, StatsComparison::UNKNOWN),
                std::make_tuple(-10, -5, -7, OperationType::LT, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::LT, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::LT, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThan, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min > value
                std::make_tuple(10, 20, 5, OperationType::GT, StatsComparison::ALL_MATCH),
                std::make_tuple(6, 10, 5, OperationType::GT, StatsComparison::ALL_MATCH),
                std::make_tuple(-10, -5, -11, OperationType::GT, StatsComparison::ALL_MATCH),
                // NONE_MATCH: max <= value
                std::make_tuple(1, 5, 5, OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(1, 5, 10, OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(5, 5, 5, OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::GT, StatsComparison::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::GT, StatsComparison::NONE_MATCH),
                // UNKNOWN: min <= value < max
                std::make_tuple(1, 10, 5, OperationType::GT, StatsComparison::UNKNOWN),
                std::make_tuple(-10, -5, -7, OperationType::GT, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::GT, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::GT, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        LessThanEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: max <= value
                std::make_tuple(1, 5, 5, OperationType::LE, StatsComparison::ALL_MATCH),
                std::make_tuple(1, 5, 10, OperationType::LE, StatsComparison::ALL_MATCH),
                std::make_tuple(5, 5, 5, OperationType::LE, StatsComparison::ALL_MATCH),
                std::make_tuple(0, 0, 0, OperationType::LE, StatsComparison::ALL_MATCH),
                // NONE_MATCH: min > value
                std::make_tuple(5, 10, 4, OperationType::LE, StatsComparison::NONE_MATCH),
                std::make_tuple(5, 5, 4, OperationType::LE, StatsComparison::NONE_MATCH),
                std::make_tuple(1, 10, 0, OperationType::LE, StatsComparison::NONE_MATCH),
                // UNKNOWN
                std::make_tuple(1, 10, 5, OperationType::LE, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::LE, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::LE, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        GreaterThanEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min >= value
                std::make_tuple(5, 10, 5, OperationType::GE, StatsComparison::ALL_MATCH),
                std::make_tuple(5, 10, 3, OperationType::GE, StatsComparison::ALL_MATCH),
                std::make_tuple(5, 5, 5, OperationType::GE, StatsComparison::ALL_MATCH),
                // NONE_MATCH: max < value
                std::make_tuple(1, 5, 10, OperationType::GE, StatsComparison::NONE_MATCH),
                std::make_tuple(5, 5, 6, OperationType::GE, StatsComparison::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::GE, StatsComparison::NONE_MATCH),
                // UNKNOWN
                std::make_tuple(1, 10, 5, OperationType::GE, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::GE, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::GE, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        Equals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: min == max == value
                std::make_tuple(5, 5, 5, OperationType::EQ, StatsComparison::ALL_MATCH),
                std::make_tuple(0, 0, 0, OperationType::EQ, StatsComparison::ALL_MATCH),
                std::make_tuple(-3, -3, -3, OperationType::EQ, StatsComparison::ALL_MATCH),
                // NONE_MATCH: value outside [min, max]
                std::make_tuple(1, 5, 6, OperationType::EQ, StatsComparison::NONE_MATCH),
                std::make_tuple(1, 5, 0, OperationType::EQ, StatsComparison::NONE_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::EQ, StatsComparison::NONE_MATCH),
                std::make_tuple(-10, -5, -11, OperationType::EQ, StatsComparison::NONE_MATCH),
                // UNKNOWN: value inside [min, max] but min != max
                std::make_tuple(1, 10, 5, OperationType::EQ, StatsComparison::UNKNOWN),
                std::make_tuple(1, 5, 1, OperationType::EQ, StatsComparison::UNKNOWN),
                std::make_tuple(1, 5, 5, OperationType::EQ, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::EQ, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::EQ, StatsComparison::UNKNOWN)
        )
);

INSTANTIATE_TEST_SUITE_P(
        NotEquals, BinaryComparisonTest,
        ::testing::Values(
                // ALL_MATCH: value outside [min, max]
                std::make_tuple(1, 5, 6, OperationType::NE, StatsComparison::ALL_MATCH),
                std::make_tuple(1, 5, 0, OperationType::NE, StatsComparison::ALL_MATCH),
                std::make_tuple(-10, -5, -3, OperationType::NE, StatsComparison::ALL_MATCH),
                std::make_tuple(0, 0, 1, OperationType::NE, StatsComparison::ALL_MATCH),
                // NONE_MATCH: min == max == value
                std::make_tuple(5, 5, 5, OperationType::NE, StatsComparison::NONE_MATCH),
                std::make_tuple(0, 0, 0, OperationType::NE, StatsComparison::NONE_MATCH),
                std::make_tuple(-3, -3, -3, OperationType::NE, StatsComparison::NONE_MATCH),
                // UNKNOWN: value inside [min, max] but min != max
                std::make_tuple(1, 10, 5, OperationType::NE, StatsComparison::UNKNOWN),
                std::make_tuple(1, 5, 3, OperationType::NE, StatsComparison::UNKNOWN),
                std::make_tuple(-5, 5, 0, OperationType::NE, StatsComparison::UNKNOWN),
                // UNKNOWN: missing stats
                std::make_tuple(std::nullopt, std::nullopt, 5, OperationType::NE, StatsComparison::UNKNOWN)
        )
);

// NaT is not treated specially by column stats - it compares as its raw int64 value (int64_min).
class StatsComparatorNaTTest
    : public ::testing::TestWithParam<std::tuple<Value, Value, OperationType, StatsComparison>> {};

TEST_P(StatsComparatorNaTTest, NaTTreatedAsNormalValue) {
    auto [max_val, query_val, op, expected] = GetParam();

    Value min_val = Value(NaT, DataType::NANOSECONDS_UTC64);
    std::vector<ColumnStatsValues> stats{{min_val, max_val}};
    auto query = std::make_shared<Value>(query_val);
    auto result = std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, query, op));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.at(0), expected);
}

// range=[NaT, NaT], i.e. [int64_min, int64_min] - a single-value range
INSTANTIATE_TEST_SUITE_P(
        NaTRange, StatsComparatorNaTTest,
        ::testing::Values(
                // query=NaT: all values equal query
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::LT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::GE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::EQ, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::NE, StatsComparison::NONE_MATCH
                ),
                // query>NaT: all values in range are less than query
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::LT, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{0}, DataType::NANOSECONDS_UTC64),
                        OperationType::NE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::LT, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        OperationType::NE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::LT, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(NaT, DataType::NANOSECONDS_UTC64), Value(timestamp{2}, DataType::NANOSECONDS_UTC64),
                        OperationType::NE, StatsComparison::ALL_MATCH
                )
        )
);

// range=[NaT, 1], i.e. [int64_min, 1]
INSTANTIATE_TEST_SUITE_P(
        NaTMinNormalMax, StatsComparatorNaTTest,
        ::testing::Values(
                // query=NaT (int64_min): inside range at lower bound
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::LT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::LE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::GT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::GE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::EQ, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), Value(NaT, DataType::NANOSECONDS_UTC64),
                        OperationType::NE, StatsComparison::UNKNOWN
                ),
                // query=0: inside range
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::LT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::LE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::GT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::GE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::EQ, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{0}, DataType::NANOSECONDS_UTC64), OperationType::NE, StatsComparison::UNKNOWN
                ),
                // query=1: at upper bound
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::LT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::GE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::EQ, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64), OperationType::NE, StatsComparison::UNKNOWN
                ),
                // query=2: above range
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::LT, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::LE, StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::GT, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::GE, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        Value(timestamp{1}, DataType::NANOSECONDS_UTC64),
                        Value(timestamp{2}, DataType::NANOSECONDS_UTC64), OperationType::NE, StatsComparison::ALL_MATCH
                )
        )
);

enum class ComparisonValueType { NA, REAL };

class StatsComparatorBothNaNTest : public ::testing::TestWithParam<std::tuple<OperationType, ComparisonValueType>> {};

TEST_P(StatsComparatorBothNaNTest, BothNaN) {
    auto [op, value_type] = GetParam();
    std::vector<ColumnStatsValues> stats{{nan_value, nan_value}};

    Value query;
    if (value_type == ComparisonValueType::NA) {
        query = nan_value;
    } else {
        query = construct_value(5.0);
    }
    auto result =
            std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, std::make_shared<Value>(query), op));
    ASSERT_EQ(result.size(), 1);

    StatsComparison expected;
    if (op == OperationType::NE) {
        expected = StatsComparison::ALL_MATCH; // NaN != (NaN|5) is true
    } else {
        expected = StatsComparison::NONE_MATCH; // NaN (<|==|>) (NaN|5) is always false
    }
    ASSERT_EQ(result.at(0), expected);
}

INSTANTIATE_TEST_SUITE_P(
        AllComparisonOps, StatsComparatorBothNaNTest,
        ::testing::Combine(
                ::testing::Values(
                        OperationType::LT, OperationType::LE, OperationType::GT, OperationType::GE, OperationType::EQ,
                        OperationType::NE
                ),
                ::testing::Values(ComparisonValueType::NA, ComparisonValueType::REAL)
        )
);

class StatsComparatorNaNValueTest : public ::testing::TestWithParam<std::tuple<OperationType>> {};

TEST_P(StatsComparatorNaNValueTest, NaNInQuery) {
    auto [op] = GetParam();
    Value min_val = construct_value(1.0);
    Value max_val = construct_value(5.0);
    std::vector<ColumnStatsValues> stats{{std::move(min_val), std::move(max_val)}};

    auto result =
            std::get<std::vector<StatsComparison>>(dispatch_binary_stats(stats, std::make_shared<Value>(nan_value), op)
            );
    ASSERT_EQ(result.size(), 1);

    StatsComparison expected;
    if (op == OperationType::NE) {
        expected = StatsComparison::ALL_MATCH; // NaN != (NaN|5) is true
    } else {
        expected = StatsComparison::NONE_MATCH; // NaN (<|==|>) (NaN|5) is always false
    }
    ASSERT_EQ(result.at(0), expected);
}

INSTANTIATE_TEST_SUITE_P(
        AllComparisonOps, StatsComparatorNaNValueTest,
        ::testing::Values(
                OperationType::LT, OperationType::LE, OperationType::GT, OperationType::GE, OperationType::EQ,
                OperationType::NE
        )
);

// Parameters: (min, max, query_value, op, expected)
class BinaryComparisonInfinityTest
    : public ::testing::TestWithParam<std::tuple<double, double, double, OperationType, StatsComparison>> {};

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
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::LT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::LE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::GT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::GE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::EQ, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 5.0, OperationType::NE, StatsComparison::UNKNOWN
                ),
                // [-inf, 10] < 20 -> ALL_MATCH (max=10 < 20)
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 20.0, OperationType::LT,
                        StatsComparison::ALL_MATCH
                ),
                // [-inf, 10] > 20 -> NONE_MATCH (max=10 <= 20)
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), 10.0, 20.0, OperationType::GT,
                        StatsComparison::NONE_MATCH
                )
        )
);

INSTANTIATE_TEST_SUITE_P(
        PositiveInfMax, BinaryComparisonInfinityTest,
        ::testing::Values(
                // [10, +inf] with various operators and query value 5
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LT,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LE,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::GT,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::GE,
                        StatsComparison::ALL_MATCH
                ),
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::EQ,
                        StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::NE,
                        StatsComparison::ALL_MATCH
                ),
                // [10, +inf] < 5 -> NONE_MATCH (min=10 >= 5)
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 5.0, OperationType::LT,
                        StatsComparison::NONE_MATCH
                ),
                // [10, +inf] > 20 -> UNKNOWN (min=10 <= 20 but max=inf > 20)
                std::make_tuple(
                        10.0, std::numeric_limits<double>::infinity(), 20.0, OperationType::GT, StatsComparison::UNKNOWN
                )
        )
);

INSTANTIATE_TEST_SUITE_P(
        BothInfinite, BinaryComparisonInfinityTest,
        ::testing::Values(
                // [-inf, +inf]: can only give meaningful results if value is NaN
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::quiet_NaN(), OperationType::EQ, StatsComparison::NONE_MATCH
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::quiet_NaN(), OperationType::NE, StatsComparison::ALL_MATCH
                ),
                // [-inf, +inf]: every other comparison is UNKNOWN
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::LT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::LE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::GT, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::GE, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::EQ, StatsComparison::UNKNOWN
                ),
                std::make_tuple(
                        -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 5.0,
                        OperationType::NE, StatsComparison::UNKNOWN
                )
        )
);

namespace {

template<typename StatsType, typename QueryType, typename Func>
StatsComparison typed_stats_comparator(StatsType min, StatsType max, QueryType query_val, Func&& func) {
    ColumnStatsValues csv{std::optional<Value>{construct_value(min)}, std::optional<Value>{construct_value(max)}};
    Value query = construct_value(query_val);
    return stats_comparator(csv, query, std::forward<Func>(func));
}

} // namespace

// int32_t stats, double query value
TEST(MixedTypeStatsComparator, Int32StatsDoubleQuery) {
    // [1, 10] > 5.5 => UNKNOWN (min=1 <= 5.5 but max=10 > 5.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 5.5, GreaterThanOperator{}), StatsComparison::UNKNOWN);
    // [1, 10] > 10.5 => NONE_MATCH (max=10 <= 10.5)
    ASSERT_EQ(
            typed_stats_comparator(int32_t{1}, int32_t{10}, 10.5, GreaterThanOperator{}), StatsComparison::NONE_MATCH
    );
    // [1, 10] < 0.5 => NONE_MATCH (min=1 >= 0.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 0.5, LessThanOperator{}), StatsComparison::NONE_MATCH);
    // [1, 10] < 10.5 => ALL_MATCH (max=10 < 10.5)
    ASSERT_EQ(typed_stats_comparator(int32_t{1}, int32_t{10}, 10.5, LessThanOperator{}), StatsComparison::ALL_MATCH);
    // [5, 5] == 5.0 => ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int32_t{5}, int32_t{5}, 5.0, EqualsOperator{}), StatsComparison::ALL_MATCH);
    // [5, 5] == 5.5 => NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int32_t{5}, int32_t{5}, 5.5, EqualsOperator{}), StatsComparison::NONE_MATCH);
}

// float stats, int64_t query value
TEST(MixedTypeStatsComparator, FloatStatsInt64Query) {
    // [1.0f, 10.0f] > 5 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{5}, GreaterThanOperator{}), StatsComparison::UNKNOWN);
    // [1.0f, 10.0f] > 11 => NONE_MATCH (max=10 <= 11)
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{11}, GreaterThanOperator{}), StatsComparison::NONE_MATCH);
    // [1.0f, 10.0f] < 0 => NONE_MATCH (min=1 >= 0)
    ASSERT_EQ(typed_stats_comparator(1.0f, 10.0f, int64_t{0}, LessThanOperator{}), StatsComparison::NONE_MATCH);
    // [100.0f, 200.0f] > 50 => ALL_MATCH (min=100 > 50)
    ASSERT_EQ(typed_stats_comparator(100.0f, 200.0f, int64_t{50}, GreaterThanOperator{}), StatsComparison::ALL_MATCH);
}

// double stats, int32_t query value
TEST(MixedTypeStatsComparator, DoubleStatsInt32Query) {
    // [1.5, 10.5] > 10 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{10}, GreaterThanOperator{}), StatsComparison::UNKNOWN);
    // [1.5, 10.5] >= 1 => ALL_MATCH (min=1.5 >= 1)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{1}, GreaterThanEqualsOperator{}), StatsComparison::ALL_MATCH);
    // [1.5, 10.5] <= 11 => ALL_MATCH (max=10.5 <= 11)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{11}, LessThanEqualsOperator{}), StatsComparison::ALL_MATCH);
    // [1.5, 10.5] <= 1 => NONE_MATCH (min=1.5 > 1)
    ASSERT_EQ(typed_stats_comparator(1.5, 10.5, int32_t{1}, LessThanEqualsOperator{}), StatsComparison::NONE_MATCH);
}

// uint32_t stats, int64_t query value (signed/unsigned promotion)
TEST(MixedTypeStatsComparator, Uint32StatsInt64Query) {
    // [100, 200] > 150 => UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{150}, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // [100, 200] > 50 => ALL_MATCH (min=100 > 50)
    ASSERT_EQ(
            typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{50}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // [100, 200] < 50 => NONE_MATCH (min=100 >= 50)
    ASSERT_EQ(
            typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{50}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // [100, 200] > -1 => ALL_MATCH (min=100 > -1, tests signed/unsigned interaction)
    ASSERT_EQ(
            typed_stats_comparator(uint32_t{100}, uint32_t{200}, int64_t{-1}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
}

// int8_t stats, double query value (small integer to double)
TEST(MixedTypeStatsComparator, Int8StatsDoubleQuery) {
    // [-100, 100] > 50.5 => UNKNOWN
    ASSERT_EQ(typed_stats_comparator(int8_t{-100}, int8_t{100}, 50.5, GreaterThanOperator{}), StatsComparison::UNKNOWN);
    // [-100, -50] < -49.5 => ALL_MATCH (max=-50 < -49.5)
    ASSERT_EQ(typed_stats_comparator(int8_t{-100}, int8_t{-50}, -49.5, LessThanOperator{}), StatsComparison::ALL_MATCH);
    // [50, 100] != 25.5 => ALL_MATCH (25.5 < min=50)
    ASSERT_EQ(typed_stats_comparator(int8_t{50}, int8_t{100}, 25.5, NotEqualsOperator{}), StatsComparison::ALL_MATCH);
}

// uint64_t stats, double query value
TEST(MixedTypeStatsComparator, Uint64StatsDoubleQuery) {
    // [1000, 2000] > 1500.5 => UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{1000}, uint64_t{2000}, 1500.5, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // [1000, 2000] > 500.5 => ALL_MATCH (min=1000 > 500.5)
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{1000}, uint64_t{2000}, 500.5, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
}

// int64_t stats, uint64_t query value — exercises the signed/unsigned comparison overloads
TEST(MixedTypeStatsComparator, Int64StatsUint64Query_GreaterThan) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Query has MSB set: any int64 range is not greater → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, GreaterThanOperator{}), StatsComparison::NONE_MATCH);
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-1}, int64_t{100}, U64_MAX, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = uint64(INT64_MAX): max possible int64 is INT64_MAX, not greater → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = uint64(INT64_MAX - 1): INT64_MAX > INT64_MAX-1 → ALL_MATCH when min is also INT64_MAX
    ASSERT_EQ(
            typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX - 1), GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Range spanning the boundary
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX - 1), GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
    // Query = 0: positive range is greater
    ASSERT_EQ(
            typed_stats_comparator(int64_t{1}, int64_t{10}, uint64_t{0}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = 0: negative range is not greater
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = 0: range spanning zero
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-5}, int64_t{5}, uint64_t{0}, GreaterThanOperator{}),
            StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_LessThan) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: any int64 is less → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, LessThanOperator{}), StatsComparison::ALL_MATCH);
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, std::numeric_limits<uint64_t>::max(), LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = uint64(INT64_MAX): range entirely below → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX - 1, uint64_t(I64_MAX), LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = uint64(INT64_MAX): min = INT64_MAX, not less → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), LessThanOperator{}), StatsComparison::NONE_MATCH
    );
    // Query = 0: negative range entirely below → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, LessThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = 0: range starting at 0 → min not less → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_Equals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: no int64 can equal it → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, EqualsOperator{}), StatsComparison::NONE_MATCH);
    // Exact match at INT64_MAX boundary
    ASSERT_EQ(
            typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), EqualsOperator{}), StatsComparison::ALL_MATCH
    );
    // Range contains the query value but is not a point → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), EqualsOperator{}), StatsComparison::UNKNOWN
    );
    // Value outside range → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{11}, EqualsOperator{}), StatsComparison::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_NotEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: every int64 differs → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, NotEqualsOperator{}), StatsComparison::ALL_MATCH);
    // Point range equal to query → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), NotEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Non-point range containing query → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), NotEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_LessThanEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: any int64 ≤ it → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(int64_t{0}, I64_MAX, MSB, LessThanEqualsOperator{}), StatsComparison::ALL_MATCH);
    // Query = uint64(INT64_MAX): max = INT64_MAX ≤ INT64_MAX → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = 0: positive range min > 0 → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{1}, int64_t{10}, uint64_t{0}, LessThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = 0: range starting at 0 → max may exceed → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, LessThanEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeStatsComparator, Int64StatsUint64Query_GreaterThanEquals) {
    constexpr auto I64_MAX = std::numeric_limits<int64_t>::max();
    constexpr auto MSB = uint64_t{1} << 63;
    // Query has MSB set: no int64 ≥ it → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, MSB, GreaterThanEqualsOperator{}), StatsComparison::NONE_MATCH
    );
    // Query = uint64(INT64_MAX): only INT64_MAX itself qualifies
    ASSERT_EQ(
            typed_stats_comparator(I64_MAX, I64_MAX, uint64_t(I64_MAX), GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, I64_MAX, uint64_t(I64_MAX), GreaterThanEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
    // Query = 0: range [0, 10], min ≥ 0 → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{0}, int64_t{10}, uint64_t{0}, GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Query = 0: negative range → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(int64_t{-10}, int64_t{-1}, uint64_t{0}, GreaterThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
}

// uint64_t stats, int64_t query value — exercises the signed/unsigned comparison overloads
TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_GreaterThan) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto I64_MIN = std::numeric_limits<int64_t>::min();
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Stats min has MSB set: always greater than any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, U64_MAX, I64_MAX, GreaterThanOperator{}), StatsComparison::ALL_MATCH);
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, int64_t{0}, GreaterThanOperator{}), StatsComparison::ALL_MATCH);
    // Query negative: any uint64 > negative → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{0}, I64_MIN, GreaterThanOperator{}), StatsComparison::ALL_MATCH
    );
    // Query = INT64_MAX, stats just below: max = INT64_MAX-1 as uint64, not greater → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t(I64_MAX - 1), I64_MAX, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = INT64_MAX, stats spanning the MSB boundary → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, GreaterThanOperator{}), StatsComparison::UNKNOWN
    );
    // Query = 0, stats entirely in shared range: [0, 0] > 0 → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{0}, int64_t{0}, GreaterThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Query = 0: [1, 10] > 0 → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{1}, uint64_t{10}, int64_t{0}, GreaterThanOperator{}),
            StatsComparison::ALL_MATCH
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_LessThan) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    constexpr auto U64_MAX = std::numeric_limits<uint64_t>::max();
    // Stats min has MSB set: uint64 with MSB is never less than any int64 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, U64_MAX, I64_MAX, LessThanOperator{}), StatsComparison::NONE_MATCH);
    // Query negative: no uint64 < negative → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Stats entirely below query in shared range: [0, 4] < 5 → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{4}, int64_t{5}, LessThanOperator{}), StatsComparison::ALL_MATCH
    );
    // Stats spanning the MSB boundary, query = INT64_MAX → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, LessThanOperator{}), StatsComparison::UNKNOWN
    );
    // Point at INT64_MAX: not less → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, LessThanOperator{}),
            StatsComparison::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_Equals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: no int64 can equal → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, EqualsOperator{}), StatsComparison::NONE_MATCH);
    // Query negative: no uint64 equals negative → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, EqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Exact match in shared range
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, EqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Range contains query in shared range → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{50}, EqualsOperator{}), StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_NotEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: every value differs from any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, NotEqualsOperator{}), StatsComparison::ALL_MATCH);
    // Query negative: every uint64 differs → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, NotEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Point range equal to query → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, NotEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_LessThanEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats with MSB set: never ≤ any int64 → NONE_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, LessThanEqualsOperator{}), StatsComparison::NONE_MATCH);
    // Query negative: no uint64 ≤ negative → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{100}, int64_t{-1}, LessThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
    // Max in shared range ≤ query → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{10}, I64_MAX, LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Point at boundary: INT64_MAX ≤ INT64_MAX → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, LessThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Spanning MSB boundary → UNKNOWN
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX - 1), MSB, I64_MAX, LessThanEqualsOperator{}),
            StatsComparison::UNKNOWN
    );
}

TEST(MixedTypeStatsComparator, Uint64StatsInt64Query_GreaterThanEquals) {
    constexpr auto I64_MAX = int64_t{std::numeric_limits<int64_t>::max()};
    constexpr auto I64_MIN = std::numeric_limits<int64_t>::min();
    constexpr auto MSB = uint64_t{1} << 63;
    // Stats min has MSB set: always ≥ any int64 → ALL_MATCH
    ASSERT_EQ(typed_stats_comparator(MSB, MSB, I64_MAX, GreaterThanEqualsOperator{}), StatsComparison::ALL_MATCH);
    // Query negative: any uint64 ≥ negative → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{0}, int64_t{-1}, GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{0}, I64_MIN, GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Point at boundary: INT64_MAX ≥ INT64_MAX → ALL_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t(I64_MAX), uint64_t(I64_MAX), I64_MAX, GreaterThanEqualsOperator{}),
            StatsComparison::ALL_MATCH
    );
    // Max below query in shared range → NONE_MATCH
    ASSERT_EQ(
            typed_stats_comparator(uint64_t{0}, uint64_t{4}, int64_t{5}, GreaterThanEqualsOperator{}),
            StatsComparison::NONE_MATCH
    );
}

INSTANTIATE_TEST_SUITE_P(
        InfinityQueryValue, BinaryComparisonInfinityTest,
        ::testing::Values(
                // Finite range, query value is +inf
                // [1, 10] < +inf -> ALL_MATCH (max=10 < inf)
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::LT,
                        StatsComparison::ALL_MATCH
                ),
                // [1, 10] > +inf -> NONE_MATCH (max=10 <= inf)
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::GT,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] == +inf -> NONE_MATCH (max=10 < inf)
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::EQ,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] != +inf -> ALL_MATCH (max=10 < inf)
                std::make_tuple(
                        1.0, 10.0, std::numeric_limits<double>::infinity(), OperationType::NE,
                        StatsComparison::ALL_MATCH
                ),
                // Finite range, query value is -inf
                // [1, 10] > -inf -> ALL_MATCH (min=1 > -inf)
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::GT,
                        StatsComparison::ALL_MATCH
                ),
                // [1, 10] < -inf -> NONE_MATCH (min=1 >= -inf)
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::LT,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] == -inf -> NONE_MATCH (min=1 > -inf)
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::EQ,
                        StatsComparison::NONE_MATCH
                ),
                // [1, 10] != -inf -> ALL_MATCH (min=1 > -inf)
                std::make_tuple(
                        1.0, 10.0, -std::numeric_limits<double>::infinity(), OperationType::NE,
                        StatsComparison::ALL_MATCH
                )
        )
);
