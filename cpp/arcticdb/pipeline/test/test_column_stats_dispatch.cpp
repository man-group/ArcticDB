#include <gtest/gtest.h>
#include <arcticdb/pipeline/column_stats_dispatch.hpp>

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