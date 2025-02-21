/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <vector>
#include <span>
#include <arcticdb/column_store/statistics.hpp>

TEST(FieldStatsTest, IntegralStatisticsBasic) {
    using namespace arcticdb;

    std::vector<int64_t> data{1, 2, 3, 2, 1, 4, 5, 3};
    auto field_stats = generate_numeric_statistics<int64_t>(std::span(data));

    EXPECT_TRUE(field_stats.has_min());
    EXPECT_TRUE(field_stats.has_max());
    EXPECT_TRUE(field_stats.has_unique());

    EXPECT_EQ(field_stats.get_min<int64_t>(), 1);
    EXPECT_EQ(field_stats.get_max<int64_t>(), 5);
    EXPECT_EQ(field_stats.unique_count_, 5);
    EXPECT_EQ(field_stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(FieldStatsTest, IntegralStatisticsSingleValue) {
    using namespace arcticdb;
    std::vector<int32_t> data{42, 42, 42, 42};
    auto field_stats = generate_numeric_statistics<int32_t>(std::span(data));

    EXPECT_TRUE(field_stats.has_min());
    EXPECT_TRUE(field_stats.has_max());
    EXPECT_TRUE(field_stats.has_unique());

    EXPECT_EQ(field_stats.get_min<int32_t>(), 42);
    EXPECT_EQ(field_stats.get_max<int32_t>(), 42);
    EXPECT_EQ(field_stats.unique_count_, 1);
    EXPECT_EQ(field_stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(FieldStatsTest, StringStatisticsBasic) {
    using namespace arcticdb;
    std::vector<uint64_t> data{0x123, 0x456, 0x123, 0x789};
    auto field_stats = generate_string_statistics(std::span(data));

    EXPECT_FALSE(field_stats.has_min());
    EXPECT_FALSE(field_stats.has_max());
    EXPECT_TRUE(field_stats.has_unique());

    EXPECT_EQ(field_stats.unique_count_, 3);
    EXPECT_EQ(field_stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(FieldStatsTest, FieldStatsImplConstruction) {
    using namespace arcticdb;
    FieldStatsImpl stats(100u, UniqueCountType::PRECISE);

    EXPECT_FALSE(stats.has_min());
    EXPECT_FALSE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());
    EXPECT_TRUE(stats.unique_count_is_precise());
    EXPECT_EQ(stats.unique_count_, 100u);
}

TEST(FieldStatsTest, FieldStatsImplFullConstruction) {
    using namespace arcticdb;
    FieldStatsImpl stats(static_cast<int64_t>(1), static_cast<int64_t>(100), 50u, UniqueCountType::PRECISE);

    EXPECT_TRUE(stats.has_min());
    EXPECT_TRUE(stats.has_max());
    EXPECT_TRUE(stats.has_unique());
    EXPECT_TRUE(stats.unique_count_is_precise());

    EXPECT_EQ(stats.get_max<int64_t>(), 100);
    EXPECT_EQ(stats.get_min<int64_t>(), 1);
    EXPECT_EQ(stats.unique_count_, 50u);
}

TEST(FieldStatsTest, EmptyStringStatistics) {
    using namespace arcticdb;
    std::vector<uint64_t> data;
    auto field_stats = generate_string_statistics(std::span(data));

    EXPECT_FALSE(field_stats.has_min());
    EXPECT_FALSE(field_stats.has_max());
    EXPECT_TRUE(field_stats.has_unique());

    EXPECT_EQ(field_stats.unique_count_, 0);
    EXPECT_EQ(field_stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(FieldStatsTest, EmptyIntegralStatistics) {
    using namespace arcticdb;
    std::vector<int64_t> data;
    auto field_stats = generate_numeric_statistics<int64_t>(std::span(data));

    EXPECT_FALSE(field_stats.has_min());
    EXPECT_FALSE(field_stats.has_max());
    EXPECT_FALSE(field_stats.has_unique());

    EXPECT_EQ(field_stats.unique_count_, 0);
    EXPECT_EQ(field_stats.unique_count_precision_, UniqueCountType::PRECISE);
}

TEST(FieldStatsTest, ComposeIntegerStats) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<int64_t>(100);
    stats1.set_min<int64_t>(10);
    stats1.set_unique(5, UniqueCountType::PRECISE);

    FieldStatsImpl stats2;
    stats2.set_max<int64_t>(50);
    stats2.set_min<int64_t>(20);
    stats2.set_unique(3, UniqueCountType::PRECISE);

    stats1.compose<int64_t>(stats2);

    EXPECT_TRUE(stats1.has_max());
    EXPECT_TRUE(stats1.has_min());
    EXPECT_TRUE(stats1.has_unique());
    EXPECT_EQ(stats1.get_max<int64_t>(), 100);
    EXPECT_EQ(stats1.get_min<int64_t>(), 10);
    EXPECT_EQ(stats1.unique_count_, 8);
}

TEST(FieldStatsTest, ComposeFloatStats) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<float>(100.5f);
    stats1.set_min<float>(10.5f);
    stats1.set_unique(5, UniqueCountType::PRECISE);

    FieldStatsImpl stats2;
    stats2.set_max<float>(50.5f);
    stats2.set_min<float>(5.5f);
    stats2.set_unique(3, UniqueCountType::PRECISE);

    stats1.compose<float>(stats2);

    EXPECT_TRUE(stats1.has_max());
    EXPECT_TRUE(stats1.has_min());
    EXPECT_TRUE(stats1.has_unique());
    EXPECT_FLOAT_EQ(stats1.get_max<float>(), 100.5f);
    EXPECT_FLOAT_EQ(stats1.get_min<float>(), 5.5f);
    EXPECT_EQ(stats1.unique_count_, 8);
}

TEST(FieldStatsTest, ComposePartialTemplatedStats) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<double>(100.5);

    FieldStatsImpl stats2;
    stats2.set_min<double>(5.5);

    stats1.compose<double>(stats2);

    EXPECT_TRUE(stats1.has_max());
    EXPECT_TRUE(stats1.has_min());
    EXPECT_DOUBLE_EQ(stats1.get_max<double>(), 100.5);
    EXPECT_DOUBLE_EQ(stats1.get_min<double>(), 5.5);
}

TEST(FieldStatsTest, ComposeEmptyTemplatedStats) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<int32_t>(100);
    stats1.set_min<int32_t>(10);
    stats1.set_unique(5, UniqueCountType::PRECISE);

    FieldStatsImpl empty_stats;
    stats1.compose<int32_t>(empty_stats);

    EXPECT_TRUE(stats1.has_max());
    EXPECT_TRUE(stats1.has_min());
    EXPECT_TRUE(stats1.has_unique());
    EXPECT_EQ(stats1.get_max<int32_t>(), 100);
    EXPECT_EQ(stats1.get_min<int32_t>(), 10);
    EXPECT_EQ(stats1.unique_count_, 5);
}

TEST(FieldStatsTest, ComposeNegativeNumbers) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<int64_t>(-10);
    stats1.set_min<int64_t>(-100);

    FieldStatsImpl stats2;
    stats2.set_max<int64_t>(-20);
    stats2.set_min<int64_t>(-50);

    stats1.compose<int64_t>(stats2);

    EXPECT_TRUE(stats1.has_max());
    EXPECT_TRUE(stats1.has_min());
    EXPECT_EQ(stats1.get_max<int64_t>(), -10);
    EXPECT_EQ(stats1.get_min<int64_t>(), -100);
}

TEST(FieldStatsTest, ComposeMismatchingPrecisionTemplated) {
    using namespace arcticdb;
    FieldStatsImpl stats1;
    stats1.set_max<int32_t>(100);
    stats1.set_min<int32_t>(10);
    stats1.set_unique(5, UniqueCountType::PRECISE);

    FieldStatsImpl stats2;
    stats2.set_max<int32_t>(200);
    stats2.set_min<int32_t>(20);
    stats2.set_unique(3, UniqueCountType::HYPERLOGLOG);

    EXPECT_THROW(stats1.compose<int32_t>(stats2), std::exception);
}