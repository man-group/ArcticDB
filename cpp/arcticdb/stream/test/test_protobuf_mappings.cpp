#include <gtest/gtest.h>
#include <arcticdb/stream/protobuf_mappings.hpp>

TEST(FieldStatsTest, ProtoConversion) {
    using namespace arcticdb;
    // Create a FieldStatsImpl with some values
    FieldStatsImpl field_stats;
    field_stats.set_max<int64_t>(100);
    field_stats.set_min<int64_t>(1);
    field_stats.set_unique(50, UniqueCountType::PRECISE);

    // Convert to proto
    arcticdb::proto::encoding::FieldStats msg;
    field_stats_to_proto(field_stats, msg);

    // Verify proto values
    EXPECT_EQ(msg.max(), 100);
    EXPECT_EQ(msg.min(), 1);
    EXPECT_EQ(msg.unique_count(), 50);
    EXPECT_EQ(msg.unique_count_precision(), arcticdb::proto::encoding::FieldStats::PRECISE);
    EXPECT_EQ(msg.set(), field_stats.set_);

    // Convert back to FieldStatsImpl
    FieldStatsImpl roundtrip;
    field_stats_from_proto(msg, roundtrip);

    // Verify roundtrip values
    EXPECT_EQ(roundtrip.max_, field_stats.max_);
    EXPECT_EQ(roundtrip.min_, field_stats.min_);
    EXPECT_EQ(roundtrip.unique_count_, field_stats.unique_count_);
    EXPECT_EQ(roundtrip.unique_count_precision_, field_stats.unique_count_precision_);
    EXPECT_EQ(roundtrip.set_, field_stats.set_);
}

TEST(FieldStatsTest, ProtoConversionHyperLogLog) {
    using namespace arcticdb;
    FieldStatsImpl field_stats;
    field_stats.set_unique(1000, UniqueCountType::HYPERLOGLOG);

    arcticdb::proto::encoding::FieldStats msg;
    field_stats_to_proto(field_stats, msg);

    EXPECT_EQ(msg.unique_count_precision(), arcticdb::proto::encoding::FieldStats::HYPERLOGLOG);

    FieldStatsImpl roundtrip;
    field_stats_from_proto(msg, roundtrip);

    EXPECT_EQ(roundtrip.unique_count_precision_, UniqueCountType::HYPERLOGLOG);
    EXPECT_EQ(roundtrip.unique_count_, 1000);
}

TEST(FieldStatsTest, CreateFromProto) {
    using namespace arcticdb;

    arcticdb::proto::encoding::FieldStats msg;
    msg.set_max(100);
    msg.set_min(1);
    msg.set_unique_count(50);
    msg.set_unique_count_precision(arcticdb::proto::encoding::FieldStats::PRECISE);
    msg.set_set(7); // Example value with multiple flags set

    FieldStatsImpl stats = create_from_proto(msg);

    EXPECT_EQ(stats.max_, 100);
    EXPECT_EQ(stats.min_, 1);
    EXPECT_EQ(stats.unique_count_, 50);
    EXPECT_EQ(stats.unique_count_precision_, UniqueCountType::PRECISE);
    EXPECT_EQ(stats.set_, 7);
}
