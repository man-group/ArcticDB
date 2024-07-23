#include <gtest/gtest.h>

#include <arcticdb/version/version_map_batch_methods.hpp>

TEST(StreamVersionData, SpecificVersion) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{SpecificVersionQuery{VersionId(12), false}, false};
    stream_version_data.react(query_1);
    VersionQuery query_2{SpecificVersionQuery{VersionId(4), false}, false};
    stream_version_data.react(query_2);
    ASSERT_EQ(stream_version_data.count_, 2);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_DOWNTO);
    ASSERT_EQ(stream_version_data.load_param_.load_until_version_, 4);
}

TEST(StreamVersionData, SpecificVersionReversed) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data(VersionQuery{SpecificVersionQuery{VersionId(4), false}, false});
    VersionQuery query_2{SpecificVersionQuery{VersionId(12), false}, false};
    stream_version_data.react(query_2);
    ASSERT_EQ(stream_version_data.count_, 2);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_DOWNTO);
    ASSERT_EQ(stream_version_data.load_param_.load_until_version_, 4);
}

TEST(StreamVersionData, Timestamp) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{TimestampVersionQuery{timestamp(12), false}, false};
    stream_version_data.react(query_1);
    VersionQuery query_2{TimestampVersionQuery{timestamp(4), false}, false};
    stream_version_data.react(query_2);
    ASSERT_EQ(stream_version_data.count_, 2);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_FROM_TIME);
    ASSERT_EQ(stream_version_data.load_param_.load_from_time_, 4);
}

TEST(StreamVersionData, TimestampUnordered) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{TimestampVersionQuery{timestamp(3), false}, false};
    stream_version_data.react(query_1);
    VersionQuery query_2{TimestampVersionQuery{timestamp(7), false}, false};
    stream_version_data.react(query_2);
    VersionQuery query_3{TimestampVersionQuery{timestamp(4), false}, false};
    stream_version_data.react(query_3);
    ASSERT_EQ(stream_version_data.count_, 3);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_FROM_TIME);
    ASSERT_EQ(stream_version_data.load_param_.load_from_time_, 3);
}

TEST(StreamVersionData, Latest) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{std::monostate{}, false};
    stream_version_data.react(query_1);
    ASSERT_EQ(stream_version_data.count_, 1);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_LATEST_UNDELETED);
    ASSERT_EQ(stream_version_data.load_param_.load_until_version_.has_value(), false);
}

TEST(StreamVersionData, SpecificToTimestamp) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{SpecificVersionQuery{VersionId(12), false}, false};
    stream_version_data.react(query_1);
    VersionQuery query_2{TimestampVersionQuery{timestamp(3), false}, false};
    stream_version_data.react(query_2);
    ASSERT_EQ(stream_version_data.count_, 2);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_UNDELETED);
    ASSERT_EQ(stream_version_data.load_param_.load_until_version_.has_value(), false);
    ASSERT_EQ(stream_version_data.load_param_.load_from_time_.has_value(), false);
}

TEST(StreamVersionData, TimestampToSpecific) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    StreamVersionData stream_version_data;
    VersionQuery query_1{TimestampVersionQuery{timestamp(3), false}, false};
    stream_version_data.react(query_1);
    VersionQuery query_2{SpecificVersionQuery{VersionId(12), false}, false};
    stream_version_data.react(query_2);
    ASSERT_EQ(stream_version_data.count_, 2);
    ASSERT_EQ(stream_version_data.load_param_.load_type_, LoadType::LOAD_UNDELETED);
    ASSERT_EQ(stream_version_data.load_param_.load_until_version_.has_value(), false);
    ASSERT_EQ(stream_version_data.load_param_.load_from_time_.has_value(), false);
}