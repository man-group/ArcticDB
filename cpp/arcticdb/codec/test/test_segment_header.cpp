#include <gtest/gtest.h>
#include <arcticdb/codec/segment_header.hpp>

TEST(SegmentHeader, WriteAndReadFields) {
    using namespace arcticdb;
    SegmentHeader header{EncodingVersion::V1};
    auto& string_pool_field = header.mutable_string_pool_field(2);
    auto* values = string_pool_field.mutable_ndarray()->add_values();
    values->set_in_bytes(23);
    values = string_pool_field.mutable_ndarray()->add_values();
    values->set_in_bytes(47);

    const auto& read_string_pool = header.string_pool_field();
    auto& read_values1 = read_string_pool.values(0);
    ASSERT_EQ(read_values1.in_bytes(), 23);
    auto&read_values2 = read_string_pool.values(1);
    ASSERT_EQ(read_values2.in_bytes(), 47);
}

TEST(SegmentHeader, HasFields) {
    using namespace arcticdb;
    SegmentHeader header{EncodingVersion::V1};
    ASSERT_EQ(header.has_index_descriptor_field(), false);
    ASSERT_EQ(header.has_metadata_field(), false);
    ASSERT_EQ(header.has_column_fields(), false);
    ASSERT_EQ(header.has_descriptor_field(), false);
    ASSERT_EQ(header.has_string_pool_field(), false);

    (void)header.mutable_string_pool_field(2);
    ASSERT_EQ(header.has_index_descriptor_field(), false);
    ASSERT_EQ(header.has_metadata_field(), false);
    ASSERT_EQ(header.has_column_fields(), false);
    ASSERT_EQ(header.has_descriptor_field(), false);
    ASSERT_EQ(header.has_string_pool_field(), true);

    (void)header.mutable_descriptor_field(2);
    ASSERT_EQ(header.has_index_descriptor_field(), false);
    ASSERT_EQ(header.has_metadata_field(), false);
    ASSERT_EQ(header.has_column_fields(), false);
    ASSERT_EQ(header.has_descriptor_field(), true);
    ASSERT_EQ(header.has_string_pool_field(), true);

    (void)header.mutable_column_fields(2);
    ASSERT_EQ(header.has_index_descriptor_field(), false);
    ASSERT_EQ(header.has_metadata_field(), false);
    ASSERT_EQ(header.has_column_fields(), true);
    ASSERT_EQ(header.has_descriptor_field(), true);
    ASSERT_EQ(header.has_string_pool_field(), true);
}