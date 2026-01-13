/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/stream/index.hpp>

TEST(SegmentHeader, WriteAndReadFields) {
    using namespace arcticdb;
    SegmentHeader header{EncodingVersion::V1};
    auto& string_pool_field = header.mutable_string_pool_field(2);
    auto* values = string_pool_field.mutable_ndarray()->add_values(EncodingVersion::V2);
    values->set_in_bytes(23);
    values = string_pool_field.mutable_ndarray()->add_values(EncodingVersion::V2);
    values->set_in_bytes(47);

    const auto& read_string_pool = header.string_pool_field();
    auto& read_values1 = read_string_pool.values(0);
    ASSERT_EQ(read_values1.in_bytes(), 23);
    auto& read_values2 = read_string_pool.values(1);
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

TEST(SegmentHeader, SerializeUnserializeV1) {
    using namespace arcticdb;
    SegmentHeader header{EncodingVersion::V1};
    auto& string_pool_field = header.mutable_string_pool_field(10);
    for (auto i = 0U; i < 5; ++i) {
        auto* shapes = string_pool_field.mutable_ndarray()->add_shapes();
        shapes->set_in_bytes(i + 1);
        shapes->mutable_codec()->mutable_lz4()->acceleration_ = 1;
        auto* values = string_pool_field.mutable_ndarray()->add_values(EncodingVersion::V1);
        values->set_in_bytes(i + 1);
        values->mutable_codec()->mutable_lz4()->acceleration_ = 1;
    }

    auto desc = stream_descriptor(StreamId{"thing"}, stream::RowCountIndex{}, {scalar_field(DataType::UINT8, "ints")});

    auto proto = generate_v1_header(header, desc);
    const auto header_size = proto.ByteSizeLong();
    std::vector<uint8_t> vec(header_size);
    auto read_header = decode_protobuf_header(vec.data(), header_size);

    const auto& string_pool = read_header.proto().string_pool_field();
    auto expected = 1U;
    for (const auto& value : string_pool.ndarray().values()) {
        ASSERT_EQ(value.in_bytes(), expected++);
    }

    expected = 1U;
    for (const auto& shape : string_pool.ndarray().shapes()) {
        ASSERT_EQ(shape.in_bytes(), expected++);
    }
}