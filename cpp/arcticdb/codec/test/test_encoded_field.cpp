/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb/codec/protobuf_mappings.hpp>

TEST(EncodedField, ScalarBlocks) {
    using namespace arcticdb;
    EncodedFieldCollection coll;
    coll.reserve(calc_field_bytes(4), 1);
    auto* field_ptr = coll.add_field(4);
    auto& field = *field_ptr;
    auto* v1 = field.add_values(EncodingVersion::V1);
    v1->mutable_codec()->mutable_lz4()->acceleration_ = 1;
    auto* v2 = field.add_values(EncodingVersion::V1);
    v2->mutable_codec()->mutable_lz4()->acceleration_ = 2;
    auto* v3 = field.add_values(EncodingVersion::V1);
    v3->mutable_codec()->mutable_lz4()->acceleration_ = 3;
    auto* v4 = field.add_values(EncodingVersion::V1);
    v4->mutable_codec()->mutable_lz4()->acceleration_ = 4;

    ASSERT_EQ(field.values_size(), 4);
    ASSERT_EQ(field.shapes_size(), 0);

    auto expected = 1;
    for (const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        ++expected;
    }
    field.validate();
}

TEST(EncodedField, OldStyleShapes) {
    using namespace arcticdb;
    EncodedFieldCollection coll;
    coll.reserve(calc_field_bytes(8), 1);
    auto* field_ptr = coll.add_field(8);
    auto& field = *field_ptr;
    auto* s1 = field.add_shapes();
    s1->mutable_codec()->mutable_lz4()->acceleration_ = 1;
    auto* v1 = field.add_values(EncodingVersion::V1);
    v1->mutable_codec()->mutable_lz4()->acceleration_ = 2;
    auto* s2 = field.add_shapes();
    s2->mutable_codec()->mutable_lz4()->acceleration_ = 3;
    auto* v2 = field.add_values(EncodingVersion::V1);
    v2->mutable_codec()->mutable_lz4()->acceleration_ = 4;
    auto* s3 = field.add_shapes();
    s3->mutable_codec()->mutable_lz4()->acceleration_ = 5;
    auto* v3 = field.add_values(EncodingVersion::V1);
    v3->mutable_codec()->mutable_lz4()->acceleration_ = 6;
    auto* s4 = field.add_shapes();
    s4->mutable_codec()->mutable_lz4()->acceleration_ = 7;
    auto* v4 = field.add_values(EncodingVersion::V1);
    v4->mutable_codec()->mutable_lz4()->acceleration_ = 8;

    ASSERT_EQ(field.values_size(), 4);
    ASSERT_EQ(field.shapes_size(), 4);

    auto expected = 2;
    for (const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        expected += 2;
    }

    expected = 1;
    for (const auto& shape : field.shapes()) {
        ASSERT_EQ(shape.codec().lz4().acceleration_, expected);
        expected += 2;
    }
    field.validate();
}

TEST(EncodedField, OldStyleShapesEnterShapesFirst) {
    using namespace arcticdb;
    EncodedFieldCollection coll;
    coll.reserve(calc_field_bytes(8), 1);
    auto* field_ptr = coll.add_field(8);
    auto& field = *field_ptr;
    auto* s1 = field.add_shapes();
    s1->mutable_codec()->mutable_lz4()->acceleration_ = 1;
    auto* s2 = field.add_shapes();
    s2->mutable_codec()->mutable_lz4()->acceleration_ = 3;
    auto* s3 = field.add_shapes();
    s3->mutable_codec()->mutable_lz4()->acceleration_ = 5;
    auto* s4 = field.add_shapes();
    s4->mutable_codec()->mutable_lz4()->acceleration_ = 7;

    auto* v1 = field.add_values(EncodingVersion::V1);
    v1->mutable_codec()->mutable_lz4()->acceleration_ = 2;
    auto* v2 = field.add_values(EncodingVersion::V1);
    v2->mutable_codec()->mutable_lz4()->acceleration_ = 4;
    auto* v3 = field.add_values(EncodingVersion::V1);
    v3->mutable_codec()->mutable_lz4()->acceleration_ = 6;
    auto* v4 = field.add_values(EncodingVersion::V1);
    v4->mutable_codec()->mutable_lz4()->acceleration_ = 8;

    ASSERT_EQ(field.values_size(), 4);
    ASSERT_EQ(field.shapes_size(), 4);

    auto expected = 2;
    for (const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        expected += 2;
    }

    expected = 1;
    for (const auto& shape : field.shapes()) {
        ASSERT_EQ(shape.codec().lz4().acceleration_, expected);
        expected += 2;
    }
    field.validate();
}

TEST(EncodedField, NewStyleShapes) {
    using namespace arcticdb;
    EncodedFieldCollection coll;
    coll.reserve(calc_field_bytes(5), 1);
    auto* field_ptr = coll.add_field(5);
    auto& field = *field_ptr;
    auto* s1 = field.add_shapes();
    s1->mutable_codec()->mutable_lz4()->acceleration_ = 1;
    auto* v1 = field.add_values(EncodingVersion::V2);
    v1->mutable_codec()->mutable_lz4()->acceleration_ = 2;
    auto* v2 = field.add_values(EncodingVersion::V2);
    v2->mutable_codec()->mutable_lz4()->acceleration_ = 3;
    auto* v3 = field.add_values(EncodingVersion::V2);
    v3->mutable_codec()->mutable_lz4()->acceleration_ = 4;
    auto* v4 = field.add_values(EncodingVersion::V2);
    v4->mutable_codec()->mutable_lz4()->acceleration_ = 5;

    ASSERT_EQ(field.values_size(), 4);
    ASSERT_EQ(field.shapes_size(), 1);

    auto expected = 2;
    for (const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        ++expected;
    }

    for (const auto& shape : field.shapes()) {
        ASSERT_EQ(shape.codec().lz4().acceleration_, 1);
    }
    field.validate();
}

TEST(EncodedFieldTest, Roundtrip) {
    using namespace arcticdb;
    arcticdb::proto::encoding::EncodedField original_proto;
    original_proto.set_offset(123);
    original_proto.set_num_elements(456);

    auto* ndarray = original_proto.mutable_ndarray();
    ndarray->set_items_count(789);

    auto* shape_block = ndarray->add_shapes();
    shape_block->set_in_bytes(100);
    shape_block->set_out_bytes(50);
    shape_block->set_hash(1234567890);
    shape_block->set_encoder_version(1);

    auto* shape_block_codec = shape_block->mutable_codec();
    shape_block_codec->mutable_zstd()->set_level(5);
    shape_block_codec->mutable_zstd()->set_is_streaming(true);

    auto* value_block = ndarray->add_values();
    value_block->set_in_bytes(200);
    value_block->set_out_bytes(100);
    value_block->set_hash(987654321);
    value_block->set_encoder_version(2);

    auto* value_block_codec = value_block->mutable_codec();
    value_block_codec->mutable_lz4()->set_acceleration(10);

    ndarray->set_sparse_map_bytes(1024);
    EncodedFieldCollection collection;
    auto encoded_field = collection.add_field(2);
    encoded_field_from_proto(original_proto, *encoded_field);

    arcticdb::proto::encoding::EncodedField roundtrip_proto;
    copy_encoded_field_to_proto(*encoded_field, roundtrip_proto);

    ASSERT_TRUE(original_proto.has_ndarray());
    ASSERT_TRUE(roundtrip_proto.has_ndarray());

    const auto& original_ndarray = original_proto.ndarray();
    const auto& roundtrip_ndarray = roundtrip_proto.ndarray();

    ASSERT_EQ(original_ndarray.items_count(), roundtrip_ndarray.items_count());
    ASSERT_EQ(original_ndarray.sparse_map_bytes(), roundtrip_ndarray.sparse_map_bytes());

    ASSERT_EQ(original_ndarray.shapes_size(), roundtrip_ndarray.shapes_size());
    ASSERT_EQ(original_ndarray.values_size(), roundtrip_ndarray.values_size());

    for (int i = 0; i < original_ndarray.shapes_size(); ++i) {
        const auto& original_shape = original_ndarray.shapes(i);
        const auto& roundtrip_shape = roundtrip_ndarray.shapes(i);

        ASSERT_EQ(original_shape.in_bytes(), roundtrip_shape.in_bytes());
        ASSERT_EQ(original_shape.out_bytes(), roundtrip_shape.out_bytes());
        ASSERT_EQ(original_shape.hash(), roundtrip_shape.hash());
        ASSERT_EQ(original_shape.encoder_version(), roundtrip_shape.encoder_version());

        ASSERT_TRUE(original_shape.has_codec());
        ASSERT_TRUE(roundtrip_shape.has_codec());

        const auto& original_shape_codec = original_shape.codec();
        const auto& roundtrip_shape_codec = roundtrip_shape.codec();

        ASSERT_TRUE(original_shape_codec.has_zstd());
        ASSERT_TRUE(roundtrip_shape_codec.has_zstd());

        ASSERT_EQ(original_shape_codec.zstd().level(), roundtrip_shape_codec.zstd().level());
        ASSERT_EQ(original_shape_codec.zstd().is_streaming(), roundtrip_shape_codec.zstd().is_streaming());
    }

    for (int i = 0; i < original_ndarray.values_size(); ++i) {
        const auto& original_value = original_ndarray.values(i);
        const auto& roundtrip_value = roundtrip_ndarray.values(i);

        ASSERT_EQ(original_value.in_bytes(), roundtrip_value.in_bytes());
        ASSERT_EQ(original_value.out_bytes(), roundtrip_value.out_bytes());
        ASSERT_EQ(original_value.hash(), roundtrip_value.hash());
        ASSERT_EQ(original_value.encoder_version(), roundtrip_value.encoder_version());

        ASSERT_TRUE(original_value.has_codec());
        ASSERT_TRUE(roundtrip_value.has_codec());

        const auto& original_value_codec = original_value.codec();
        const auto& roundtrip_value_codec = roundtrip_value.codec();

        ASSERT_TRUE(original_value_codec.has_lz4());
        ASSERT_TRUE(roundtrip_value_codec.has_lz4());

        ASSERT_EQ(original_value_codec.lz4().acceleration(), roundtrip_value_codec.lz4().acceleration());
    }
}