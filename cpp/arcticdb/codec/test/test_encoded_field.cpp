/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>

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
    v4 ->mutable_codec()->mutable_lz4()->acceleration_ = 4;

    ASSERT_EQ(field.values_size(), 4);
    ASSERT_EQ(field.shapes_size(), 0);

    auto expected = 1;
    for(const auto& value : field.values()) {
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
    for(const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        expected += 2;
    }

    expected = 1;
    for(const auto& shape : field.shapes()) {
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
    for(const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        expected += 2;
    }

    expected = 1;
    for(const auto& shape : field.shapes()) {
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
    for(const auto& value : field.values()) {
        ASSERT_EQ(value.codec().lz4().acceleration_, expected);
        ++expected;
    }

    for (const auto& shape: field.shapes()) {
        ASSERT_EQ(shape.codec().lz4().acceleration_, 1);
    }
    field.validate();
}