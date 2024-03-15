#include <gtest/gtest.h>
#include <arcticdb/codec/encoded_field_collection.hpp>

TEST(EncodedFieldCollection, AddField) {
    using namespace arcticdb;
    EncodedFieldCollection fields;
    auto* field1 = fields.add_field(3);
    auto* block1 = field1->mutable_ndarray()->add_shapes();
    block1->set_in_bytes(1);
    auto* block2 = field1->mutable_ndarray()->add_values();
    block2->set_in_bytes(2);
    auto* block3 = field1->mutable_ndarray()->add_values();
    block3->set_in_bytes(3);
    auto* field2 = fields.add_field(2);
    auto* block4 = field2->mutable_ndarray()->add_values();
    block4->set_in_bytes(4);
    auto* block5 = field2->mutable_ndarray()->add_values();
    block5->set_in_bytes(5);

    const auto& read1 = fields.at(0);
    auto b1 = read1.shapes(0);
    ASSERT_EQ(b1.in_bytes(), 1);
    auto b2 = read1.values(0);
    ASSERT_EQ(b2.in_bytes(), 2);
    auto b3 = read1.values(1);
    ASSERT_EQ(b3.in_bytes(), 3);

    const auto& read2 = fields.at(1);
    auto b4 = read2.values(0);
    ASSERT_EQ(b4.in_bytes(), 4);
    auto b5 = read2.values(1);
    ASSERT_EQ(b5.in_bytes(), 5);
}