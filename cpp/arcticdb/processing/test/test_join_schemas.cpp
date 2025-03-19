/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;
using namespace google::protobuf::util;

class GenerateIndexDescriptorTest : public testing::Test {
protected:
    IndexDescriptorImpl rowcount_0{0, IndexDescriptor::Type::ROWCOUNT};
    IndexDescriptorImpl timestamp_1{1, IndexDescriptor::Type::TIMESTAMP};
    IndexDescriptorImpl timestamp_2{2, IndexDescriptor::Type::TIMESTAMP};
    IndexDescriptorImpl string_1{1, IndexDescriptor::Type::STRING};
};

TEST_F(GenerateIndexDescriptorTest, SingleDescriptor) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2}), timestamp_2);
}

TEST_F(GenerateIndexDescriptorTest, IdenticalDescriptors) {
    ASSERT_EQ(generate_index_descriptor({timestamp_2, timestamp_2}), timestamp_2);
}

TEST_F(GenerateIndexDescriptorTest, SameTypeDifferentFieldCount) {
    ASSERT_THROW(generate_index_descriptor({timestamp_2, timestamp_1}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({timestamp_1, timestamp_2}), SchemaException);
}

TEST_F(GenerateIndexDescriptorTest, SameFieldCountDifferentType) {
    ASSERT_THROW(generate_index_descriptor({timestamp_1, string_1}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({string_1, timestamp_1}), SchemaException);
}

TEST_F(GenerateIndexDescriptorTest, DifferentFieldCountDifferentType) {
    ASSERT_THROW(generate_index_descriptor({timestamp_1, rowcount_0}), SchemaException);
    ASSERT_THROW(generate_index_descriptor({rowcount_0, timestamp_1}), SchemaException);
}