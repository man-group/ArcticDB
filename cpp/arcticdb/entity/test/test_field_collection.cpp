/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/field_collection.hpp>

namespace arcticdb {
TEST(FieldCollection, Iterator) {
    FieldCollection coll;
    coll.add_field(scalar_field(DataType::UINT32, "thing1"));
    coll.add_field(scalar_field(DataType::UINT16, "thing2"));
    coll.add_field(scalar_field(DataType::INT32, "thing3"));
    coll.add_field(scalar_field(DataType::INT8, "thing4"));

    auto it = coll.begin();
    ASSERT_EQ(it->type().data_type(), DataType::UINT32);
    ASSERT_EQ(it->name(), "thing1");

    ++it;
    ASSERT_EQ(it->type().data_type(), DataType::UINT16);
    ASSERT_EQ(it->name(), "thing2");

    ++it;
    ASSERT_EQ(it->type().data_type(), DataType::INT32);
    ASSERT_EQ(it->name(), "thing3");

    ++it;
    ASSERT_EQ(it->type().data_type(), DataType::INT8);
    ASSERT_EQ(it->name(), "thing4");
}
} // namespace arcticdb