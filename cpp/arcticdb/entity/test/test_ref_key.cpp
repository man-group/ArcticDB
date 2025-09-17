/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/entity/ref_key.hpp>

TEST(RefKey, Basic) {
    using namespace arcticdb::entity;
    RefKey rk{"HelloWorld", KeyType::STORAGE_INFO};
    ASSERT_EQ(rk.id(), arcticdb::VariantId("HelloWorld"));
    ASSERT_EQ(rk.type(), KeyType::STORAGE_INFO);
}