/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/entity/ref_key.hpp>

TEST(RefKey, Basic) {
    using namespace arcticdb::entity;
    RefKey rk{ "HelloWorld", KeyType::STORAGE_INFO};
    ASSERT_EQ(rk.id(), VariantId("HelloWorld"));
    ASSERT_EQ(rk.type(), KeyType::STORAGE_INFO);
}