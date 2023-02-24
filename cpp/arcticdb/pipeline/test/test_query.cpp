/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/pipeline/query.hpp>

#include <arcticdb/pipeline/test/test_container.hpp>


TEST(BitsetForIndex, DynamicSchemaStrictlyBefore) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(3, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(0, 2);
    std::unique_ptr<util::BitSet> input;
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(container, rg, true, false, std::move(input));
    ASSERT_EQ(bitset->count(), 0);
}

TEST(BitsetForIndex, DynamicSchemaStrictlyAfter) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(0, 2);
    container.seg().set_range(3, 4);
    IndexRange rg(5, 7);
    std::unique_ptr<util::BitSet> input;
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(container, rg, true, false, std::move(input));
    ASSERT_EQ(bitset->count(), 0);
}

TEST(BitsetForIndex, DynamicSchemaMiddle) {
   using namespace arcticdb;
   using namespace arcticdb::pipelines;
   TestContainer container;
   container.seg().set_range(0, 2);
   container.seg().set_range(5, 7);
   IndexRange rg(3, 4);
   std::unique_ptr<util::BitSet> input;
   auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(container, rg, true, false, std::move(input));
   ASSERT_EQ(bitset->count(), 0);
}

TEST(BitsetForIndex, DynamicSchemaOverlapBegin) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(2, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(1, 3);
    std::unique_ptr<util::BitSet> input;
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(container, rg, true, false, std::move(input));
    ASSERT_EQ((*bitset)[0], true);
    ASSERT_EQ(bitset->count(), 1);
}

TEST(BitsetForIndex, DynamicSchemaOverlapEnd) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(2, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(6, 8);
    std::unique_ptr<util::BitSet> input;
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(container, rg, true, false, std::move(input));
    ASSERT_EQ((*bitset)[1], true);
    ASSERT_EQ(bitset->count(), 1);
}
