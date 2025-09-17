/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/pipeline/query.hpp>

#include <arcticdb/pipeline/test/test_container.hpp>

struct BitsetForIndex : public testing::TestWithParam<bool> {
    bool is_read_operation() const { return GetParam(); }
};

TEST_P(BitsetForIndex, DynamicSchemaStrictlyBefore) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(3, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(NumericIndex{0}, NumericIndex{2});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ(bitset->count(), 0);
}

TEST_P(BitsetForIndex, DynamicSchemaStrictlyAfter) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(0, 2);
    container.seg().set_range(3, 4);
    IndexRange rg(NumericIndex{5}, NumericIndex{7});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ(bitset->count(), 0);
}

TEST_P(BitsetForIndex, DynamicSchemaMiddle) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(0, 2);
    container.seg().set_range(5, 7);
    IndexRange rg(NumericIndex{3}, NumericIndex{4});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ(bitset->count(), 0);
}

TEST_P(BitsetForIndex, DynamicSchemaOverlapBegin) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(2, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(NumericIndex{1}, NumericIndex{3});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ((*bitset)[0], true);
    ASSERT_EQ(bitset->count(), 1);
}

TEST_P(BitsetForIndex, DynamicSchemaOverlapEnd) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(2, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(NumericIndex{6}, NumericIndex{8});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ((*bitset)[1], true);
    ASSERT_EQ(bitset->count(), 1);
}

INSTANTIATE_TEST_SUITE_P(BitsetForIndexTests, BitsetForIndex, testing::Values(true, false));
