/* Copyright 2026 Man Group Operations Limited
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

TEST_P(BitsetForIndex, DynamicSchemaMatchEndIndex) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    TestContainer container;
    container.seg().set_range(2, 4);
    container.seg().set_range(5, 7);
    IndexRange rg(NumericIndex{7}, NumericIndex{7});
    auto bitset = build_bitset_for_index<TestContainer, TimeseriesIndex>(
            container, rg, true, false, is_read_operation(), std::unique_ptr<util::BitSet>{}
    );
    ASSERT_EQ((*bitset)[1], is_read_operation());
    ASSERT_EQ(bitset->count(), is_read_operation() ? 1 : 0);
}

TEST_P(BitsetForIndex, SegmentEndingAtInt64Max) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;
    constexpr timestamp max = std::numeric_limits<timestamp>::max();
    for (const bool dynamic_schema : {true, false}) {
        TestContainer container;
        container.seg().set_range(max - 10, max - 5);
        // The last segment's end_index, max + 1, wraps to INT64_MIN
        container.seg().set_range(max - 5, static_cast<uint64_t>(end_index_from_last_index_value(max)));
        auto bitset_for = [&](timestamp start, timestamp end) {
            return build_bitset_for_index<TestContainer, TimeseriesIndex>(
                    container,
                    IndexRange(NumericIndex{start}, NumericIndex{end}),
                    dynamic_schema,
                    false,
                    is_read_operation(),
                    std::unique_ptr<util::BitSet>{}
            );
        };
        auto at_max = bitset_for(max, max);
        EXPECT_EQ(at_max->count(), 1) << "dynamic_schema=" << dynamic_schema;
        EXPECT_TRUE((*at_max)[1]) << "dynamic_schema=" << dynamic_schema;
        EXPECT_EQ(bitset_for(max - 8, max)->count(), 2) << "dynamic_schema=" << dynamic_schema;
        auto before_last = bitset_for(max - 9, max - 7);
        EXPECT_EQ(before_last->count(), 1) << "dynamic_schema=" << dynamic_schema;
        EXPECT_TRUE((*before_last)[0]) << "dynamic_schema=" << dynamic_schema;
    }
}

TEST(EndIndex, WrapsAtInt64Max) {
    using namespace arcticdb::entity;
    constexpr timestamp max = std::numeric_limits<timestamp>::max();
    constexpr timestamp min = std::numeric_limits<timestamp>::min();
    static_assert(end_index_from_last_index_value(max) == min);
    static_assert(last_index_value_from_end_index(min) == max);
    static_assert(last_index_value_from_end_index(end_index_from_last_index_value(max - 1)) == max - 1);
    static_assert(last_index_value_from_end_index(end_index_from_last_index_value(min + 1)) == min + 1);
    const auto wrapped = segment_index_range(NumericIndex{max - 1}, NumericIndex{min});
    EXPECT_EQ(std::get<NumericIndex>(wrapped.inclusive_end()), max);
    const auto normal = segment_index_range(NumericIndex{0}, NumericIndex{10});
    EXPECT_EQ(std::get<NumericIndex>(normal.inclusive_end()), 9);
}

INSTANTIATE_TEST_SUITE_P(BitsetForIndexTests, BitsetForIndex, testing::Values(true, false));
