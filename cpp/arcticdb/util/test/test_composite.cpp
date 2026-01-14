/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/composite.hpp>

struct TestThing {
    int info_;

    explicit TestThing(int val) : info_(val) {}
};

TEST(Composite, TestFold) {
    using namespace arcticdb;
    TestThing single_thing(3);
    Composite<TestThing> c1{std::move(single_thing)};
    int res = c1.fold([](int i, const TestThing& thing) { return i + thing.info_; }, 0);
    ASSERT_EQ(res, 3);
    res = c1.fold([](int i, const TestThing& thing) { return i + thing.info_; }, 4);
    ASSERT_EQ(res, 7);
    std::vector<TestThing> more_things{TestThing{1}, TestThing{4}, TestThing{3}};
    Composite<TestThing> c2(std::move(more_things));
    res = c2.fold([](int i, const TestThing& thing) { return i + thing.info_; }, 0);
    ASSERT_EQ(res, 8);
    res = c2.fold([](int i, const TestThing& thing) { return i + thing.info_; }, 2);
    ASSERT_EQ(res, 10);
}

TEST(Composite, IterationSimple) {
    using namespace arcticdb;
    Composite<int> comp;
    for (auto i = 0; i < 10; ++i)
        comp.push_back(i);

    int expected = 0;
    for (auto x : comp.as_range())
        ASSERT_EQ(x, expected++);
}

TEST(Composite, Empty) {
    using namespace arcticdb;
    Composite<int> comp1;
    Composite<int> comp2;
    comp1.push_back(std::move(comp2));
    ASSERT_EQ(comp1.size(), 0);
    ASSERT_TRUE(comp1.empty());

    Composite<int> comp3;
    Composite<int> comp4(0);
    comp3.push_back(std::move(comp4));
    ASSERT_EQ(comp3.size(), 1);
    ASSERT_FALSE(comp3.empty());
}
