/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/composite.hpp>


struct TestThing {
    int info_;

    explicit TestThing(int val) :
    info_(val) {}
};


TEST(Composite, TestFold) {
    using namespace arcticdb;
    TestThing single_thing(3);
    Composite<TestThing> c1{std::move(single_thing)};
    int res = c1.fold([] (int i, const TestThing& thing) { return i + thing.info_; }, 0);
    ASSERT_EQ(res, 3);
    res = c1.fold([] (int i, const TestThing& thing) { return i + thing.info_; }, 4);
    ASSERT_EQ(res, 7);
    std::vector<TestThing> more_things{ TestThing{1}, TestThing{4}, TestThing{3} };
    Composite<TestThing> c2(std::move(more_things));
    res = c2.fold([] (int i, const TestThing& thing) { return i + thing.info_; }, 0);
    ASSERT_EQ(res, 8);
    res = c2.fold([] (int i, const TestThing& thing) { return i + thing.info_; }, 2);
    ASSERT_EQ(res, 10);
}

TEST(Composite, IterationSimple) {
    using namespace arcticdb;
    Composite<int> comp;
    for(auto i = 0; i < 10; ++i)
        comp.push_back(i);

    int expected = 0;
    for(auto x : comp.as_range())
        ASSERT_EQ(x, expected++);
}
