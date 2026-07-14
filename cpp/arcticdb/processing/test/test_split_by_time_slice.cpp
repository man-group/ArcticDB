#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>

#include <random>

using namespace arcticdb;
using namespace std::ranges;

class TestSplitByTimeSlice : public testing::Test {
  protected:
    TestSplitByTimeSlice() : g(rd()) {}
    void shuffle(std::span<RangesAndKey> input) { std::ranges::shuffle(input, g); }
    std::random_device rd;
    std::mt19937 g;
};

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0}, {1}, {2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 8> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {2, 3}, {4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(20).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(40).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(60).end_index(70).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0}, {1, 2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 8> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1}, {2, 3, 4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(30).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1, 2}, {3, 4, 5, 6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 8> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(20).end_index(36).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5, 6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1, 2, 3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 8> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1, 2, 3, 4, 5, 6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(16).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, Chain_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1}, {1, 2}, {2, 3}}};
    const std::vector<std::vector<size_t>> result = structure_by_time_slice(input);
    ASSERT_TRUE(std::ranges::equal(result, expected));
}

TEST_F(TestSplitByTimeSlice, Chain_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 8> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1, 2, 3}, {2, 3, 4, 5}, {4, 5, 6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, Chain_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1, 2, 3, 4, 5}, {3, 4, 5, 6, 7, 8}, {6, 7, 8, 9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleRowSlice_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 3> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1, 2}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, AdjacentTimeRangesShareNoValue_OneColPerRowSlice) {
    std::array<RangesAndKey, 2> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0}, {1}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, TwoSeparateSharedValueGroups_OneColPerRowSlice) {
    std::array<RangesAndKey, 4> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(9).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(40).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(39).end_index(45).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1}, {2, 3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, MinimalSeamOverlap_OneColPerRowSlice) {
    std::array<RangesAndKey, 2> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(5).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(4).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, ChainOfFiveRowSlices_OneColPerRowSlice) {
    std::array<RangesAndKey, 5> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(100).end_index(105).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(104).end_index(110).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(109).end_index(115).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(114).end_index(120).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(119).end_index(125).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {1, 2}, {2, 3}, {3, 4}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, IsolatedThenCliqueThenIsolatedThenClique_OneColPerRowSlice) {
    std::array<RangesAndKey, 7> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(15).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(40).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(45).end_index(50).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{30, 35},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(49).end_index(55).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0}, {1, 2, 3}, {4}, {5, 6}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_OneColPerRowSlice) {
    std::array<RangesAndKey, 6> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2}, {2, 3}, {3, 4}, {4, 5}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_TwoColsPerRowSlice) {
    std::array<RangesAndKey, 12> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2, 3, 4, 5}, {4, 5, 6, 7}, {6, 7, 8, 9}, {8, 9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_ThreeColsPerRowSlice) {
    std::array<RangesAndKey, 18> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(19).end_index(25).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(24).end_index(30).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{2, 3},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{3, 4},
                    AtomKeyBuilder().start_index(29).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{
            {{0, 1, 2, 3, 4, 5, 6, 7, 8}, {6, 7, 8, 9, 10, 11}, {9, 10, 11, 12, 13, 14}, {12, 13, 14, 15, 16, 17}}
    };
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, TwoCliquesShareBridgingRowSlice_OneColPerRowSlice) {
    std::array<RangesAndKey, 5> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(10).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(18).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(17).end_index(18).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(17).end_index(22).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2}, {2, 3, 4}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, ChainInterleavedWithClique_OneColPerRowSlice) {
    std::array<RangesAndKey, 7> input{{
            RangesAndKey{
                    RowRange{0, 5},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(0).end_index(5).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{5, 10},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(4).end_index(10).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{10, 15},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(9).end_index(15).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{15, 20},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(14).end_index(20).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{20, 25},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(30).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{25, 30},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(34).end_index(35).build<KeyType::TABLE_DATA>("t")
            },
            RangesAndKey{
                    RowRange{30, 35},
                    ColRange{1, 2},
                    AtomKeyBuilder().start_index(34).end_index(40).build<KeyType::TABLE_DATA>("t")
            },
    }};
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {1, 2}, {2, 3}, {4, 5, 6}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, EmptyInput) {
    std::array<RangesAndKey, 0> input{};
    shuffle(input);
    const std::array<std::vector<size_t>, 0> expected{};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}
