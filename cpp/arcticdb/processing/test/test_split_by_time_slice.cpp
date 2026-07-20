#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>

#include <random>

using namespace arcticdb;
using namespace std::ranges;
namespace {
std::vector<RangesAndKey> generate_ranges_and_keys(
        std::span<const TimestampRange> time_ranges, size_t col_slice_count
) {
    std::vector<RangesAndKey> res;
    res.reserve(time_ranges.size() * col_slice_count);
    RowRange current_row_range{0, 5};
    ColRange current_col_range{1, 2};
    for (const TimestampRange& time_range : time_ranges) {
        for (size_t col_slice_idx = 0; col_slice_idx < col_slice_count; ++col_slice_idx) {
            res.emplace_back(
                    current_row_range,
                    current_col_range,
                    AtomKeyBuilder()
                            .start_index(time_range.first)
                            .end_index(time_range.second)
                            .build<KeyType::TABLE_DATA>("t")
            );
            current_col_range = ColRange{current_col_range.second, current_col_range.second + 1};
        }
        current_row_range = RowRange{current_row_range.second, current_row_range.second + 5};
        current_col_range = ColRange{1, 2};
    }
    return res;
}

} // namespace

class TestSplitByTimeSlice : public testing::Test {
  protected:
    TestSplitByTimeSlice() : g(42) {}
    void shuffle(std::span<RangesAndKey> input) { std::ranges::shuffle(input, g); }
    std::mt19937 g;
};

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{20, 30}, TimestampRange{40, 50}, TimestampRange{60, 70}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0}, {1}, {2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{20, 30}, TimestampRange{40, 50}, TimestampRange{60, 70}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {2, 3}, {4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, DisjointTimeRanges_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{20, 30}, TimestampRange{40, 50}, TimestampRange{60, 70}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{15, 20}, TimestampRange{19, 25}, TimestampRange{30, 50}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0}, {1, 2}, {2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{15, 20}, TimestampRange{19, 25}, TimestampRange{30, 50}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {2, 3, 4, 5}, {4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleSharedValueTwoRowSlices_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{15, 20}, TimestampRange{19, 25}, TimestampRange{30, 50}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2}, {3, 4, 5, 6, 7, 8}, {6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{20, 36}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{20, 36}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansThreeRowSlices_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{20, 36}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5, 6, 7, 8}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{14, 16}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{14, 16}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5, 6, 7}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SharedValueSpansFourRowSlices_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15}, TimestampRange{14, 15}, TimestampRange{14, 15}, TimestampRange{14, 16}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {9, 10, 11}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, Chain_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 5}, TimestampRange{4, 10}, TimestampRange{9, 15}, TimestampRange{14, 20}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {1, 2}, {2, 3}, {3}}};
    const std::vector<std::vector<size_t>> result = structure_by_time_slice(input);
    ASSERT_TRUE(std::ranges::equal(result, expected));
}

TEST_F(TestSplitByTimeSlice, Chain_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 5}, TimestampRange{4, 10}, TimestampRange{9, 15}, TimestampRange{14, 20}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1, 2, 3}, {2, 3, 4, 5}, {4, 5, 6, 7}, {6, 7}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, Chain_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 5}, TimestampRange{4, 10}, TimestampRange{9, 15}, TimestampRange{14, 20}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{
            {{0, 1, 2, 3, 4, 5}, {3, 4, 5, 6, 7, 8}, {6, 7, 8, 9, 10, 11}, {9, 10, 11}}
    };
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, SingleRowSlice_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{TimestampRange{0, 10}};
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 1> expected{{{0, 1, 2}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, AdjacentTimeRangesShareNoValue_OneColPerRowSlice) {
    constexpr static std::array time_ranges{TimestampRange{0, 10}, TimestampRange{10, 20}};
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0}, {1}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, TwoSeparateSharedValueGroups_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10}, TimestampRange{9, 15}, TimestampRange{30, 40}, TimestampRange{39, 45}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 4> expected{{{0, 1}, {1}, {2, 3}, {3}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, MinimalSeamOverlap_OneColPerRowSlice) {
    constexpr static std::array time_ranges{TimestampRange{0, 5}, TimestampRange{4, 10}};
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 2> expected{{{0, 1}, {1}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, ChainOfFiveRowSlices_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 5},
            TimestampRange{4, 10},
            TimestampRange{9, 15},
            TimestampRange{14, 20},
            TimestampRange{19, 25}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 5> expected{{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, IsolatedThenCliqueThenIsolatedThenClique_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 10},
            TimestampRange{15, 20},
            TimestampRange{19, 20},
            TimestampRange{19, 25},
            TimestampRange{30, 40},
            TimestampRange{45, 50},
            TimestampRange{49, 55}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 6> expected{{{0}, {1, 2, 3}, {3}, {4}, {5, 6}, {6}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15},
            TimestampRange{14, 15},
            TimestampRange{14, 20},
            TimestampRange{19, 25},
            TimestampRange{24, 30},
            TimestampRange{29, 35}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 5> expected{{{0, 1, 2}, {2, 3}, {3, 4}, {4, 5}, {5}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_TwoColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15},
            TimestampRange{14, 15},
            TimestampRange{14, 20},
            TimestampRange{19, 25},
            TimestampRange{24, 30},
            TimestampRange{29, 35}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 2);
    shuffle(input);
    const std::array<std::vector<size_t>, 5> expected{
            {{0, 1, 2, 3, 4, 5}, {4, 5, 6, 7}, {6, 7, 8, 9}, {8, 9, 10, 11}, {10, 11}}
    };
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, CliqueThenChain_ThreeColsPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15},
            TimestampRange{14, 15},
            TimestampRange{14, 20},
            TimestampRange{19, 25},
            TimestampRange{24, 30},
            TimestampRange{29, 35}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 3);
    shuffle(input);
    const std::array<std::vector<size_t>, 5> expected{
            {{0, 1, 2, 3, 4, 5, 6, 7, 8},
             {6, 7, 8, 9, 10, 11},
             {9, 10, 11, 12, 13, 14},
             {12, 13, 14, 15, 16, 17},
             {15, 16, 17}}
    };
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, TwoCliquesShareBridgingRowSlice_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{10, 15},
            TimestampRange{14, 15},
            TimestampRange{14, 18},
            TimestampRange{17, 18},
            TimestampRange{17, 22}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 3> expected{{{0, 1, 2}, {2, 3, 4}, {4}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, ChainInterleavedWithClique_OneColPerRowSlice) {
    constexpr static std::array time_ranges{
            TimestampRange{0, 5},
            TimestampRange{4, 10},
            TimestampRange{9, 15},
            TimestampRange{14, 20},
            TimestampRange{30, 35},
            TimestampRange{34, 35},
            TimestampRange{34, 40}
    };
    std::vector<RangesAndKey> input = generate_ranges_and_keys(time_ranges, 1);
    shuffle(input);
    const std::array<std::vector<size_t>, 6> expected{{{0, 1}, {1, 2}, {2, 3}, {3}, {4, 5, 6}, {6}}};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}

TEST_F(TestSplitByTimeSlice, EmptyInput) {
    std::array<RangesAndKey, 0> input{};
    constexpr static std::array<std::vector<size_t>, 0> expected{};
    ASSERT_TRUE(std::ranges::equal(structure_by_time_slice(input), expected));
}
