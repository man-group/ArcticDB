#include <gtest/gtest.h>
#include <vector>
#include <cstdint>
#include <limits>
#include <arcticdb/codec/compression/rle.hpp>


namespace arcticdb {

class RLETest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RLETest, EmptyInput) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    const int *input = nullptr;
    rle_compress(input, 0, dict, runs);

    EXPECT_TRUE(dict.empty());
    EXPECT_TRUE(runs.empty());

    rle_decompress(dict, runs, output);
    EXPECT_TRUE(output.empty());
}

TEST_F(RLETest, SingleValue) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    const int input[] = {42};
    rle_compress(input, 1, dict, runs);

    EXPECT_EQ(dict.size(), 1);
    EXPECT_EQ(runs.size(), 1);
    EXPECT_EQ(dict[0], 42);
    EXPECT_EQ(runs[0], 0);

    rle_decompress(dict, runs, output);
    EXPECT_EQ(output.size(), 1);
    EXPECT_EQ(output[0], 42);
}

TEST_F(RLETest, RepeatedValues) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    const int input[] = {1, 1, 1, 1, 1};
    rle_compress(input, 5, dict, runs);

    EXPECT_EQ(dict.size(), 1);
    EXPECT_EQ(runs.size(), 5);
    EXPECT_EQ(dict[0], 1);
    for (const auto &run : runs) {
        EXPECT_EQ(run, 0);
    }

    rle_decompress(dict, runs, output);
    EXPECT_EQ(output.size(), 5);
    for (const auto &val : output) {
        EXPECT_EQ(val, 1);
    }
}

TEST_F(RLETest, AlternatingValues) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    const int input[] = {1, 2, 1, 2, 1};
    rle_compress(input, 5, dict, runs);

    // Should create a new dictionary entry each time the value changes
    EXPECT_EQ(dict.size(), 5);
    EXPECT_EQ(runs.size(), 5);

    // Dictionary should contain each value in sequence when it changes
    std::vector<int> expected_dict = {1, 2, 1, 2, 1};
    EXPECT_EQ(dict, expected_dict);

    // Runs should increment each time we add to dictionary
    std::vector<uint16_t> expected_runs = {0, 1, 2, 3, 4};
    EXPECT_EQ(runs, expected_runs);

    rle_decompress(dict, runs, output);
    EXPECT_EQ(output.size(), 5);
    for (size_t i = 0; i < output.size(); ++i) {
        EXPECT_EQ(output[i], input[i]);
    }
}


TEST_F(RLETest, MixedPattern) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    const int input[] = {1, 1, 2, 2, 2, 3, 4, 4, 4, 1};
    rle_compress(input, 10, dict, runs);

    EXPECT_EQ(dict.size(), 5);
    EXPECT_EQ(runs.size(), 10);

    std::vector<int> expected_dict = {1, 2, 3, 4, 1};
    std::vector<uint16_t> expected_runs = {0, 0, 1, 1, 1, 2, 3, 3, 3, 4};

    EXPECT_EQ(dict, expected_dict);
    EXPECT_EQ(runs, expected_runs);

    rle_decompress(dict, runs, output);
    EXPECT_EQ(output.size(), 10);
    for (size_t i = 0; i < output.size(); ++i) {
        EXPECT_EQ(output[i], input[i]);
    }
}

TEST_F(RLETest, InvalidInput) {
    std::vector<int> dict;
    std::vector<uint16_t> runs;
    std::vector<int> output;

    // Test null pointer with non-zero size
    EXPECT_THROW(rle_compress<int>(nullptr, 1, dict, runs), std::invalid_argument);

    // Test decompression with empty dictionary but non-empty runs
    runs.push_back(0);
    EXPECT_THROW(rle_decompress(dict, runs, output), std::invalid_argument);
}

TEST_F(RLETest, DifferentTypes) {
    // Test with double
    std::vector<double> dict_d;
    std::vector<uint16_t> runs_d;
    std::vector<double> output_d;

    const double input_d[] = {1.5, 1.5, 2.7, 3.14};
    rle_compress(input_d, 4, dict_d, runs_d);

    EXPECT_EQ(dict_d.size(), 3);
    EXPECT_EQ(runs_d.size(), 4);

    rle_decompress(dict_d, runs_d, output_d);
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_DOUBLE_EQ(output_d[i], input_d[i]);
    }
}

} // namespace arcticdb