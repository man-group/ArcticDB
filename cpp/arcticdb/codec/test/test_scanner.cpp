#include <gtest/gtest.h>

#include <arcticdb/codec/scanner.hpp>

namespace arcticdb {
TEST(CalculateScoreTest, EncoderSelection) {
    const size_t original_size = 97;
    const size_t encoder_a_speed = 10;
    const size_t encoder_a_estimated = 113;
    const size_t encoder_b_speed = 20;
    const size_t encoder_b_estimated = 63;

    // At weight 16 the faster encoding is preferred, whereas at
    // weight 18 the smaller encoding is preferred.
    size_t score_a_w16 = calculate_score(encoder_a_speed, encoder_a_estimated, original_size, 16);
    size_t score_b_w16 = calculate_score(encoder_b_speed, encoder_b_estimated, original_size, 16);
    EXPECT_LT(score_a_w16, score_b_w16);

    size_t score_a_w18 = calculate_score(encoder_a_speed, encoder_a_estimated, original_size, 18);
    size_t score_b_w18 = calculate_score(encoder_b_speed, encoder_b_estimated, original_size, 18);
    EXPECT_GT(score_a_w18, score_b_w18);
}
}
