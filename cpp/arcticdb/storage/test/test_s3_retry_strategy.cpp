/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/s3/s3_retry_strategy.hpp>

#include <aws/core/client/AdaptiveRetryStrategy.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Errors.h>

#include <memory>

namespace arcticdb::storage::s3 {

using Aws::Client::AWSError;
using Aws::Client::CoreErrors;

namespace {

// The error the SDK's ChecksumInterceptor returns on a response checksum mismatch
AWSError<CoreErrors> checksum_mismatch_error() {
    return {CoreErrors::VALIDATION, "", "Response checksums mismatch", false};
}

} // namespace

TEST(S3ChecksumMismatchRetryStrategy, IdentifiesChecksumMismatch) {
    ASSERT_TRUE(is_response_checksum_mismatch(checksum_mismatch_error()));
    ASSERT_TRUE(is_response_checksum_mismatch(Aws::S3::S3Error(checksum_mismatch_error())));
    ASSERT_FALSE(
            is_response_checksum_mismatch(AWSError<CoreErrors>{CoreErrors::VALIDATION, "", "Invalid DNS Label", false})
    );
    ASSERT_FALSE(is_response_checksum_mismatch(
            AWSError<CoreErrors>{CoreErrors::UNKNOWN, "", "Response checksums mismatch", false}
    ));
}

TEST(S3ChecksumMismatchRetryStrategy, UnwrappedStrategiesDoNotRetryChecksumMismatch) {
    // Documents why the wrapper exists: if the SDK starts retrying these itself, it can be removed
    ASSERT_FALSE(Aws::Client::DefaultRetryStrategy(3).ShouldRetry(checksum_mismatch_error(), 0));
    ASSERT_FALSE(Aws::Client::StandardRetryStrategy(3).ShouldRetry(checksum_mismatch_error(), 0));
    ASSERT_FALSE(Aws::Client::AdaptiveRetryStrategy(3).ShouldRetry(checksum_mismatch_error(), 0));
}

TEST(S3ChecksumMismatchRetryStrategy, RetriesChecksumMismatchUpToMaxRetries) {
    for (long max_retries : {0L, 1L, 5L}) {
        ChecksumMismatchRetryStrategy strategy(std::make_shared<Aws::Client::DefaultRetryStrategy>(10), max_retries);
        for (long attempted_retries = 0; attempted_retries < max_retries; ++attempted_retries) {
            ASSERT_TRUE(strategy.ShouldRetry(checksum_mismatch_error(), attempted_retries));
        }
        ASSERT_FALSE(strategy.ShouldRetry(checksum_mismatch_error(), max_retries));
    }
}

TEST(S3ChecksumMismatchRetryStrategy, MaxRetriesCountsRetriesForOtherErrorsToo) {
    // A known limitation: the SDK gives one retry count per request, whatever the errors
    ChecksumMismatchRetryStrategy strategy(std::make_shared<Aws::Client::DefaultRetryStrategy>(10), 1);
    ASSERT_TRUE(strategy.ShouldRetry({CoreErrors::NETWORK_CONNECTION, true}, 0));
    ASSERT_FALSE(strategy.ShouldRetry(checksum_mismatch_error(), 1));
}

TEST(S3ChecksumMismatchRetryStrategy, ChecksumMismatchRetriesCountTowardsTheInnerAttemptLimit) {
    // The wrapped strategies allow 3 attempts, i.e. 2 retries in total
    ChecksumMismatchRetryStrategy standard_strategy(std::make_shared<Aws::Client::StandardRetryStrategy>(3), 5);
    ChecksumMismatchRetryStrategy adaptive_strategy(std::make_shared<Aws::Client::AdaptiveRetryStrategy>(3), 5);
    for (const ChecksumMismatchRetryStrategy* strategy : {&standard_strategy, &adaptive_strategy}) {
        ASSERT_EQ(strategy->GetMaxAttempts(), 3);
        ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 0));
        ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 1));
        ASSERT_FALSE(strategy->ShouldRetry(checksum_mismatch_error(), 2));
    }
}

TEST(S3ChecksumMismatchRetryStrategy, DelegatesOtherErrorsToTheInnerStrategy) {
    ChecksumMismatchRetryStrategy strategy(std::make_shared<Aws::Client::DefaultRetryStrategy>(3), 5);
    ASSERT_TRUE(strategy.ShouldRetry({CoreErrors::NETWORK_CONNECTION, true}, 0));
    ASSERT_FALSE(strategy.ShouldRetry({CoreErrors::NETWORK_CONNECTION, true}, 3));
    ASSERT_FALSE(strategy.ShouldRetry({CoreErrors::ACCESS_DENIED, false}, 0));
    ASSERT_FALSE(strategy.ShouldRetry({CoreErrors::VALIDATION, "", "Invalid DNS Label", false}, 0));
}

} // namespace arcticdb::storage::s3
