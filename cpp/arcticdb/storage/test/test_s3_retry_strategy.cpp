/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/s3/s3_retry_strategy.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <aws/core/client/AdaptiveRetryStrategy.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include <memory>

namespace arcticdb::storage::s3 {

using Aws::Client::AWSError;
using Aws::Client::CoreErrors;

namespace {

// The error the SDK's ChecksumInterceptor returns on a response checksum mismatch
AWSError<CoreErrors> checksum_mismatch_error() {
    return {CoreErrors::VALIDATION, "", "Response checksums mismatch", false};
}

std::shared_ptr<Aws::Client::RetryStrategy> wrap(std::shared_ptr<Aws::Client::RetryStrategy> inner) {
    return std::make_shared<ChecksumMismatchRetryStrategy>(std::move(inner));
}

} // namespace

TEST(S3ChecksumMismatchRetryStrategy, IdentifiesChecksumMismatch) {
    ASSERT_TRUE(is_response_checksum_mismatch(checksum_mismatch_error()));
    ASSERT_FALSE(is_response_checksum_mismatch({CoreErrors::VALIDATION, "ValidationErrorException", "Other", false}));
    ASSERT_FALSE(is_response_checksum_mismatch({CoreErrors::UNKNOWN, "", "Response checksums mismatch", false}));
}

TEST(S3ChecksumMismatchRetryStrategy, UnwrappedStrategiesDoNotRetryChecksumMismatch) {
    // Documents why the wrapper exists: if the SDK starts retrying these itself, it can be removed
    ASSERT_FALSE(Aws::Client::DefaultRetryStrategy(3).ShouldRetry(checksum_mismatch_error(), 0));
    ASSERT_FALSE(Aws::Client::StandardRetryStrategy(3).ShouldRetry(checksum_mismatch_error(), 0));
}

TEST(S3ChecksumMismatchRetryStrategy, RetriesChecksumMismatchUpToTheInnerMaxAttempts) {
    // 3 retries means 4 attempts in total
    auto strategy = wrap(std::make_shared<Aws::Client::DefaultRetryStrategy>(3));
    ASSERT_EQ(strategy->GetMaxAttempts(), 4);
    ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 0));
    ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 2));
    ASSERT_FALSE(strategy->ShouldRetry(checksum_mismatch_error(), 3));
}

TEST(S3ChecksumMismatchRetryStrategy, WorksWithStandardAndAdaptiveStrategies) {
    for (auto strategy :
         {wrap(std::make_shared<Aws::Client::StandardRetryStrategy>(3)),
          wrap(std::make_shared<Aws::Client::AdaptiveRetryStrategy>(3))}) {
        ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 0));
        ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 1));
        ASSERT_FALSE(strategy->ShouldRetry(checksum_mismatch_error(), 2));
    }
}

TEST(S3ChecksumMismatchRetryStrategy, DelegatesOtherErrorsToTheInnerStrategy) {
    auto strategy = wrap(std::make_shared<Aws::Client::DefaultRetryStrategy>(3));
    ASSERT_TRUE(strategy->ShouldRetry({CoreErrors::NETWORK_CONNECTION, true}, 0));
    ASSERT_FALSE(strategy->ShouldRetry({CoreErrors::NETWORK_CONNECTION, true}, 3));
    ASSERT_FALSE(strategy->ShouldRetry({CoreErrors::ACCESS_DENIED, false}, 0));
}

TEST(S3ChecksumMismatchRetryStrategy, ReportsTheInnerStrategyName) {
    ASSERT_STREQ(
            wrap(std::make_shared<Aws::Client::AdaptiveRetryStrategy>(3))->GetStrategyName(),
            Aws::Client::AdaptiveRetryStrategy(3).GetStrategyName()
    );
}

TEST(S3ChecksumMismatchRetryStrategy, InstalledOnTheS3ClientConfiguration) {
    S3ApiInstance::instance();
    arcticdb::proto::s3_storage::Config s3_config;
    auto client_configuration = get_s3_config_and_set_env_var(s3_config);
    auto* strategy = dynamic_cast<ChecksumMismatchRetryStrategy*>(client_configuration.retryStrategy.get());
    ASSERT_NE(strategy, nullptr);
    // Wraps the strategy the SDK would have picked itself
    ASSERT_STREQ(strategy->GetStrategyName(), Aws::Client::InitRetryStrategy()->GetStrategyName());
    ASSERT_TRUE(strategy->ShouldRetry(checksum_mismatch_error(), 0));
}

} // namespace arcticdb::storage::s3
