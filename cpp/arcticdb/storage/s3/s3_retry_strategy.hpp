/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/client/RetryStrategy.h>

#include <memory>

namespace arcticdb::storage::s3 {

// Whether the error is the one the SDK's ChecksumInterceptor returns when the checksum in an `x-amz-checksum-*`
// response header does not match the body it received. The SDK gives this error no exception name, so the message is
// the only thing that identifies it.
bool is_response_checksum_mismatch(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error);

// Decorates any SDK retry strategy so that response checksum mismatches are retried. The SDK marks them as not
// retryable, so every built-in strategy gives up on them after the first attempt, even though a fresh download usually
// succeeds. Everything else, including delays, the attempt limit and the adaptive strategy's rate limiting, is
// delegated to the wrapped strategy.
class ChecksumMismatchRetryStrategy : public Aws::Client::RetryStrategy {
  public:
    explicit ChecksumMismatchRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> inner);

    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attempted_retries)
            const override;
    long CalculateDelayBeforeNextRetry(
            const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attempted_retries
    ) const override;
    long GetMaxAttempts() const override;
    void GetSendToken() override;
    bool HasSendToken() override;
    void RequestBookkeeping(const Aws::Client::HttpResponseOutcome& http_response_outcome) override;
    void RequestBookkeeping(
            const Aws::Client::HttpResponseOutcome& http_response_outcome,
            const Aws::Client::AWSError<Aws::Client::CoreErrors>& last_error
    ) override;
    const char* GetStrategyName() const override;

  private:
    std::shared_ptr<Aws::Client::RetryStrategy> inner_;
};

} // namespace arcticdb::storage::s3
