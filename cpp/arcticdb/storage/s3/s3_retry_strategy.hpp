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

// The SDK's ChecksumInterceptor reports a response checksum mismatch only through this message. Many other errors are
// also VALIDATION, e.g. "Invalid DNS Label found in URI host"
template<typename ErrorType>
bool is_response_checksum_mismatch(const Aws::Client::AWSError<ErrorType>& error) {
    return static_cast<int>(error.GetErrorType()) == static_cast<int>(Aws::Client::CoreErrors::VALIDATION) &&
           error.GetMessage().find("Response checksums mismatch") != Aws::String::npos;
}

// The SDK marks a response checksum mismatch as not retryable, but it is corruption in transit that a fresh download
// normally fixes. Wraps whichever strategy the SDK picks and retries a mismatch as if it were retryable, while the
// request has been retried fewer than max_retries times and the wrapped strategy's attempt limit (AWS_MAX_ATTEMPTS)
// also allows it. The SDK only gives one retry count per request, covering every error, so retries of a request for
// other errors use up max_retries too. Everything else is forwarded. retry-strategy-interface-guard.patch in the
// aws-sdk-cpp vcpkg overlay fails the build when RetryStrategy changes, as a reminder to update the forwarding here.
class ChecksumMismatchRetryStrategy : public Aws::Client::RetryStrategy {
  public:
    ChecksumMismatchRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> inner, long max_retries);

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
    long max_retries_;
};

} // namespace arcticdb::storage::s3
