/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_retry_strategy.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::storage::s3 {

ChecksumMismatchRetryStrategy::ChecksumMismatchRetryStrategy(
        std::shared_ptr<Aws::Client::RetryStrategy> inner, long max_retries
) :
    inner_(std::move(inner)),
    max_retries_(max_retries) {
    util::check(inner_ != nullptr, "ChecksumMismatchRetryStrategy requires a strategy to wrap");
}

bool ChecksumMismatchRetryStrategy::ShouldRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attempted_retries
) const {
    if (!is_response_checksum_mismatch(error)) {
        return inner_->ShouldRetry(error, attempted_retries);
    }
    // attempted_retries counts every retry of the request, not only the ones for mismatches: the SDK passes no other
    // count. The wrapped strategy's attempt limit applies too.
    auto retryable = error;
    retryable.SetRetryableType(Aws::Client::RetryableType::RETRYABLE);
    const bool will_retry = attempted_retries < max_retries_ && inner_->ShouldRetry(retryable, attempted_retries);
    log::storage().warn(
            "S3 response checksum mismatch from {} (attempt {}), {}",
            error.GetRemoteHostIpAddress(),
            attempted_retries + 1,
            will_retry ? "retrying" : "giving up"
    );
    return will_retry;
}

long ChecksumMismatchRetryStrategy::CalculateDelayBeforeNextRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attempted_retries
) const {
    return inner_->CalculateDelayBeforeNextRetry(error, attempted_retries);
}

long ChecksumMismatchRetryStrategy::GetMaxAttempts() const { return inner_->GetMaxAttempts(); }

void ChecksumMismatchRetryStrategy::GetSendToken() { inner_->GetSendToken(); }

bool ChecksumMismatchRetryStrategy::HasSendToken() { return inner_->HasSendToken(); }

void ChecksumMismatchRetryStrategy::RequestBookkeeping(const Aws::Client::HttpResponseOutcome& http_response_outcome) {
    inner_->RequestBookkeeping(http_response_outcome);
}

void ChecksumMismatchRetryStrategy::RequestBookkeeping(
        const Aws::Client::HttpResponseOutcome& http_response_outcome,
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& last_error
) {
    inner_->RequestBookkeeping(http_response_outcome, last_error);
}

const char* ChecksumMismatchRetryStrategy::GetStrategyName() const { return inner_->GetStrategyName(); }

} // namespace arcticdb::storage::s3
