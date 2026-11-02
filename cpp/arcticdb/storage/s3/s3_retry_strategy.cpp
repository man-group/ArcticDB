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

namespace {
// Must match the message set in the SDK's smithy/client/features/ChecksumInterceptor.h
constexpr const char* RESPONSE_CHECKSUM_MISMATCH_MESSAGE = "Response checksums mismatch";
} // namespace

bool is_response_checksum_mismatch(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error) {
    return error.GetErrorType() == Aws::Client::CoreErrors::VALIDATION &&
           error.GetMessage() == RESPONSE_CHECKSUM_MISMATCH_MESSAGE;
}

ChecksumMismatchRetryStrategy::ChecksumMismatchRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> inner) :
    inner_(std::move(inner)) {
    util::check(inner_ != nullptr, "ChecksumMismatchRetryStrategy requires a strategy to wrap");
}

bool ChecksumMismatchRetryStrategy::ShouldRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attempted_retries
) const {
    if (is_response_checksum_mismatch(error)) {
        const bool retry = attempted_retries + 1 < inner_->GetMaxAttempts();
        log::storage().warn(
                "S3 response checksum mismatch after {} retries, {}",
                attempted_retries,
                retry ? "retrying" : "giving up"
        );
        return retry;
    }
    return inner_->ShouldRetry(error, attempted_retries);
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
