/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/mock/s3_mock_client.hpp>

#include <aws/s3/S3Errors.h>

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

std::optional<Aws::S3::S3Error> S3ClientTestWrapper::has_failure_trigger(
        const std::string& s3_object_name, const std::string& bucket_name, StorageOperation operation
) const {
    if (auto error = has_bucket_failure_trigger(bucket_name)) {
        return error;
    }
    return has_object_failure_trigger(s3_object_name, operation);
}

std::optional<Aws::S3::S3Error> S3ClientTestWrapper::has_bucket_failure_trigger(const std::string& bucket_name) const {
    // Check if mock failures are enabled
    if (ConfigsMap::instance()->get_int("S3ClientTestWrapper.EnableFailures", 0) != 1) {
        return std::nullopt;
    }

    // Get target buckets (if not set or "all", affects all buckets)
    if (auto failure_buckets_str = ConfigsMap::instance()->get_string("S3ClientTestWrapper.FailureBucket", "all");
        failure_buckets_str != "all") {
        // Split the comma-separated bucket names and check if current bucket is in the list
        std::istringstream bucket_stream(failure_buckets_str);
        std::string target_bucket;
        bool bucket_found = false;

        while (std::getline(bucket_stream, target_bucket, ',')) {
            // Trim whitespace
            target_bucket.erase(0, target_bucket.find_first_not_of(" \t"));
            target_bucket.erase(target_bucket.find_last_not_of(" \t") + 1);

            if (target_bucket == bucket_name) {
                bucket_found = true;
                break;
            }
        }

        if (!bucket_found) {
            return std::nullopt;
        }
    }

    // Get error configuration
    auto error_code = ConfigsMap::instance()->get_int(
            "S3ClientTestWrapper.ErrorCode", static_cast<int>(Aws::S3::S3Errors::NETWORK_CONNECTION)
    );
    auto retryable = ConfigsMap::instance()->get_int("S3ClientTestWrapper.ErrorRetryable", 0) == 1;

    auto failure_error_ = Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(
            static_cast<Aws::S3::S3Errors>(error_code),
            "SimulatedFailure",
            "Simulated failure from environment variables",
            retryable
    ));

    return failure_error_;
}

S3Result<std::monostate> S3ClientTestWrapper::head_object(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    if (auto maybe_error = has_failure_trigger(s3_object_name, bucket_name, StorageOperation::EXISTS)) {
        return {*maybe_error};
    }

    return actual_client_->head_object(s3_object_name, bucket_name);
}

S3Result<Segment> S3ClientTestWrapper::get_object(const std::string& s3_object_name, const std::string& bucket_name)
        const {
    if (auto maybe_error = has_failure_trigger(s3_object_name, bucket_name, StorageOperation::READ)) {
        return {*maybe_error};
    }

    return actual_client_->get_object(s3_object_name, bucket_name);
}

folly::Future<S3Result<Segment>> S3ClientTestWrapper::get_object_async(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    if (auto maybe_error = has_failure_trigger(s3_object_name, bucket_name, StorageOperation::READ)) {
        return folly::makeFuture<S3Result<Segment>>({*maybe_error});
    }

    return actual_client_->get_object_async(s3_object_name, bucket_name);
}

S3Result<std::monostate> S3ClientTestWrapper::put_object(
        const std::string& s3_object_name, Segment& segment, const std::string& bucket_name, PutHeader header
) {
    if (auto maybe_bucket_error = has_bucket_failure_trigger(bucket_name)) {
        return {*maybe_bucket_error};
    }
    if (auto maybe_object_error = has_object_failure_trigger(s3_object_name, StorageOperation::WRITE)) {
        if (header == PutHeader::IF_NONE_MATCH) {
            return {not_implemented_error};
        }
        return {*maybe_object_error};
    }

    if (header == PutHeader::IF_NONE_MATCH) {
        if (auto head_result = actual_client_->head_object(s3_object_name, bucket_name); head_result.is_success()) {
            return {precondition_failed_error};
        }
    }

    return actual_client_->put_object(s3_object_name, segment, bucket_name, header);
}

S3Result<DeleteObjectsOutput> S3ClientTestWrapper::delete_objects(
        const std::vector<std::string>& s3_object_names, const std::string& bucket_name
) {
    if (auto maybe_bucket_error = has_bucket_failure_trigger(bucket_name)) {
        return {*maybe_bucket_error};
    }

    for (const auto& s3_object_name : s3_object_names) {
        if (auto maybe_object_error = has_object_failure_trigger(s3_object_name, StorageOperation::DELETE)) {
            return {*maybe_object_error};
        }
    }

    auto result = actual_client_->delete_objects(s3_object_names, bucket_name);
    if (result.is_success()) {
        for (const auto& s3_object_name : s3_object_names) {
            if (has_object_failure_trigger(s3_object_name, StorageOperation::DELETE_LOCAL)) {
                result.get_output().failed_deletes.emplace_back(s3_object_name, "Simulated local delete failure");
            }
        }
    }

    return result;
}

folly::Future<S3Result<std::monostate>> S3ClientTestWrapper::delete_object(
        const std::string& s3_object_name, const std::string& bucket_name
) {
    if (auto maybe_error = has_failure_trigger(s3_object_name, bucket_name, StorageOperation::DELETE)) {
        return folly::makeFuture(S3Result<std::monostate>{*maybe_error});
    } else if (auto maybe_object_error = has_object_failure_trigger(s3_object_name, StorageOperation::DELETE_LOCAL)) {
        return folly::makeFuture(S3Result<std::monostate>{*maybe_object_error});
    }
    return actual_client_->delete_object(s3_object_name, bucket_name);
}

S3Result<ListObjectsOutput> S3ClientTestWrapper::list_objects(
        const std::string& name_prefix, const std::string& bucket_name,
        const std::optional<std::string>& continuation_token
) const {
    if (auto maybe_bucket_error = has_bucket_failure_trigger(bucket_name); maybe_bucket_error.has_value()) {
        return {*maybe_bucket_error};
    }

    auto result = actual_client_->list_objects(name_prefix, bucket_name, continuation_token);

    if (result.is_success()) {
        for (const auto& s3_object_name : result.get_output().s3_object_names) {
            if (auto maybe_object_error = has_object_failure_trigger(s3_object_name, StorageOperation::LIST)) {
                return {*maybe_object_error};
            }
        }
    }

    return result;
}

} // namespace s3

} // namespace arcticdb::storage
