/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>

#include <aws/s3/S3Errors.h>

namespace arcticdb::storage{

using namespace object_store_utils;

namespace s3 {

std::optional<Aws::S3::S3Error> S3ClientTestWrapper::has_failure_trigger(const std::string& bucket_name) const {
    bool static_failures_enabled = ConfigsMap::instance()->get_int("S3ClientTestWrapper.EnableFailures", 0) == 1;
    // Check if mock failures are enabled
    if (!static_failures_enabled) {
        return std::nullopt;
    }

    // Get target buckets (if not set or "all", affects all buckets)
    auto failure_buckets_str = ConfigsMap::instance()->get_string("S3ClientTestWrapper.FailureBucket", "all");
    
    if (failure_buckets_str != "all") {
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
    auto error_code = ConfigsMap::instance()->get_int("S3ClientTestWrapper.ErrorCode", static_cast<int>(Aws::S3::S3Errors::NETWORK_CONNECTION));
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
        const std::string& s3_object_name,
        const std::string &bucket_name) const {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return {*maybe_error};
    }
    

    return actual_client_->head_object(s3_object_name, bucket_name);
}

S3Result<Segment> S3ClientTestWrapper::get_object(
        const std::string &s3_object_name,
        const std::string &bucket_name) const {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return {*maybe_error};
    }

    return actual_client_->get_object(s3_object_name, bucket_name);
}

folly::Future<S3Result<Segment>> S3ClientTestWrapper::get_object_async(
    const std::string &s3_object_name,
    const std::string &bucket_name) const {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return folly::makeFuture<S3Result<Segment>>({*maybe_error});
    }

    return actual_client_->get_object_async(s3_object_name, bucket_name);
}

S3Result<std::monostate> S3ClientTestWrapper::put_object(
        const std::string &s3_object_name,
        Segment &segment,
        const std::string &bucket_name,
        PutHeader header) {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return {*maybe_error};
    }

    return actual_client_->put_object(s3_object_name, segment, bucket_name, header);
}

S3Result<DeleteObjectsOutput> S3ClientTestWrapper::delete_objects(
        const std::vector<std::string>& s3_object_names,
        const std::string& bucket_name) {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return {*maybe_error};
    }


    return actual_client_->delete_objects(s3_object_names, bucket_name);
}

folly::Future<S3Result<std::monostate>> S3ClientTestWrapper::delete_object(
    const std::string& s3_object_name,
    const std::string& bucket_name) {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return folly::makeFuture<S3Result<std::monostate>>({*maybe_error});
    }

    return actual_client_->delete_object(s3_object_name, bucket_name);
}

S3Result<ListObjectsOutput> S3ClientTestWrapper::list_objects(
        const std::string& name_prefix,
        const std::string& bucket_name,
        const std::optional<std::string>& continuation_token) const {
    auto maybe_error = has_failure_trigger(bucket_name);
    if (maybe_error.has_value()) {
        return {*maybe_error};
    }

    return actual_client_->list_objects(name_prefix, bucket_name, continuation_token);
}

}

}
