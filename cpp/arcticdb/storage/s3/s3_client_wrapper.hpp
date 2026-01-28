/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/s3/s3_client_interface.hpp>

namespace arcticdb::storage::s3 {

// A wrapper around the actual S3 client which can simulate failures based on the configuration.
// The S3ClientTestWrapper delegates to the real client by default, but can intercept operations
// to simulate failures or track operations for testing purposes.
class S3ClientTestWrapper : public S3ClientInterface {
  public:
    explicit S3ClientTestWrapper(std::unique_ptr<S3ClientInterface> actual_client) :
        actual_client_(std::move(actual_client)) {}

    ~S3ClientTestWrapper() override = default;

    // Can be used to trigger a simulated failure inside S3ClientTestWrapper. For example:
    // auto object_to_trigger_put_failure = get_failure_trigger("test", StorageOperation::WRITE,
    // Aws::S3::S3Errors::NETWORK_FAILURE, false); mock_s3_client.put_object(object_to_trigger_put_failure, segment,
    // bucket_name); // This will return a network failure.
    //
    // The returned name looks like "{s3_object_name}#Failure_{operation_to_fail}_{error_to_fail_with}_{retryable}".
    // For example: "symbol_1#Failure_Delete_99_1" will trigger a delete failure with code 99 which is retryable.
    static std::string get_failure_trigger(
            const std::string& s3_object_name, StorageOperation operation_to_fail, Aws::S3::S3Errors error_to_fail_with,
            bool retryable = true
    );

    [[nodiscard]] S3Result<std::monostate> head_object(
            const std::string& s3_object_name, const std::string& bucket_name
    ) const override;

    [[nodiscard]] S3Result<Segment> get_object(const std::string& s3_object_name, const std::string& bucket_name)
            const override;

    [[nodiscard]] folly::Future<S3Result<Segment>> get_object_async(
            const std::string& s3_object_name, const std::string& bucket_name
    ) const override;

    S3Result<std::monostate> put_object(
            const std::string& s3_object_name, Segment& segment, const std::string& bucket_name,
            PutHeader header = PutHeader::NONE
    ) override;

    S3Result<DeleteObjectsOutput> delete_objects(
            const std::vector<std::string>& s3_object_names, const std::string& bucket_name
    ) override;

    folly::Future<S3Result<std::monostate>> delete_object(
            const std::string& s3_object_names, const std::string& bucket_name
    ) override;

    S3Result<ListObjectsOutput> list_objects(
            const std::string& prefix, const std::string& bucket_name,
            const std::optional<std::string>& continuation_token
    ) const override;

    void add_list_objects_failure_unretryable(Aws::S3::S3Errors error);

  private:
    // Returns error if failures are enabled for the given bucket
    std::optional<Aws::S3::S3Error> has_bucket_failure_trigger(const std::string& bucket_name) const;
    std::optional<Aws::S3::S3Error> has_failure_trigger(
            const std::string& s3_object_name, const std::string& bucket_name, StorageOperation operation
    ) const;

    std::unique_ptr<S3ClientInterface> actual_client_;
    // Failing listing operations based on symbol names isn't sufficient for testing the express bucket fallback
    // mechanism. Allow setting a list of failure modes for list_objects calls. This list will be popped from until
    // empty, at which point the listing operation will succeed.
    mutable std::deque<std::pair<Aws::S3::S3Errors, bool>> list_objects_failures_;
};

} // namespace arcticdb::storage::s3
