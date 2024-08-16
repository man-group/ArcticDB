/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/s3/S3Client.h>

#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/storage_mock_client.hpp>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>

namespace arcticdb::storage::s3 {

struct S3Key {
  std::string bucket_name;
  std::string s3_object_name;

  bool operator<(const S3Key& other) const {
    return std::tie(bucket_name, s3_object_name) <
           std::tie(other.bucket_name, other.s3_object_name);
  }
};

// A mock S3ClientWrapper which can simulate failures.
// The MockS3Client stores the segments in memory to simulate regular S3 behavior for
// unit tests. The MockS3Client can simulate storage failures by using the
// get_failure_trigger for s3_object_names.
class MockS3Client : public S3ClientWrapper {
public:
  MockS3Client() {}

  // Can be used to trigger a simulated failure inside MockS3Client. For example:
  // auto object_to_trigger_put_failure = get_failure_trigger("test",
  // StorageOperation::WRITE, Aws::S3::S3Errors::NETWORK_FAILURE, false);
  // mock_s3_client.put_object(object_to_trigger_put_failure, segment, bucket_name); //
  // This will return a network failure.
  //
  // The returned name looks like
  // "{s3_object_name}#Failure_{operation_to_fail}_{error_to_fail_with}_{retryable}".
  // For example: "symbol_1#Failure_Delete_99_1" will trigger a delete failure with code
  // 99 which is retryable.
  static std::string get_failure_trigger(const std::string& s3_object_name,
                                         StorageOperation operation_to_fail,
                                         Aws::S3::S3Errors error_to_fail_with,
                                         bool retryable = true);

  S3Result<std::monostate> head_object(const std::string& s3_object_name,
                                       const std::string& bucket_name) const override;

  S3Result<Segment> get_object(const std::string& s3_object_name,
                               const std::string& bucket_name) const override;

  S3Result<std::monostate> put_object(const std::string& s3_object_name,
                                      Segment&& segment,
                                      const std::string& bucket_name) override;

  S3Result<DeleteOutput> delete_objects(const std::vector<std::string>& s3_object_names,
                                        const std::string& bucket_name) override;

  S3Result<ListObjectsOutput>
  list_objects(const std::string& prefix, const std::string& bucket_name,
               const std::optional<std::string> continuation_token) const override;

private:
  std::map<S3Key, Segment> s3_contents;
};

} // namespace arcticdb::storage::s3
