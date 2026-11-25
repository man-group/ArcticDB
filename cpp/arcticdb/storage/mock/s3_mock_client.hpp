/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>

namespace arcticdb::storage::s3 {

extern const Aws::S3::S3Error not_found_error;
extern const Aws::S3::S3Error precondition_failed_error;
extern const Aws::S3::S3Error not_implemented_error;

std::optional<Aws::S3::S3Error> has_object_failure_trigger(
        const std::string& s3_object_name, StorageOperation operation
);

struct S3Key {
    std::string bucket_name;
    std::string s3_object_name;

    bool operator<(const S3Key& other) const {
        return std::tie(bucket_name, s3_object_name) < std::tie(other.bucket_name, other.s3_object_name);
    }
};

// The MockS3Client stores the segments in memory to simulate regular S3 behavior for unit tests.
class MockS3Client : public S3ClientInterface {
  public:
    MockS3Client() = default;

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
            const std::string& s3_object_name, const std::string& bucket_name
    ) override;

    S3Result<ListObjectsOutput> list_objects(
            const std::string& prefix, const std::string& bucket_name,
            const std::optional<std::string>& continuation_token
    ) const override;

  private:
    struct MockObject {
        Segment segment;
        ObjectMetadata metadata;
    };

    // Deleted entries stay as std::nullopt so continuation tokens stay stable across concurrent writes/deletes.
    std::map<S3Key, std::optional<MockObject>> s3_contents_;
    uint64_t write_seq_{0};
    mutable std::mutex mutex_; // Used to guard the map.
};

} // namespace arcticdb::storage::s3
