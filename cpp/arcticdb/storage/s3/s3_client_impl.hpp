/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/s3/S3Client.h>
#include <arcticdb/storage/s3/s3_client_interface.hpp>

namespace arcticdb::storage::s3 {

class S3ClientImpl : public S3ClientInterface {
  public:
    template<typename... Args>
    S3ClientImpl(Args&&... args) : s3_client(std::forward<Args>(args)...){};

    S3Result<std::monostate> head_object(const std::string& s3_object_name, const std::string& bucket_name)
            const override;

    S3Result<Segment> get_object(const std::string& s3_object_name, const std::string& bucket_name) const override;

    folly::Future<S3Result<Segment>> get_object_async(const std::string& s3_object_name, const std::string& bucket_name)
            const override;

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
    Aws::S3::S3Client s3_client;
};

} // namespace arcticdb::storage::s3
