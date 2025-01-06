/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/s3/S3Client.h>

#include <arcticdb/storage/s3/s3_client_wrapper.hpp>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>


namespace arcticdb::storage::s3{

// A real S3ClientWrapper around Aws::S3::Client, which executes actual requests to S3 storage.
class RealS3Client : public S3ClientWrapper {
public:
    template<typename ...Args>
    RealS3Client(Args && ...args):s3_client(std::forward<Args>(args)...){};

    S3Result<std::monostate> head_object(const std::string& s3_object_name, const std::string& bucket_name) const override;

    S3Result<Segment> get_object(const std::string& s3_object_name, const std::string& bucket_name) const override;

    S3Result<std::monostate> put_object(
            const std::string& s3_object_name,
            Segment& segment,
            const std::string& bucket_name,
            PutHeader header = PutHeader::NONE) override;

    S3Result<DeleteOutput> delete_objects(
            const std::vector<std::string>& s3_object_names,
            const std::string& bucket_name) override;

    S3Result<ListObjectsOutput> list_objects(
            const std::string& prefix,
            const std::string& bucket_name,
            const std::optional<std::string> continuation_token) const override;
private:
    Aws::S3::S3Client s3_client;
};

}
