/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/s3/S3Client.h>
#include <s3_storage.pb.h>

namespace arcticdb::proto {
namespace s3_storage = arcticc::pb2::s3_storage_pb2;
}

namespace arcticdb::storage::s3 {

class S3ApiInstance;

class S3StorageTool {
  public:
    using Config = arcticdb::proto::s3_storage::Config;
    S3StorageTool(const Config& conf);

    template<class Visitor>
    void iterate_bucket(Visitor&& visitor, const std::string& prefix = std::string());

    void delete_bucket(const std::string& prefix = std::string());

    std::vector<std::string> list_bucket(const std::string& prefix = std::string());

    void set_object(const std::string& key, const std::string& data);

    std::string get_object(const std::string& key);

    size_t get_file_size(const std::string& key);
    std::pair<size_t, size_t> get_prefix_info(const std::string& prefix = std::string());

    void delete_object(const std::string& key);

  private:
    std::shared_ptr<S3ApiInstance> s3_api_;
    Aws::S3::S3Client s3_client_;
    std::string bucket_name_;
};

} // namespace arcticdb::storage::s3
