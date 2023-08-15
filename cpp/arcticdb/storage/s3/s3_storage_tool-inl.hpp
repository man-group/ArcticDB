/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_S3_STORAGE_TOOL_H_
#error "This should only be included by file_storage.hpp"
#endif

#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

namespace arcticdb::storage::s3 {

template<class Visitor>
void S3StorageTool::iterate_bucket(Visitor&& visitor, const std::string& prefix) {
    Aws::S3::Model::ListObjectsV2Request objects_request;
    objects_request.WithBucket(bucket_name_.c_str());
    if (!prefix.empty())
        objects_request.SetPrefix(prefix.c_str());

    bool more;
    do {
        auto list_objects_outcome = s3_client_.ListObjectsV2(objects_request);

        if (list_objects_outcome.IsSuccess()) {
            const auto& object_list = list_objects_outcome.GetResult().GetContents();
            for (auto const& s3_object : object_list) {
                const auto& key = s3_object.GetKey();
                visitor(key.c_str());
            }
            more = list_objects_outcome.GetResult().GetIsTruncated();
            if (more)
                objects_request.SetContinuationToken(list_objects_outcome.GetResult().GetNextContinuationToken());

        }
        else {
            const auto& error = list_objects_outcome.GetError();
            log::storage().error("Failed to iterate bucket '{}' {}:{}",
                                 bucket_name_,
                                 error.GetExceptionName().c_str(),
                                 error.GetMessage().c_str());
            return;
        }
    } while (more);
}

inline void S3StorageTool::set_object(const std::string& key, const std::string& data) {
    Aws::S3::Model::PutObjectRequest object_request;
    object_request.SetBucket(bucket_name_.c_str());
    object_request.SetKey(key.c_str());
    ARCTICDB_TRACE(log::storage(), "Set s3 key {}", object_request.GetKey().c_str());

    auto body = std::make_shared<Aws::StringStream>();
    body->write(data.data(), data.size());
    object_request.SetBody(body);

    auto put_object_outcome = s3_client_.PutObject(object_request);
    if (!put_object_outcome.IsSuccess()) {
        auto& error = put_object_outcome.GetError();
        util::raise_rte("Failed to write s3 with key '{}' {}: {}",
                        key,
                        error.GetExceptionName().c_str(),
                        error.GetMessage().c_str());
    }
    ARCTICDB_DEBUG(log::storage(), "Wrote key {} with {} bytes of data",
                         key,
                         data.size());
}

inline std::string S3StorageTool::get_object(const std::string& key) {
    Aws::S3::Model::GetObjectRequest object_request;

    object_request.SetBucket(bucket_name_.c_str());
    object_request.SetKey(key.c_str());
    auto get_object_outcome = s3_client_.GetObject(object_request);

    if (get_object_outcome.IsSuccess()) {
        auto& retrieved = get_object_outcome.GetResult().GetBody();
        auto vec = storage::stream_to_vector(retrieved);
        return std::string(vec.data(), vec.size());
    }
    else {
        auto& error = get_object_outcome.GetError();
        log::storage().warn("Failed to find data for key '{}' {}: {}",
                            key,
                            error.GetExceptionName().c_str(),
                            error.GetMessage().c_str());
        return std::string();
    }
}

inline void S3StorageTool::delete_object(const std::string& key) {
    Aws::S3::Model::DeleteObjectRequest object_request;
    object_request.WithBucket(bucket_name_.c_str()).WithKey(key.c_str());

    auto delete_object_outcome = s3_client_.DeleteObject(object_request);
    if (delete_object_outcome.IsSuccess())
        ARCTICDB_DEBUG(log::storage(), "Deleted object with key '{}'", key);
    else {
        const auto& error = delete_object_outcome.GetError();
        log::storage().warn("Failed to delete segment with key '{}': {}",
                            key,
                            error.GetExceptionName().c_str(),
                            error.GetMessage().c_str());
    }
}

inline std::vector<std::string> S3StorageTool::list_bucket(const std::string& prefix) {
    std::vector<std::string> objects;
    iterate_bucket([&](const std::string& str) { objects.push_back(str); }, prefix);
    return objects;
}

inline std::pair<size_t, size_t> S3StorageTool::get_prefix_info(const std::string& prefix) {
    size_t total_size = 0;
    size_t total_items = 0;
    Aws::S3::Model::ListObjectsV2Request objects_request;
    objects_request.WithBucket(bucket_name_.c_str());
    if (!prefix.empty())
        objects_request.SetPrefix(prefix.c_str());

    bool more;
    do {
        auto list_objects_outcome = s3_client_.ListObjectsV2(objects_request);

        if (list_objects_outcome.IsSuccess()) {
            const auto& object_list = list_objects_outcome.GetResult().GetContents();
            for (auto const& s3_object : object_list) {
                total_size += s3_object.GetSize();
                ++total_items;
            }
            more = list_objects_outcome.GetResult().GetIsTruncated();
            if (more)
                objects_request.SetContinuationToken(list_objects_outcome.GetResult().GetNextContinuationToken());

        }
        else {
            const auto& error = list_objects_outcome.GetError();
            log::storage().error("Failed to iterate bucket to get sizes'{}' {}:{}",
                                 bucket_name_,
                                 error.GetExceptionName().c_str(),
                                 error.GetMessage().c_str());
            return {0, 0};
        }
    } while (more);
    return {total_size, total_items};
}

inline size_t S3StorageTool::get_file_size(const std::string& key) {
    Aws::S3::Model::HeadObjectRequest head_request;
    head_request.SetBucket(bucket_name_.c_str());
    head_request.SetKey(key.c_str());

    auto object = s3_client_.HeadObject(head_request);
    if (object.IsSuccess())
    {
        auto file_sz = object.GetResultWithOwnership().GetContentLength();
        ARCTICDB_TRACE(log::storage(), "Size of {}: {}", key, file_sz);
        return file_sz;
    }
    else
    {
        log::storage().error("Head Object error:  {} - {}", object .GetError().GetExceptionName(),
                             object .GetError().GetMessage());
        return 0;
    }
}


inline void S3StorageTool::delete_bucket(const std::string& prefix) {
    iterate_bucket([&](const std::string& key) {
                       Aws::S3::Model::DeleteObjectRequest object_request;
                       object_request.WithBucket(bucket_name_.c_str()).WithKey(key.c_str());

                       auto delete_object_outcome = s3_client_.DeleteObject(object_request);
                       if (delete_object_outcome.IsSuccess())
                           ARCTICDB_DEBUG(log::storage(), "Deleted object with key '{}'", key);
                       else {
                           const auto& error = delete_object_outcome.GetError();
                           log::storage().warn("Failed to delete object with key '{}' {}:{}",
                                               key,
                                               error.GetExceptionName().c_str(),
                                               error.GetMessage().c_str());
                       }
                   },
                   prefix);
}

} // namespace arcticdb::storage::s3
