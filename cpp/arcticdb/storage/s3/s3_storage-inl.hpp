/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_S3_STORAGE_H_
#error "This should only be included by s3_storage.cpp"
#endif

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <boost/interprocess/streams/bufferstream.hpp>
#include <folly/ThreadLocal.h>

#include <arcticdb/storage/s3/detail-inl.hpp>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

namespace fg = folly::gen;

CheckAccessibilityResult do_check_accessibility(const Aws::S3::S3Client& s3_client, const std::string& bucket_name) {
    using namespace Aws::S3;
    using namespace spdlog::level;
    // We cannot easily change the timeouts of the s3_client_, so use async:
    auto future = s3_client.HeadBucketCallable(Model::HeadBucketRequest().WithBucket(bucket_name.c_str()));
    auto wait = std::chrono::milliseconds(ConfigsMap::instance()->get_int("S3Storage.CheckBucketMaxWait", 1000));
    if (future.wait_for(wait) == std::future_status::ready) {
        auto outcome = future.get();
        if (!outcome.IsSuccess()) {
            auto& error = outcome.GetError();
            auto details = fmt::format("HTTP Status: {}. Server response: {}",
                    int(error.GetResponseCode()), error.GetMessage().c_str());

            // HEAD request can't return the error details, so can't use the more detailed error codes.
            switch (error.GetResponseCode()) {
                case Aws::Http::HttpResponseCode::NOT_FOUND:
                    return {err, fmt::format("The specified bucket [{}] does not exist", bucket_name), std::move(details)};
                case Aws::Http::HttpResponseCode::UNAUTHORIZED:
                case Aws::Http::HttpResponseCode::FORBIDDEN:
                    // This is not an error because AWS's ACL scheme might be able to block HEAD'ing the bucket, but
                    // allow accessing the blobs
                    return {warn, "No permission to access the bucket", std::move(details)};
                case Aws::Http::HttpResponseCode::REQUEST_NOT_MADE:
                    return {warn, "Cannot connect to the given server", std::move(details)};
                default:
                    return {warn,
                            "Unexpected error while checking for storage access. "
                            "Please report an issue with below details:",
                            std::move(details)};
            }
        }
        return {debug, "Bucket access check successful"};
    } else {
        return {info, "Unable to determine bucket accessibility within the allotted time"};
    }
}

inline std::string S3Storage::get_key_path(const VariantKey& key) const {
    auto b = FlatBucketizer{};
    auto key_type_dir = key_type_folder(root_folder_, variant_key_type(key));
    return object_path(b.bucketize(key_type_dir, key), key);
    // FUTURE: if we can reuse `detail::do_*` below doing polymorphism instead of templating, then this method is useful
    // to most of them
}

inline void S3Storage::do_write(Composite<KeySegmentPair>&& kvs) {
    detail::do_write_impl(std::move(kvs), root_folder_, bucket_name_, s3_client_, FlatBucketizer{});
}

inline void S3Storage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    detail::do_update_impl(std::move(kvs), root_folder_, bucket_name_, s3_client_, FlatBucketizer{});
}

inline void S3Storage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
    detail::do_read_impl(std::move(ks), visitor, root_folder_, bucket_name_, s3_client_, FlatBucketizer{}, opts);
}

inline void S3Storage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    detail::do_remove_impl(std::move(ks), root_folder_, bucket_name_, s3_client_, FlatBucketizer{});
}

inline void S3Storage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };

    detail::do_iterate_type_impl(key_type, std::move(visitor), root_folder_, bucket_name_, s3_client_, FlatBucketizer{}, std::move(prefix_handler), prefix);
}

inline bool S3Storage::do_key_exists(const VariantKey& key) {
    return detail::do_key_exists_impl(key, root_folder_, bucket_name_, s3_client_, FlatBucketizer{});
}


} // namespace s3

} // namespace arcticdb::storage
