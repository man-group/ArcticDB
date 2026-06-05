/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/storage/s3/s3_stream_buffer.hpp>

#include <aws/s3/S3Client.h>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <boost/interprocess/streams/bufferstream.hpp>

#include <cstdlib>

// GetMessage macro on windows shadows AWS's GetMessage:
// https://github.com/aws/aws-sdk-cpp/issues/402
#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

S3Result<std::monostate> S3ClientImpl::head_object(const std::string& s3_object_name, const std::string& bucket_name)
        const {

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Looking for head of object {}", s3_object_name);
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
    auto outcome = s3_client.HeadObject(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning head of object {}", s3_object_name);
    return {std::monostate()};
}

Aws::IOStreamFactory S3StreamFactory() {
    return [=]() { return Aws::New<S3IOStream>(""); };
}

S3Result<Segment> S3ClientImpl::get_object(const std::string& s3_object_name, const std::string& bucket_name) const {
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Looking for object {}", s3_object_name);
    auto start = util::SysClock::coarse_nanos_since_epoch();
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
    request.SetResponseStreamFactory(S3StreamFactory());
    auto outcome = s3_client.GetObject(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }

    auto& retrieved = dynamic_cast<S3IOStream&>(outcome.GetResult().GetBody());
    auto nanos = util::SysClock::coarse_nanos_since_epoch() - start;
    auto time_taken = double(nanos) / BILLION;
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning object {} in {}s", s3_object_name, time_taken);
    return {Segment::from_buffer(retrieved.get_buffer())};
}

struct GetObjectAsyncHandler {
    std::shared_ptr<folly::Promise<S3Result<Segment>>> promise_;
    timestamp start_;

    GetObjectAsyncHandler(std::shared_ptr<folly::Promise<S3Result<Segment>>>&& promise) :
        promise_(std::move(promise)),
        start_(util::SysClock::coarse_nanos_since_epoch()) {}

    ARCTICDB_MOVE_COPY_DEFAULT(GetObjectAsyncHandler)

    void
    operator()(const Aws::S3::S3Client*, const Aws::S3::Model::GetObjectRequest& request, const Aws::S3::Model::GetObjectOutcome& outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        if (outcome.IsSuccess()) {
            auto& body = const_cast<Aws::S3::Model::GetObjectOutcome&>(outcome).GetResultWithOwnership().GetBody();
            auto& stream = dynamic_cast<S3IOStream&>(body);
            auto nanos = util::SysClock::coarse_nanos_since_epoch() - start_;
            auto time_taken = double(nanos) / BILLION;
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning object {} in {}", request.GetKey(), time_taken);
            promise_->setValue<S3Result<Segment>>({Segment::from_buffer(stream.get_buffer())});
        } else {
            promise_->setValue<S3Result<Segment>>({outcome.GetError()});
        }
    }
};

folly::Future<S3Result<Segment>> S3ClientImpl::get_object_async(
        const std::string& s3_object_name, const std::string& bucket_name
) const {
    auto promise = std::make_shared<folly::Promise<S3Result<Segment>>>();
    auto future = promise->getFuture().via(&async::io_executor());
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
    request.SetResponseStreamFactory(S3StreamFactory());
    ARCTICDB_RUNTIME_DEBUG(log::version(), "Scheduling async read of {}", s3_object_name);
    s3_client.GetObjectAsync(request, GetObjectAsyncHandler{std::move(promise)});
    if (std::getenv("ARCTICDB_VERIFY_ASYNC_READ") == nullptr)
        return future;
    // DIAGNOSTIC (temporary): compare the async-fetched bytes against an immediate SYNCHRONOUS
    // re-read of the same immutable object. Any mismatch proves the async transfer corrupted the
    // bytes in flight (catches even corruption that still happens to lz4-decode).
    return std::move(future).thenValue(
            [this, s3_object_name, bucket_name](S3Result<Segment>&& async_res) -> S3Result<Segment> {
                if (async_res.is_success()) {
                    try {
                        auto sync_res = get_object(s3_object_name, bucket_name);
                        if (sync_res.is_success()) {
                            auto av = async_res.get_output().buffer();
                            auto sv = sync_res.get_output().buffer();
                            if (av.bytes() != sv.bytes()) {
                                log::storage().error(
                                        "ASYNC_READ_VERIFY: SIZE mismatch for {}: async={} sync={}",
                                        s3_object_name,
                                        av.bytes(),
                                        sv.bytes()
                                );
                            } else {
                                const auto n = av.bytes();
                                const auto* a = av.data();
                                const auto* b = sv.data();
                                size_t i = 0;
                                while (i < n && a[i] == b[i])
                                    ++i;
                                if (i < n) {
                                    log::storage().error(
                                            "ASYNC_READ_VERIFY: BYTE MISMATCH for {} at offset {}/{} "
                                            "async=0x{:02x} sync=0x{:02x}",
                                            s3_object_name,
                                            i,
                                            n,
                                            static_cast<unsigned>(a[i]),
                                            static_cast<unsigned>(b[i])
                                    );
                                }
                            }
                        }
                    } catch (const std::exception& e) {
                        log::storage().warn(
                                "ASYNC_READ_VERIFY: sync re-read failed for {}: {}", s3_object_name, e.what()
                        );
                    }
                }
                return std::move(async_res);
            }
    );
}

S3Result<std::monostate> S3ClientImpl::put_object(
        const std::string& s3_object_name, Segment& segment, const std::string& bucket_name, PutHeader header
) {

    ARCTICDB_SUBSAMPLE(S3StorageWritePreamble, 0)
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(s3_object_name.c_str());
    if (header == PutHeader::IF_NONE_MATCH) {
        request.SetIfNoneMatch("*");
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Set s3 key {}", request.GetKey().c_str());
    auto [dst, write_size, buffer] = segment.serialize_header();

    auto body = std::make_shared<boost::interprocess::bufferstream>(reinterpret_cast<char*>(dst), write_size);
    util::check(body->good(), "Overflow of bufferstream with size {}", write_size);
    request.SetBody(body);

    ARCTICDB_SUBSAMPLE(S3StoragePutObject, 0)
    auto outcome = s3_client.PutObject(request);
    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Wrote key '{}', with {} bytes of data", s3_object_name, segment.size());
    return {std::monostate()};
}

S3Result<DeleteObjectsOutput> S3ClientImpl::delete_objects(
        const std::vector<std::string>& s3_object_names, const std::string& bucket_name
) {
    Aws::S3::Model::DeleteObjectsRequest request;
    request.WithBucket(bucket_name.c_str());
    Aws::S3::Model::Delete del_objects;
    for (auto& s3_object_name : s3_object_names) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing s3 object with key {}", s3_object_name);
        del_objects.AddObjects(Aws::S3::Model::ObjectIdentifier().WithKey(s3_object_name.c_str()));
    }

    ARCTICDB_SUBSAMPLE(S3StorageDeleteObjects, 0)
    request.SetDelete(del_objects);

    auto outcome = s3_client.DeleteObjects(request);
    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }

    // AN-256: Per AWS S3 documentation, deleting non-exist objects is not an error, so not handling
    // RemoveOpts.ignores_missing_key_
    std::vector<FailedDelete> failed_deletes;
    for (const auto& failed_key : outcome.GetResult().GetErrors()) {
        failed_deletes.emplace_back(failed_key.GetKey(), failed_key.GetMessage());
    }

    DeleteObjectsOutput result = {failed_deletes};
    return {result};
}

struct DeleteObjectAsyncHandler {
    std::shared_ptr<folly::Promise<S3Result<std::monostate>>> promise_;
    timestamp start_;

    DeleteObjectAsyncHandler(std::shared_ptr<folly::Promise<S3Result<std::monostate>>>&& promise) :
        promise_(std::move(promise)),
        start_(util::SysClock::coarse_nanos_since_epoch()) {}

    ARCTICDB_MOVE_COPY_DEFAULT(DeleteObjectAsyncHandler)

    void
    operator()(const Aws::S3::S3Client*, const Aws::S3::Model::DeleteObjectRequest&, const Aws::S3::Model::DeleteObjectOutcome& outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        if (outcome.IsSuccess()) {
            promise_->setValue<S3Result<std::monostate>>({});
        } else {
            promise_->setValue<S3Result<std::monostate>>({outcome.GetError()});
        }
    }
};

folly::Future<S3Result<std::monostate>> S3ClientImpl::delete_object(
        const std::string& s3_object_name, const std::string& bucket_name
) {
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing s3 object with key {} (async)", s3_object_name);
    auto promise = std::make_shared<folly::Promise<S3Result<std::monostate>>>();
    auto future = promise->getFuture();
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(bucket_name.c_str());
    request.WithKey(s3_object_name);

    s3_client.DeleteObjectAsync(request, DeleteObjectAsyncHandler{std::move(promise)});
    return future;
}

S3Result<ListObjectsOutput> S3ClientImpl::list_objects(
        const std::string& name_prefix, const std::string& bucket_name,
        const std::optional<std::string>& continuation_token
) const {

    ARCTICDB_RUNTIME_DEBUG(
            log::storage(), "Searching for objects in bucket {} with prefix {}", bucket_name, name_prefix
    );
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(bucket_name.c_str());
    request.SetPrefix(name_prefix.c_str());
    if (continuation_token.has_value())
        request.SetContinuationToken(*continuation_token);

    auto outcome = s3_client.ListObjectsV2(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Received object list");

    const auto& result = outcome.GetResult();
    auto next_continuation_token = std::optional<std::string>();
    if (result.GetIsTruncated())
        next_continuation_token = {result.GetNextContinuationToken()};

    auto s3_object_names = std::vector<std::string>();
    auto s3_object_sizes = std::vector<uint64_t>();
    for (const auto& s3_object : result.GetContents()) {
        s3_object_names.emplace_back(s3_object.GetKey());
        s3_object_sizes.emplace_back(s3_object.GetSize());
    }

    return {ListObjectsOutput{std::move(s3_object_names), std::move(s3_object_sizes), next_continuation_token}};
}

} // namespace s3

} // namespace arcticdb::storage