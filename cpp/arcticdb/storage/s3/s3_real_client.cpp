/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_real_client.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>

#include <aws/s3/S3Client.h>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>


#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <boost/interprocess/streams/bufferstream.hpp>

// GetMessage macro on windows shadows AWS's GetMessage:
// https://github.com/aws/aws-sdk-cpp/issues/402
#undef GetMessage

namespace arcticdb::storage{

using namespace object_store_utils;

namespace s3 {

S3Result<std::monostate> RealS3Client::head_object(
        const std::string& s3_object_name,
        const std::string &bucket_name) const {

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

//TODO Use buffer pool once memory profile and lifetime is well understood
struct S3StreamBuffer : public std::streambuf {
    ARCTICDB_NO_MOVE_OR_COPY(S3StreamBuffer)

    S3StreamBuffer() :
#ifdef USE_BUFFER_POOL
            buffer_(BufferPool::instance()->allocate()) {
#else
            buffer_(std::make_shared<Buffer>()) {
#endif
    }

    std::shared_ptr<Buffer> buffer_;
    size_t pos_ = 0;

    std::shared_ptr<Buffer> get_buffer() {
        buffer_->set_bytes(pos_);
        return buffer_;
    }

protected:
    std::streamsize xsputn(const char_type *s, std::streamsize n) override {
        ARCTICDB_TRACE(log::version(), "xsputn {} pos at {}, {} bytes", uintptr_t(buffer_.get()), pos_, n);
        if (buffer_->bytes() < pos_ + n) {
            ARCTICDB_TRACE(log::version(), "{} Calling ensure for {}", uintptr_t(buffer_.get()), (pos_ + n) * 2);
            buffer_->ensure((pos_ + n) * 2);
        }

        auto target = buffer_->ptr_cast<char_type>(pos_, n);
        ARCTICDB_TRACE(log::version(), "Putting {} bytes at {}", n, uintptr_t(target));
        memcpy(target, s, n);
        pos_ += n;
        ARCTICDB_TRACE(log::version(), "{} pos is now {}, returning {}", uintptr_t(buffer_.get()), pos_, n);
        return n;
    }

    int_type overflow(int_type ch) override {
        return xsputn(reinterpret_cast<char *>(&ch), 1);
    }
};


struct S3IOStream : public std::iostream {
    S3StreamBuffer stream_buf_;

    S3IOStream() :
            std::iostream(&stream_buf_) {
    }

    std::shared_ptr<Buffer> get_buffer() {
        return stream_buf_.get_buffer();
    }
};

Aws::IOStreamFactory S3StreamFactory() {
    return [=]() { return Aws::New<S3IOStream>(""); };
}

S3Result<Segment> RealS3Client::get_object(
        const std::string &s3_object_name,
        const std::string &bucket_name) const {

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Looking for object {}", s3_object_name);
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
    request.SetResponseStreamFactory(S3StreamFactory());
    auto outcome = s3_client.GetObject(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }
    auto &retrieved = dynamic_cast<S3IOStream &>(outcome.GetResult().GetBody());

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning object {}", s3_object_name);
    return {Segment::from_buffer(retrieved.get_buffer())};
}

S3Result<std::monostate> RealS3Client::put_object(
        const std::string &s3_object_name,
        Segment &&segment,
        const std::string &bucket_name) {

    ARCTICDB_SUBSAMPLE(S3StorageWritePreamble, 0)
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_name.c_str());
    request.SetKey(s3_object_name.c_str());
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Set s3 key {}", request.GetKey().c_str());

    std::shared_ptr<Buffer> tmp;
    auto hdr_size = segment.segment_header_bytes_size();
    auto [dst, write_size] = segment.try_internal_write(tmp, hdr_size);
    util::check(arcticdb::Segment::FIXED_HEADER_SIZE + hdr_size + segment.buffer().bytes() <=
                write_size,
                "Size disparity, fixed header size {} + variable header size {} + buffer size {}  >= total size {}",
                arcticdb::Segment::FIXED_HEADER_SIZE,
                hdr_size,
                segment.buffer().bytes(),
                write_size);
    auto body = std::make_shared<boost::interprocess::bufferstream>(
            reinterpret_cast<char *>(dst), write_size);
    util::check(body->good(), "Overflow of bufferstream with size {}", write_size);
    request.SetBody(body);

    ARCTICDB_SUBSAMPLE(S3StoragePutObject, 0)
    auto outcome = s3_client.PutObject(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Wrote key '{}', with {} bytes of data",
                           s3_object_name,
                           segment.total_segment_size(hdr_size));
    return {std::monostate()};
}

S3Result<DeleteOutput> RealS3Client::delete_objects(
        const std::vector<std::string>& s3_object_names,
        const std::string& bucket_name) {

    Aws::S3::Model::DeleteObjectsRequest request;
    request.WithBucket(bucket_name.c_str());
    Aws::S3::Model::Delete del_objects;
    for (auto& s3_object_name: s3_object_names) {
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
    for (const auto &failed_key: outcome.GetResult().GetErrors()) {
        failed_deletes.push_back({failed_key.GetKey(), failed_key.GetMessage()});
    }

    DeleteOutput result = {failed_deletes};

    return {result};
}

S3Result<ListObjectsOutput> RealS3Client::list_objects(
        const std::string& name_prefix,
        const std::string& bucket_name,
        const std::optional<std::string> continuation_token) const {

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Searching for objects in bucket {} with prefix {}", bucket_name,
                           name_prefix);
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(bucket_name.c_str());
    request.SetPrefix(name_prefix.c_str());
    if (continuation_token.has_value())
        request.SetContinuationToken(continuation_token.value());

    auto outcome = s3_client.ListObjectsV2(request);

    if (!outcome.IsSuccess()) {
        return {outcome.GetError()};
    }

    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Received object list");

    const auto &result = outcome.GetResult();
    auto next_continuation_token = std::optional<std::string>();
    if (result.GetIsTruncated())
        next_continuation_token = {result.GetNextContinuationToken()};

    auto s3_object_names = std::vector<std::string>();
    for (const auto &s3_object: result.GetContents()) {
        s3_object_names.emplace_back(s3_object.GetKey());
    }

    ListObjectsOutput output = {s3_object_names, next_continuation_token};
    return {output};
}

}

}