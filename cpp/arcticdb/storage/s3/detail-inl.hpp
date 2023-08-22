#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>
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
#include <aws/s3/model/Object.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <boost/interprocess/streams/bufferstream.hpp>
#include <folly/ThreadLocal.h>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

    namespace fg = folly::gen;
    namespace detail {

        static const size_t DELETE_OBJECTS_LIMIT = 1000;

        template<class It>
        using Range = folly::Range<It>;

        template<class S3ClientType, class KeyBucketizer>
        void do_write_impl(
                Composite<KeySegmentPair> &&kvs,
                const std::string &root_folder,
                const std::string &bucket_name,
                S3ClientType &s3_client,
                KeyBucketizer &&bucketizer) {
            ARCTICDB_SAMPLE(S3StorageWrite, 0)
            auto fmt_db = [](auto &&kv) { return kv.key_type(); };

            (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
                    [&s3_client, &bucket_name, &root_folder, b = std::move(bucketizer)](auto &&group) {
                        auto key_type_dir = key_type_folder(root_folder, group.key());
                        ARCTICDB_TRACE(log::storage(), "S3 key_type_folder is {}", key_type_dir);

                        ARCTICDB_SUBSAMPLE(S3StorageWriteValues, 0)
                        for (auto &kv: group.values()) {
                            auto &k = kv.variant_key();
                            auto s3_object_name = object_path(b.bucketize(key_type_dir, k), k);
                            auto &seg = kv.segment();

                            ARCTICDB_SUBSAMPLE(S3StorageWritePreamble, 0)
                            Aws::S3::Model::PutObjectRequest object_request;
                            object_request.SetBucket(bucket_name.c_str());
                            object_request.SetKey(s3_object_name.c_str());
                            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Set s3 key {}", object_request.GetKey().c_str());

                            std::shared_ptr<Buffer> tmp;
                            auto hdr_size = seg.segment_header_bytes_size();
                            auto [dst, write_size] = seg.try_internal_write(tmp, hdr_size);
                            util::check(arcticdb::Segment::FIXED_HEADER_SIZE + hdr_size + seg.buffer().bytes() <=
                                        write_size,
                                        "Size disparity, fixed header size {} + variable header size {} + buffer size {}  >= total size {}",
                                        arcticdb::Segment::FIXED_HEADER_SIZE,
                                        hdr_size,
                                        seg.buffer().bytes(),
                                        write_size);
                            auto body = std::make_shared<boost::interprocess::bufferstream>(
                                    reinterpret_cast<char *>(dst), write_size);
                            util::check(body->good(), "Overflow of bufferstream with size {}", write_size);
                            object_request.SetBody(body);

                            ARCTICDB_SUBSAMPLE(S3StoragePutObject, 0)
                            auto put_object_outcome = s3_client.PutObject(object_request);

                            if (!put_object_outcome.IsSuccess()) {
                                auto &error = put_object_outcome.GetError();
                                util::raise_rte("Failed to write s3 with key '{}' {}: {}",
                                                k,
                                                error.GetExceptionName().c_str(),
                                                error.GetMessage().c_str());
                            }
                            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Wrote key {}: {}, with {} bytes of data",
                                                   variant_key_type(k),
                                                   variant_key_view(k),
                                                   seg.total_segment_size(hdr_size));
                        }
                    });
        }

        template<class S3ClientType, class KeyBucketizer>
        void do_update_impl(
                Composite<KeySegmentPair> &&kvs,
                const std::string &root_folder,
                const std::string &bucket_name,
                S3ClientType &s3_client,
                KeyBucketizer &&bucketizer) {
            // s3 updates the key if it already exists (our buckets don't have versioning)
            do_write_impl(std::move(kvs), root_folder, bucket_name, s3_client, std::move(bucketizer));
        }

        struct UnexpectedS3ErrorException : public std::exception {
        };

        inline bool is_expected_error_type(Aws::S3::S3Errors err) {
            return err == Aws::S3::S3Errors::NO_SUCH_KEY
                   || err == Aws::S3::S3Errors::NO_SUCH_BUCKET
                   || err == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID
                   || err == Aws::S3::S3Errors::ACCESS_DENIED
                   || err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND;
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
            std::streamsize xsputn(const char_type *s, std::streamsize n) override;

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

        inline Aws::IOStreamFactory S3StreamFactory() {
            return [=]() { return Aws::New<S3IOStream>(""); };
        }

        template<class KeyType, class S3ClientType, class KeyBucketizer>
        auto get_object(
                const KeyType &key,
                const std::string &root_folder,
                const std::string &bucket_name,
                S3ClientType &s3_client,
                KeyBucketizer &b) {
            auto key_type_dir = key_type_folder(root_folder, variant_key_type(key));
            auto s3_object_name = object_path(b.bucketize(key_type_dir, key), key);

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Looking for object {}", s3_object_name);
            Aws::S3::Model::GetObjectRequest request;
            request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
            request.SetResponseStreamFactory(S3StreamFactory());
            auto res = s3_client.GetObject(request);

            if (!res.IsSuccess() && !is_expected_error_type(res.GetError().GetErrorType())) {
                log::storage().error("Got unexpected error: '{}' {}: {}",
                                     int(res.GetError().GetErrorType()),
                                     res.GetError().GetExceptionName().c_str(),
                                     res.GetError().GetMessage().c_str());

                throw UnexpectedS3ErrorException{};
            }
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning object {}", s3_object_name);
            return res;
        }

        template<class KeyType, class S3ClientType, class KeyBucketizer>
        auto head_object(
                const KeyType &key,
                const std::string &root_folder,
                const std::string &bucket_name,
                S3ClientType &s3_client,
                KeyBucketizer &b) {
            auto key_type_dir = key_type_folder(root_folder, variant_key_type(key));
            auto s3_object_name = object_path(b.bucketize(key_type_dir, key), key);

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Looking for head of object {}", s3_object_name);
            Aws::S3::Model::HeadObjectRequest request;
            request.WithBucket(bucket_name.c_str()).WithKey(s3_object_name.c_str());
            auto res = s3_client.HeadObject(request);

            if (!res.IsSuccess() && !is_expected_error_type(res.GetError().GetErrorType())) {
                log::storage().error("Got unexpected error: '{}' {}: {}",
                                     int(res.GetError().GetErrorType()),
                                     res.GetError().GetExceptionName().c_str(),
                                     res.GetError().GetMessage().c_str());

                throw UnexpectedS3ErrorException{};
            }
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Returning head of object {}", s3_object_name);
            return res;
        }


        template<class S3ClientType, class KeyBucketizer>
        void do_read_impl(Composite<VariantKey> &&ks,
                          const ReadVisitor &visitor,
                          const std::string &root_folder,
                          const std::string &bucket_name,
                          S3ClientType &s3_client,
                          KeyBucketizer &&bucketizer,
                          ReadKeyOpts opts) {
            ARCTICDB_SAMPLE(S3StorageRead, 0)
            auto fmt_db = [](auto &&k) { return variant_key_type(k); };
            std::vector<VariantKey> failed_reads;

            (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
                    [&s3_client, &bucket_name, &root_folder, b = std::move(bucketizer), &visitor, &failed_reads,
                            opts = opts](auto &&group) {

                        for (auto &k: group.values()) {
                            auto get_object_outcome = get_object(
                                    k,
                                    root_folder,
                                    bucket_name,
                                    s3_client,
                                    b);

                            if (get_object_outcome.IsSuccess()) {
                                ARCTICDB_SUBSAMPLE(S3StorageVisitSegment, 0)
                                auto &retrieved = dynamic_cast<S3IOStream &>(get_object_outcome.GetResult().GetBody());

                                visitor(k, Segment::from_buffer(retrieved.get_buffer()));

                                ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k),
                                               variant_key_view(k));
                            } else {
                                auto &error = get_object_outcome.GetError();
                                if (!opts.dont_warn_about_missing_key) {
                                    log::storage().warn("Failed to find segment for key '{}' {}: {}",
                                                        variant_key_view(k),
                                                        error.GetExceptionName().c_str(),
                                                        error.GetMessage().c_str());
                                }

                                failed_reads.push_back(k);
                            }
                        }
                    });
            if (!failed_reads.empty())
                throw KeyNotFoundException(Composite<VariantKey>{std::move(failed_reads)});
        }

        template<class S3ClientType, class KeyBucketizer>
        void do_remove_impl(Composite<VariantKey> &&ks,
                            const std::string &root_folder,
                            const std::string &bucket_name,
                            S3ClientType &s3_client,
                            KeyBucketizer &&bucketizer) {
            ARCTICDB_SUBSAMPLE(S3StorageDeleteBatch, 0)
            auto fmt_db = [](auto &&k) { return variant_key_type(k); };
            Aws::S3::Model::DeleteObjectsRequest object_request;
            object_request.WithBucket(bucket_name.c_str());
            Aws::S3::Model::Delete del_objects;
            std::vector<VariantKey> failed_deletes;
            static const size_t delete_object_limit =
                    std::min(DELETE_OBJECTS_LIMIT,
                             static_cast<size_t>(ConfigsMap::instance()->get_int("S3Storage.DeleteBatchSize", 1000)));

            (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
                    [&s3_client, &root_folder, &del_objects, &object_request, b = std::move(
                            bucketizer), &failed_deletes](auto &&group) {
                        auto key_type_dir = key_type_folder(root_folder, group.key());
                        for (auto k: folly::enumerate(group.values())) {
                            auto s3_object_name = object_path(b.bucketize(key_type_dir, *k), *k);
                            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing s3 object with key {}", s3_object_name);

                            del_objects.AddObjects(Aws::S3::Model::ObjectIdentifier().WithKey(s3_object_name.c_str()));

                            if (del_objects.GetObjects().size() == delete_object_limit || k.index + 1 == group.size()) {
                                ARCTICDB_SUBSAMPLE(S3StorageDeleteObjects, 0)
                                object_request.SetDelete(del_objects);
                                auto delete_object_outcome = s3_client.DeleteObjects(object_request);
                                if (delete_object_outcome.IsSuccess()) {
                                    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Deleted object with key '{}'",
                                                           variant_key_view(*k));
                                } else {
                                    // AN-256: Per AWS S3 documentation, deleting non-exist objects is not an error, so not handling
                                    // RemoveOpts.ignores_missing_key_
                                    for (const auto &bad_key: delete_object_outcome.GetResult().GetErrors()) {
                                        auto bad_key_name = bad_key.GetKey().substr(key_type_dir.size(),
                                                                                    std::string::npos);
                                        failed_deletes.push_back(
                                                from_tokenized_variant_key(
                                                        reinterpret_cast<const uint8_t *>(bad_key_name.data()),
                                                        bad_key_name.size(), group.key()));
                                    }
                                }
                                del_objects.SetObjects(Aws::Vector<Aws::S3::Model::ObjectIdentifier>());
                            }
                        }
                    });

            util::check(del_objects.GetObjects().empty(), "Have {} segment that have not been removed",
                        del_objects.GetObjects().size());
            if (!failed_deletes.empty())
                throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
        }

        inline auto default_prefix_handler() {
            return [](const std::string &prefix, const std::string &key_type_dir, const KeyDescriptor &key_descriptor,
                      KeyType) {
                return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
            };
        }

        template<class Visitor, class S3ClientType, class KeyBucketizer, class PrefixHandler>
        void do_iterate_type_impl(KeyType key_type,
                                  Visitor &&visitor,
                                  const std::string &root_folder,
                                  const std::string &bucket_name,
                                  S3ClientType &s3_client,
                                  KeyBucketizer &&bucketizer,
                                  PrefixHandler &&prefix_handler = default_prefix_handler(),
                                  const std::string &prefix = std::string{}
        ) {
            ARCTICDB_SAMPLE(S3StorageIterateType, 0)
            auto key_type_dir = key_type_folder(root_folder, key_type);

            // Generally we get the key descriptor from the AtomKey, but in the case of iterating version journals
            // where we want to have a narrower prefix, we can use the info that it's a version journal and derive
            // the Descriptor.
            // TODO: Set the IndexDescriptor correctly
            KeyDescriptor key_descriptor(prefix,
                                         is_ref_key_class(key_type) ? IndexDescriptor::UNKNOWN
                                                                    : IndexDescriptor::TIMESTAMP,
                                         FormatType::TOKENIZED);
            auto key_prefix = prefix_handler(prefix, key_type_dir, key_descriptor, key_type);
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Searching for objects in bucket {} with prefix {}", bucket_name,
                                   key_prefix);
            Aws::S3::Model::ListObjectsV2Request objects_request;
            objects_request.WithBucket(bucket_name.c_str());
            objects_request.SetPrefix(key_prefix.c_str());

            bool more;
            do {
                auto list_objects_outcome = s3_client.ListObjectsV2(objects_request);

                if (list_objects_outcome.IsSuccess()) {
                    auto object_list = list_objects_outcome.GetResult().GetContents();
                    const auto root_folder_size = key_type_dir.size() + 1 + bucketizer.bucketize_length(key_type);
                    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Received object list");

                    for (auto const &s3_object: object_list) {
                        auto key = s3_object.GetKey().substr(root_folder_size);
                        ARCTICDB_TRACE(log::version(), "Got object_list: {}, key: {}", s3_object.GetKey(), key);
                        auto k = variant_key_from_bytes(
                                reinterpret_cast<uint8_t *>(key.data()),
                                key.size(),
                                key_type);

                        ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k),
                                       variant_key_view(k));
                        ARCTICDB_SUBSAMPLE(S3StorageVisitKey, 0)
                        visitor(std::move(k));
                        ARCTICDB_SUBSAMPLE(S3StorageCursorNext, 0)
                    }
                    more = list_objects_outcome.GetResult().GetIsTruncated();
                    if (more)
                        objects_request.SetContinuationToken(
                                list_objects_outcome.GetResult().GetNextContinuationToken());

                } else {
                    const auto &error = list_objects_outcome.GetError();
                    log::storage().warn("Failed to iterate key type with key '{}' {}: {}",
                                        key_type,
                                        error.GetExceptionName().c_str(),
                                        error.GetMessage().c_str());
                    return;
                }
            } while (more);
        }

        template<class S3ClientType, class KeyBucketizer>
        bool do_key_exists_impl(
                const VariantKey &key,
                const std::string &root_folder,
                const std::string &bucket_name,
                S3ClientType &s3_client,
                KeyBucketizer &&bucketizer
        ) {
            auto head_object_outcome = head_object(
                    key,
                    root_folder,
                    bucket_name,
                    s3_client,
                    bucketizer);

            if (!head_object_outcome.IsSuccess()) {
                auto &error ARCTICDB_UNUSED = head_object_outcome.GetError();
                ARCTICDB_DEBUG(log::storage(), "Head object returned false for key {} {} {}:{}",
                               variant_key_view(key),
                               int(error.GetErrorType()),
                               error.GetExceptionName().c_str(),
                               error.GetMessage().c_str());
            }

            return head_object_outcome.IsSuccess();
        }

        } // namespace detail
    } // namespace s3
} // namespace arcticdb::storage