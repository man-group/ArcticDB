
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
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/s3/s3_client_interface.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/toolbox/query_stats.hpp>

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <folly/gen/Combine.h>

#include <boost/interprocess/streams/bufferstream.hpp>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

namespace fg = folly::gen;
namespace detail {

static const size_t DELETE_OBJECTS_LIMIT = 1000;

template<class It>
using Range = std::ranges::subrange<It>;

inline bool is_not_found_error(const Aws::S3::S3Errors& error) {
    return error == Aws::S3::S3Errors::NO_SUCH_KEY || error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND;
}

[[noreturn]] inline void raise_s3_exception(const Aws::S3::S3Error& err, const std::string& object_name) {
    std::string error_message;
    auto type = err.GetErrorType();

    auto error_message_suffix = fmt::format(
            "S3Error:{}, HttpResponseCode:{}, {}: {} for object '{}'",
            int(err.GetErrorType()),
            int(err.GetResponseCode()),
            err.GetExceptionName().c_str(),
            err.GetMessage().c_str(),
            object_name
    );

    // s3_client.HeadObject returns RESOURCE_NOT_FOUND if a key is not found.
    if (is_not_found_error(type)) {
        throw KeyNotFoundException(fmt::format("Key Not Found Error: {}", error_message_suffix));
    }

    if (type == Aws::S3::S3Errors::ACCESS_DENIED || type == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID ||
        type == Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH) {
        raise<ErrorCode::E_PERMISSION>(fmt::format("Permission error: {}", error_message_suffix));
    }

    if (type == Aws::S3::S3Errors::UNKNOWN) {
        // Unknown is a catchall which can contain several different important exception types which we want to identify
        if (err.GetResponseCode() == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
            raise<ErrorCode::E_ATOMIC_OPERATION_FAILED>(fmt::format("Atomic operation failed: {}", error_message_suffix)
            );
        }

        if (err.GetExceptionName().find("NotImplemented") != std::string::npos) {
            raise<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>(
                    fmt::format("Operation is not implemented for storage: {}", error_message_suffix)
            );
        }

        if (err.GetResponseCode() == Aws::Http::HttpResponseCode::INSUFFICIENT_STORAGE) {
            raise<ErrorCode::E_QUOTA_EXCEEDED>(fmt::format("Quota exceeded: {}", error_message_suffix));
        }

        if (err.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST) {
            raise<ErrorCode::E_BAD_REQUEST>(fmt::format(
                    "Aws-sdk sent a bad request to S3. This could be due to improper use of the sdk or due "
                    "to using the S3Client in parallel from forked processes. Error message: {}",
                    error_message_suffix
            ));
        }
    }

    // We create a more detailed error explanation in case of NETWORK_CONNECTION errors to remedy #880.
    if (type == Aws::S3::S3Errors::NETWORK_CONNECTION) {
        error_message = fmt::format(
                "Network error: {} "
                "This could be due to a connectivity issue or exhausted file descriptors. "
                "Having more than one open Arctic instance will use multiple file descriptors, you should reuse Arctic "
                "instances. "
                "If you need many file descriptors, consider increasing `ulimit -n`.",
                error_message_suffix
        );
    } else {
        error_message = fmt::format("Unexpected error: {}", error_message_suffix);
    }

    log::storage().error(error_message);
    if (err.ShouldRetry()) {
        raise<ErrorCode::E_S3_RETRYABLE>(fmt::format("Retry-able error: {}", error_message));
    }
    raise<ErrorCode::E_UNEXPECTED_S3_ERROR>(error_message);
}

inline bool is_expected_error_type(Aws::S3::S3Errors err) {
    return is_not_found_error(err) || err == Aws::S3::S3Errors::NO_SUCH_BUCKET;
}

inline void raise_if_unexpected_error(const Aws::S3::S3Error& err, const std::string& object_name) {
    if (!is_expected_error_type(err.GetErrorType())) {
        raise_s3_exception(err, object_name);
    }
}

template<class KeyBucketizer>
void do_write_impl(
        KeySegmentPair& key_seg, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    ARCTICDB_SAMPLE(S3StorageWrite, 0)

    auto key_type = key_seg.key_type();
    auto key_type_dir = key_type_folder(root_folder, key_type);
    ARCTICDB_TRACE(log::storage(), "S3 key_type_folder is {}", key_type_dir);

    ARCTICDB_SUBSAMPLE(S3StorageWriteValues, 0)
    auto& k = key_seg.variant_key();
    auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, k), k);
    auto seg = key_seg.segment_ptr();
    auto segment_size = seg->calculate_size();

    auto query_stat_operation_time =
            query_stats::add_task_count_and_time(query_stats::TaskType::S3_PutObject, key_type);
    auto put_object_result = s3_client.put_object(s3_object_name, *seg, bucket_name);

    if (put_object_result.is_success()) {
        query_stats::add(
                query_stats::TaskType::S3_PutObject, key_type, query_stats::StatType::SIZE_BYTES, segment_size
        );
    } else {
        auto& error = put_object_result.get_error();
        // No DuplicateKeyException is thrown because S3 overwrites the given key if it already exists.
        raise_s3_exception(error, s3_object_name);
    }
}

template<class KeyBucketizer>
void do_update_impl(
        KeySegmentPair& key_seg, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    // s3 updates the key if it already exists. We skip the check for key not found to save a round-trip.
    do_write_impl(key_seg, root_folder, bucket_name, s3_client, std::forward<KeyBucketizer>(bucketizer));
}

template<class KeyBucketizer, class KeyDecoder>
KeySegmentPair do_read_impl(
        VariantKey&& variant_key, const std::string& root_folder, const std::string& bucket_name,
        const S3ClientInterface& s3_client, KeyBucketizer&& bucketizer, KeyDecoder&& key_decoder, ReadKeyOpts opts
) {
    ARCTICDB_SAMPLE(S3StorageRead, 0)
    auto key_type = variant_key_type(variant_key);
    auto key_type_dir = key_type_folder(root_folder, key_type);
    auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, variant_key), variant_key);
    auto query_stat_operation_time =
            query_stats::add_task_count_and_time(query_stats::TaskType::S3_GetObject, key_type);
    auto get_object_result = s3_client.get_object(s3_object_name, bucket_name);
    auto unencoded_key = key_decoder(std::move(variant_key));

    if (get_object_result.is_success()) {
        ARCTICDB_SUBSAMPLE(S3StorageVisitSegment, 0)
        auto segment = std::move(get_object_result.get_output());
        query_stats::add(
                query_stats::TaskType::S3_GetObject,
                key_type,
                query_stats::StatType::SIZE_BYTES,
                segment.calculate_size()
        );
        return {VariantKey{unencoded_key}, std::move(segment)};
    } else {
        auto& error = get_object_result.get_error();
        raise_if_unexpected_error(error, s3_object_name);

        log::storage().log(
                opts.dont_warn_about_missing_key ? spdlog::level::debug : spdlog::level::warn,
                "Failed to find segment for key '{}' {}: {}",
                variant_key_view(unencoded_key),
                error.GetExceptionName().c_str(),
                error.GetMessage().c_str()
        );

        throw KeyNotFoundException(unencoded_key);
    }
    return KeySegmentPair{};
}

template<class KeyBucketizer, class KeyDecoder>
folly::Future<KeySegmentPair> do_async_read_impl(
        VariantKey&& variant_key, const std::string& root_folder, const std::string& bucket_name,
        const S3ClientInterface& s3_client, KeyBucketizer&& bucketizer, KeyDecoder&& key_decoder, ReadKeyOpts
) {
    auto key_type = variant_key_type(variant_key);
    auto key_type_dir = key_type_folder(root_folder, key_type);
    auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, variant_key), variant_key);
    return s3_client.get_object_async(s3_object_name, bucket_name)
            .thenValue(
                    [vk = std::move(variant_key),
                     decoder = std::forward<KeyDecoder>(key_decoder),
                     key_type,
                     start = std::chrono::steady_clock::now()](auto&& result) mutable -> KeySegmentPair {
                        auto query_stat_operation_time = query_stats::add_task_count_and_time(
                                query_stats::TaskType::S3_GetObjectAsync, key_type, start
                        );
                        if (result.is_success()) {
                            auto segment = std::move(result.get_output());
                            query_stats::add(
                                    query_stats::TaskType::S3_GetObjectAsync,
                                    key_type,
                                    query_stats::StatType::SIZE_BYTES,
                                    segment.calculate_size()
                            );
                            return KeySegmentPair(std::move(vk), std::move(segment));
                        } else {
                            auto unencoded_key = decoder(std::move(vk));
                            raise_s3_exception(result.get_error(), fmt::format("{}", unencoded_key));
                        }
                    }
            );
}

template<class KeyBucketizer, class KeyDecoder>
void do_read_impl(
        VariantKey&& variant_key, const ReadVisitor& visitor, const std::string& root_folder,
        const std::string& bucket_name, const S3ClientInterface& s3_client, KeyBucketizer&& bucketizer,
        KeyDecoder&& key_decoder, ReadKeyOpts opts
) {
    auto key_seg = do_read_impl(
            std::move(variant_key),
            root_folder,
            bucket_name,
            s3_client,
            std::forward<KeyBucketizer>(bucketizer),
            std::forward<KeyDecoder>(key_decoder),
            opts
    );
    visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
}

struct FailedDelete {
    VariantKey failed_key;
    std::string error_message;

    FailedDelete(VariantKey&& failed_key, std::string&& error_message) :
        failed_key(failed_key),
        error_message(error_message) {}
};

inline void raise_if_failed_deletes(const boost::container::small_vector<FailedDelete, 1>& failed_deletes) {
    if (!failed_deletes.empty()) {
        auto failed_deletes_message = std::ostringstream();
        for (auto i = 0u; i < failed_deletes.size(); ++i) {
            auto& failed = failed_deletes[i];
            failed_deletes_message << fmt::format(
                    "'{}' failed with '{}'", to_serialized_key(failed.failed_key), failed.error_message
            );
            if (i != failed_deletes.size()) {
                failed_deletes_message << ", ";
            }
        }
        auto error_message = fmt::format("Failed to delete some of the objects: {}.", failed_deletes_message.str());
        raise<ErrorCode::E_UNEXPECTED_S3_ERROR>(error_message);
    }
}

template<class KeyBucketizer>
void do_remove_impl(
        std::span<VariantKey> ks, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    ARCTICDB_SUBSAMPLE(S3StorageDeleteBatch, 0)
    auto fmt_db = [](auto&& k) { return variant_key_type(k); };
    std::vector<std::string> to_delete;
    boost::container::small_vector<FailedDelete, 1> failed_deletes;
    static const size_t delete_object_limit = std::min(
            DELETE_OBJECTS_LIMIT,
            static_cast<size_t>(ConfigsMap::instance()->get_int("S3Storage.DeleteBatchSize", 1000))
    );

    to_delete.reserve(std::min(ks.size(), delete_object_limit));

    (fg::from(ks) | fg::move | fg::groupBy(fmt_db))
            .foreach ([&s3_client,
                       &root_folder,
                       &bucket_name,
                       &to_delete,
                       b = std::forward<KeyBucketizer>(bucketizer),
                       &failed_deletes](auto&& group) {
                auto key_type_dir = key_type_folder(root_folder, group.key());
                for (auto k : folly::enumerate(group.values())) {
                    auto s3_object_name = object_path(b.bucketize(key_type_dir, *k), *k);
                    to_delete.emplace_back(std::move(s3_object_name));

                    if (to_delete.size() == delete_object_limit || k.index + 1 == group.size()) {
                        auto query_stat_operation_time = query_stats::add_task_count_and_time(
                                query_stats::TaskType::S3_DeleteObjects, group.key()
                        );
                        auto delete_object_result = s3_client.delete_objects(to_delete, bucket_name);
                        if (delete_object_result.is_success()) {
                            ARCTICDB_RUNTIME_DEBUG(
                                    log::storage(),
                                    "Deleted {} objects, one of which with key '{}'",
                                    to_delete.size(),
                                    variant_key_view(*k)
                            );
                            for (auto& bad_key : delete_object_result.get_output().failed_deletes) {
                                auto bad_key_name =
                                        bad_key.s3_object_name.substr(key_type_dir.size(), std::string::npos);
                                failed_deletes.emplace_back(
                                        variant_key_from_bytes(
                                                reinterpret_cast<const uint8_t*>(bad_key_name.data()),
                                                bad_key_name.size(),
                                                group.key()
                                        ),
                                        std::move(bad_key.error_message)
                                );
                            }
                        } else {
                            auto& error = delete_object_result.get_error();
                            std::string failed_objects = fmt::format("{}", fmt::join(to_delete, ", "));
                            raise_s3_exception(error, failed_objects);
                        }
                        to_delete.clear();
                    }
                }
            });

    util::check(to_delete.empty(), "Have {} segment that have not been removed", to_delete.size());
    raise_if_failed_deletes(failed_deletes);
}

template<class KeyBucketizer>
void do_remove_impl(
        VariantKey&& variant_key, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    std::array<VariantKey, 1> arr{std::move(variant_key)};
    do_remove_impl(std::span(arr), root_folder, bucket_name, s3_client, std::forward<KeyBucketizer>(bucketizer));
}

template<class KeyBucketizer>
void do_remove_no_batching_impl(
        std::span<VariantKey> ks, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    ARCTICDB_SUBSAMPLE(S3StorageDeleteNoBatching, 0)

    std::vector<folly::Future<S3Result<std::monostate>>> delete_object_results;
    for (const auto& k : ks) {
        auto key_type_dir = key_type_folder(root_folder, variant_key_type(k));
        auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, k), k);
        auto delete_fut = s3_client.delete_object(s3_object_name, bucket_name);
        delete_object_results.push_back(std::move(delete_fut));
    }

    folly::QueuedImmediateExecutor inline_executor;
    auto delete_results = folly::collect(std::move(delete_object_results)).via(&inline_executor).get();

    boost::container::small_vector<FailedDelete, 1> failed_deletes;
    auto keys_and_delete_results = folly::gen::from(ks) | folly::gen::move |
                                   folly::gen::zip(std::move(delete_results)) | folly::gen::as<std::vector>();
    for (auto&& [k, delete_object_result] : std::move(keys_and_delete_results)) {
        if (delete_object_result.is_success()) {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Deleted object with key '{}'", variant_key_view(k));
        } else if (const auto& error = delete_object_result.get_error(); !is_not_found_error(error.GetErrorType())) {
            auto key_type_dir = key_type_folder(root_folder, variant_key_type(k));
            auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, k), k);
            auto bad_key_name = s3_object_name.substr(key_type_dir.size(), std::string::npos);
            auto error_message = error.GetMessage();
            failed_deletes.push_back(FailedDelete{
                    variant_key_from_bytes(
                            reinterpret_cast<const uint8_t*>(bad_key_name.data()),
                            bad_key_name.size(),
                            variant_key_type(k)
                    ),
                    std::move(error_message)
            });
        } else {
            ARCTICDB_RUNTIME_DEBUG(
                    log::storage(), "Acceptable error when deleting object with key '{}'", variant_key_view(k)
            );
        }
    }

    raise_if_failed_deletes(failed_deletes);
}

template<class KeyBucketizer>
void do_remove_no_batching_impl(
        VariantKey&& variant_key, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    std::array<VariantKey, 1> arr{std::move(variant_key)};
    do_remove_no_batching_impl(
            std::span(arr), root_folder, bucket_name, s3_client, std::forward<KeyBucketizer>(bucketizer)
    );
}

template<class KeyBucketizer>
void do_write_if_none_impl(
        KeySegmentPair& kv, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    ARCTICDB_SAMPLE(S3StorageWriteIfNone, 0)
    auto key_type = kv.key_type();
    auto key_type_dir = key_type_folder(root_folder, key_type);
    auto& k = kv.variant_key();
    auto s3_object_name = object_path(bucketizer.bucketize(key_type_dir, k), k);
    auto& seg = *kv.segment_ptr();
    auto segment_size = seg.calculate_size();

    auto query_stat_operation_time =
            query_stats::add_task_count_and_time(query_stats::TaskType::S3_PutObject, key_type);
    auto put_object_result = s3_client.put_object(s3_object_name, seg, bucket_name, PutHeader::IF_NONE_MATCH);

    if (put_object_result.is_success()) {
        query_stats::add(
                query_stats::TaskType::S3_PutObject, key_type, query_stats::StatType::SIZE_BYTES, segment_size
        );
    } else {
        auto& error = put_object_result.get_error();
        raise_s3_exception(error, s3_object_name);
    }
}

template<class KeyBucketizer>
void do_update_impl(
        Composite<KeySegmentPair>&& kvs, const std::string& root_folder, const std::string& bucket_name,
        S3ClientInterface& s3_client, KeyBucketizer&& bucketizer
) {
    // s3 updates the key if it already exists. We skip the check for key not found to save a round-trip.
    do_write_impl(std::move(kvs), root_folder, bucket_name, s3_client, std::forward<KeyBucketizer>(bucketizer));
}

struct PathInfo {
    PathInfo(std::string&& prefix, size_t path_to_key_size) :
        key_prefix_(std::move(prefix)),
        path_to_key_size_(path_to_key_size) {}

    std::string key_prefix_;
    size_t path_to_key_size_;
};

inline PathInfo calculate_path_info(
        const std::string& root_folder, KeyType key_type, const bool prefix_matching_supported,
        const std::string& prefix, const size_t bucketize_length
) {
    auto key_type_dir = key_type_folder(root_folder, key_type);
    const auto path_to_key_size = key_type_dir.size() + 1 + bucketize_length;
    // if prefix is empty, add / to avoid matching both 'log' and 'logc' when key_type_dir is {root_folder}/log
    if (prefix.empty()) {
        key_type_dir += "/";
    }

    // Generally we get the key descriptor from the AtomKey, but in the case of iterating version journals
    // where we want to have a narrower prefix, we can use the info that it's a version journal and derive
    // the Descriptor.
    // TODO: Set the IndexDescriptorImpl correctly
    KeyDescriptor key_descriptor(
            prefix,
            is_ref_key_class(key_type) ? IndexDescriptorImpl::Type::UNKNOWN : IndexDescriptorImpl::Type::TIMESTAMP,
            FormatType::TOKENIZED
    );
    auto key_prefix = [&]() {
        if (prefix_matching_supported) {
            return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
        } else {
            return key_type_dir;
        }
    }();

    return {std::move(key_prefix), path_to_key_size};
}

struct Visitor {
    Visitor(const IterateTypePredicate& primary_visitor,
            const std::optional<IterateTypePredicate> fallback_visitor = std::nullopt) :
        primary_visitor_(primary_visitor),
        fallback_visitor_(fallback_visitor) {};
    IterateTypePredicate primary_visitor_;
    std::optional<IterateTypePredicate> fallback_visitor_;
    // Directory (aka express) buckets in AWS only support ListObjectsV2 requests with prefixes ending in a '/'
    // delimiter. Until we have proper feature detection, just cache if this is the case after the first failed listing
    // operation with a prefix that does not end in a '/'
    bool directory_bucket_{false};
};

inline bool do_iterate_type_impl(
        KeyType key_type, const std::string& bucket_name, const S3ClientInterface& s3_client, const PathInfo& path_info,
        Visitor& visitor
) {
    ARCTICDB_SAMPLE(S3StorageIterateType, 0)

    ARCTICDB_RUNTIME_DEBUG(
            log::storage(), "Iterating over objects in bucket {} with prefix {}", bucket_name, path_info.key_prefix_
    );

    auto continuation_token = std::optional<std::string>();
    do {
        auto query_stat_operation_time =
                query_stats::add_task_count_and_time(query_stats::TaskType::S3_ListObjectsV2, key_type);
        auto list_objects_result = s3_client.list_objects(path_info.key_prefix_, bucket_name, continuation_token);
        if (list_objects_result.is_success()) {
            auto& output = list_objects_result.get_output();

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Received object list");
            for (auto& s3_object_name : output.s3_object_names) {
                auto key = s3_object_name.substr(path_info.path_to_key_size_);
                ARCTICDB_TRACE(log::version(), "Got object_list: {}, key: {}", s3_object_name, key);
                auto k = variant_key_from_bytes(reinterpret_cast<uint8_t*>(key.data()), key.size(), key_type);

                ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
                ARCTICDB_SUBSAMPLE(S3StorageVisitKey, 0)
                if (visitor.primary_visitor_(std::move(k))) {
                    return true;
                }
                ARCTICDB_SUBSAMPLE(S3StorageCursorNext, 0)
            }
            continuation_token = output.next_continuation_token;
        } else {
            const auto& error = list_objects_result.get_error();
            if (error.GetErrorType() == Aws::S3::S3Errors::INVALID_REQUEST && !path_info.key_prefix_.ends_with('/') &&
                visitor.fallback_visitor_.has_value()) {
                log::storage().debug(
                        "Storage does not support prefix matches not ending in delimiter '/'. Falling back to "
                        "iterating all keys of type {} and prefix matching in memory.",
                        key_type
                );
                visitor.directory_bucket_ = true;
                visitor.primary_visitor_ = std::move(*visitor.fallback_visitor_);
                visitor.fallback_visitor_ = std::nullopt;
                const auto last_slash_pos = path_info.key_prefix_.find_last_of('/');
                PathInfo truncated_path_info{
                        last_slash_pos == std::string::npos ? "" : path_info.key_prefix_.substr(0, last_slash_pos + 1),
                        path_info.path_to_key_size_
                };
                return do_iterate_type_impl(key_type, bucket_name, s3_client, truncated_path_info, visitor);
            }
            log::storage().warn(
                    "Failed to iterate key type with key '{}' {}: {}",
                    key_type,
                    error.GetExceptionName().c_str(),
                    error.GetMessage().c_str()
            );
            // We don't raise on expected errors like NoSuchKey because we want to return an empty list
            // instead of raising.
            raise_if_unexpected_error(error, path_info.key_prefix_);
            return false;
        }
    } while (continuation_token.has_value());
    return false;
}

inline void do_visit_object_sizes_for_type_impl(
        KeyType key_type, const std::string& bucket_name, const S3ClientInterface& s3_client, const PathInfo& path_info,
        const ObjectSizesVisitor& visitor
) {
    ARCTICDB_SAMPLE(S3StorageCalculateSizesForType, 0)
    ARCTICDB_RUNTIME_DEBUG(
            log::storage(),
            "Calculating sizes for objects in bucket {} with prefix {}",
            bucket_name,
            path_info.key_prefix_
    );

    auto continuation_token = std::optional<std::string>();
    ObjectSizes res{key_type};
    do {
        auto list_objects_result = s3_client.list_objects(path_info.key_prefix_, bucket_name, continuation_token);
        if (list_objects_result.is_success()) {
            const auto& output = list_objects_result.get_output();

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Received object list");

            auto zipped = folly::gen::from(output.s3_object_sizes) | folly::gen::zip(output.s3_object_names) |
                          folly::gen::as<std::vector>();
            for (const auto& [size, name] : zipped) {
                auto key = name.substr(path_info.path_to_key_size_);
                auto k = variant_key_from_bytes(reinterpret_cast<uint8_t*>(key.data()), key.size(), key_type);

                visitor(k, size);
            }
            continuation_token = output.next_continuation_token;
        } else {
            const auto& error = list_objects_result.get_error();
            log::storage().warn(
                    "Failed to iterate key type with key '{}' {}: {}",
                    key_type,
                    error.GetExceptionName().c_str(),
                    error.GetMessage().c_str()
            );
            raise_if_unexpected_error(error, path_info.key_prefix_);
        }
    } while (continuation_token.has_value());
}

template<class KeyBucketizer>
bool do_key_exists_impl(
        const VariantKey& key, const std::string& root_folder, const std::string& bucket_name,
        const S3ClientInterface& s3_client, KeyBucketizer&& b
) {
    auto key_type = variant_key_type(key);
    auto key_type_dir = key_type_folder(root_folder, key_type);
    auto s3_object_name = object_path(b.bucketize(key_type_dir, key), key);

    auto query_stat_operation_time =
            query_stats::add_task_count_and_time(query_stats::TaskType::S3_HeadObject, key_type);
    auto head_object_result = s3_client.head_object(s3_object_name, bucket_name);

    if (!head_object_result.is_success()) {
        auto& error = head_object_result.get_error();
        raise_if_unexpected_error(error, s3_object_name);

        ARCTICDB_DEBUG(
                log::storage(),
                "Head object returned false for key {} {} {}:{}",
                variant_key_view(key),
                int(error.GetErrorType()),
                error.GetExceptionName().c_str(),
                error.GetMessage().c_str()
        );
    }

    return head_object_result.is_success();
}

} // namespace detail
} // namespace s3
} // namespace arcticdb::storage
