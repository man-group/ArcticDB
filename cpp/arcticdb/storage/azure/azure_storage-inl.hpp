/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_AZURE_STORAGE_H_
#error "This should only be included by file_storage.hpp"
#endif

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/buffer_pool.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <boost/interprocess/streams/bufferstream.hpp>
#include <folly/ThreadLocal.h>

#undef GetMessage

namespace arcticdb::storage::azure {

namespace fg = folly::gen;

namespace detail {

static const size_t BATCH_SUBREQUEST_LIMIT = 1000;

template<class KeyBucketizer>
void do_write_impl(
    Composite<KeySegmentPair>&& kvs,
    const std::string& root_folder,
    Azure::Storage::Blobs::BlobContainerClient& container_client,
    KeyBucketizer&& bucketizer) {
    ARCTICDB_SAMPLE(AzureStorageWrite, 0)
    auto fmt_db = [](auto&& kv) { return kv.key_type(); };

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
        [&container_client, &root_folder, b=std::move(bucketizer)] (auto&& group) {
        auto key_type_folder = object_store_utils::key_type_folder(root_folder, group.key());
        ARCTICDB_TRACE(log::storage(), "Azure key_type_folder is {}", key_type_folder);

        ARCTICDB_SUBSAMPLE(AzureStorageWriteValues, 0)
        for (auto& kv : group.values()) {
            auto& k = kv.variant_key();
            auto blob_name = object_store_utils::object_path(b.bucketize(key_type_folder, k), k);
            auto& seg = kv.segment();

            std::shared_ptr<Buffer> tmp;
            auto hdr_size = seg.segment_header_bytes_size();
            auto [dst, write_size] = seg.try_internal_write(tmp, hdr_size);
            util::check(arcticdb::Segment::FIXED_HEADER_SIZE + hdr_size + seg.buffer().bytes() <= write_size,
                            "Size disparity, fixed header size {} + variable header size {} + buffer size {}  >= total size {}",
                            arcticdb::Segment::FIXED_HEADER_SIZE,
                            hdr_size,
                            seg.buffer().bytes(),
                            write_size);

            ARCTICDB_SUBSAMPLE(AzureStorageUploadObject, 0)

            try{
                auto blob_client = container_client.GetBlockBlobClient(blob_name);
                blob_client.UploadFrom(dst, write_size);
            }
            catch (const Azure::Core::RequestFailedException& e){
                util::raise_rte("Failed to write azure with key '{}' {} {}: {}",
                                    k,
                                    blob_name,
                                    static_cast<int>(e.StatusCode),
                                    e.ReasonPhrase);
            }

            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Wrote key {}: {}, with {} bytes of data",
                                variant_key_type(k),
                                variant_key_view(k),
                                seg.total_segment_size(hdr_size));
        }
    });
}

template<class KeyBucketizer>
void do_update_impl(
        Composite<KeySegmentPair>&& kvs,
        const std::string& root_folder,
        Azure::Storage::Blobs::BlobContainerClient& container_client,
        KeyBucketizer&& bucketizer) {
    // azure updates the key if it already exists
    do_write_impl(std::move(kvs), root_folder, container_client, std::move(bucketizer));
}

struct UnexpectedAzureErrorException : public std::exception {};

template<class Visitor, class KeyBucketizer>
void do_read_impl(Composite<VariantKey> && ks,
                  Visitor&& visitor,
                  const std::string& root_folder,
                  Azure::Storage::Blobs::BlobContainerClient& container_client,
                  KeyBucketizer&& bucketizer,
                  ReadKeyOpts opts) {
    ARCTICDB_SAMPLE(AzureStorageRead, 0)
    auto fmt_db = [](auto&& k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
        [&container_client, &root_folder, b=std::move(bucketizer), &visitor, &failed_reads,
         opts=opts] (auto&& group) {
        for (auto& k : group.values()) {
            auto key_type_folder = object_store_utils::key_type_folder(root_folder, variant_key_type(k));
            auto blob_name = object_store_utils::object_path(b.bucketize(key_type_folder, k), k);
            try{
                auto blob_client = container_client.GetBlockBlobClient(blob_name);
                auto properties = blob_client.GetProperties().Value;
                std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>(properties.BlobSize);
                blob_client.DownloadTo(buffer->preamble(), buffer->available());
                ARCTICDB_SUBSAMPLE(AzureStorageVisitSegment, 0)
                visitor(k, Segment::from_buffer(std::move(buffer)));

                ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k), variant_key_view(k));
            }
            catch (const Azure::Core::RequestFailedException& e){
                if (!opts.dont_warn_about_missing_key) {
                    log::storage().warn("Failed to read azure segment with key '{}' {} {}: {}",
                                            k,
                                            blob_name,
                                            static_cast<int>(e.StatusCode),
                                            e.ReasonPhrase);
                }
                failed_reads.push_back(k);
            }
        }
    });
    if(!failed_reads.empty())
        throw KeyNotFoundException(Composite<VariantKey>{std::move(failed_reads)});
}

template<class KeyBucketizer>
void do_remove_impl(Composite<VariantKey>&& ks,
                    const std::string& root_folder,
                    Azure::Storage::Blobs::BlobContainerClient& container_client,
                    KeyBucketizer&& bucketizer,
                    bool connect_to_azurite) {
    ARCTICDB_SUBSAMPLE(AzureStorageDeleteBatch, 0)
    auto fmt_db = [](auto&& k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_deletes;
    if (connect_to_azurite){ //https://github.com/Azure/Azurite/issues/1822 due to an open issue of Azurite not filling subrequeset response after batch delete
        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
            [&container_client, &root_folder, b=std::move(bucketizer), &failed_deletes, &ks] (auto&& group) {
                auto key_type_folder = object_store_utils::key_type_folder(root_folder, group.key());
                for (auto& k : group.values()) {
                    auto blob_name = object_store_utils::object_path(b.bucketize(key_type_folder, k), k);
                    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing azure object with key {}", blob_name);
                    ARCTICDB_SUBSAMPLE(AzureStorageDeleteObjects, 0)
                    bool deleted = false;
                    try{
                        auto blob_client = container_client.GetBlockBlobClient(blob_name);
                        auto response = blob_client.Delete();
                        deleted = response.Value.Deleted;
                    }
                    catch (const Azure::Core::RequestFailedException& e){
                        util::raise_rte("Failed to process azure segment remove request {}: {}",
                                            static_cast<int>(e.StatusCode),
                                            e.ReasonPhrase);
                        deleted = false;
                    }
                    if (!deleted)
                        failed_deletes.push_back(k);
                }
            }
        );
    }
    else {
        static const size_t delete_object_limit =
            std::min(BATCH_SUBREQUEST_LIMIT,
                    static_cast<size_t>(ConfigsMap::instance()->get_int("AzureStorage.DeleteBatchSize", BATCH_SUBREQUEST_LIMIT)));
        auto batch = container_client.CreateBatch();

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
            [&container_client, &root_folder, b=std::move(bucketizer), &failed_deletes, &batch, &ks, delete_object_limit=delete_object_limit] (auto&& group) {//bypass incorrect 'set but no used" error for delete_object_limit
                auto key_type_folder = object_store_utils::key_type_folder(root_folder, group.key());
                std::vector<Azure::Storage::DeferredResponse<Azure::Storage::Blobs::Models::DeleteBlobResult>> subrequest_responses;
                for (auto k : folly::enumerate(group.values())) {
                    auto blob_name = object_store_utils::object_path(b.bucketize(key_type_folder, *k), *k);
                    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Removing azure object with key {}", blob_name);
                    subrequest_responses.push_back(batch.DeleteBlob(blob_name));
                    if (k.index + 1 == delete_object_limit || k.index + 1 == group.size()) {
                        ARCTICDB_SUBSAMPLE(AzureStorageDeleteObjects, 0)
                        try{
                            auto response = container_client.SubmitBatch(batch);
                        }
                        catch (const Azure::Core::RequestFailedException& e){
                            util::raise_rte("Failed to create azure segment batch remove request {}: {}",
                                                static_cast<int>(e.StatusCode),
                                                e.ReasonPhrase);
                        }
                        for (const auto& subrequest_response : folly::enumerate(subrequest_responses)) {
                            if (!subrequest_response->GetResponse().Value.Deleted) {
                                const auto& key = group.values()[subrequest_response.index];
                                failed_deletes.push_back(std::move(key));
                            }
                        }
                    }
                }
            }
        );
    }

    util::check(failed_deletes.empty(), "Have {} segment that have not been removed", failed_deletes.size());
    if(!failed_deletes.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
}

inline auto default_prefix_handler() {
    return [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor& key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };
}

template<class Visitor, class KeyBucketizer, class PrefixHandler>
void do_iterate_type_impl(KeyType key_type,
                          Visitor&& visitor,
                          const std::string& root_folder,
                          Azure::Storage::Blobs::BlobContainerClient& container_client,
                          KeyBucketizer&& bucketizer,
                          PrefixHandler&& prefix_handler = default_prefix_handler(),
                          const std::string& prefix = std::string{}
) {
    ARCTICDB_SAMPLE(AzureStorageIterateType, 0)
    auto key_type_dir = object_store_utils::key_type_folder(root_folder, key_type);

    KeyDescriptor key_descriptor(prefix,
        is_ref_key_class(key_type) ? IndexDescriptor::UNKNOWN : IndexDescriptor::TIMESTAMP, FormatType::TOKENIZED);
    auto key_prefix = prefix_handler(prefix, key_type_dir, key_descriptor, key_type);
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Searching for objects with prefix {}", key_prefix);
    const auto root_folder_size = key_type_dir.size() + 1 + bucketizer.bucketize_length(key_type);

    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = key_prefix;
    try{
        for (auto page = container_client.ListBlobs(options); page.HasPage(); page.MoveToNextPage()) {
            for (const auto& blob : page.Blobs) {
                auto key = blob.Name.substr(root_folder_size);
                ARCTICDB_TRACE(log::version(), "Got object_list: {}, key: {}", blob.Name, key);
                auto k = variant_key_from_bytes(
                            reinterpret_cast<uint8_t*>(key.data()),
                            key.size(),
                            key_type);
                ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
                ARCTICDB_SUBSAMPLE(AzureStorageVisitKey, 0)
                visitor(std::move(k));
                ARCTICDB_SUBSAMPLE(AzureStorageCursorNext, 0)
            }
        }
    }
    catch (const Azure::Core::RequestFailedException& e){
        log::storage().warn("Failed to iterate azure blobs '{}' {}: {}",
                            key_type,
                            static_cast<int>(e.StatusCode),
                            e.ReasonPhrase);
    }
}

template<class KeyBucketizer>
bool do_key_exists_impl(
    const VariantKey& key,
    const std::string& root_folder,
    Azure::Storage::Blobs::BlobContainerClient& container_client,
    KeyBucketizer&& bucketizer
) {
    auto key_type_folder = object_store_utils::key_type_folder(root_folder, variant_key_type(key));
    auto blob_name = object_store_utils::object_path(bucketizer.bucketize(key_type_folder, key), key);
    try{
        auto blob_client = container_client.GetBlockBlobClient(blob_name);
        auto properties = blob_client.GetProperties().Value;

        if (properties.ETag.HasValue())
            return true;
    }
    catch (const Azure::Core::RequestFailedException& e){
        ARCTICDB_DEBUG(log::storage(), "Failed to check azure key '{}' {} {}: {}",
                            key,
                            blob_name,
                            static_cast<int>(e.StatusCode),
                            e.ReasonPhrase);
    }
    return false;
}
} //namespace detail

inline void AzureStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    detail::do_write_impl(std::move(kvs), root_folder_, container_client_, object_store_utils::FlatBucketizer{});
}

inline void AzureStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    detail::do_update_impl(std::move(kvs), root_folder_, container_client_, object_store_utils::FlatBucketizer{});
}

template<class Visitor>
void AzureStorage::do_read(Composite<VariantKey>&& ks, Visitor&& visitor, ReadKeyOpts opts) {
    detail::do_read_impl(std::move(ks), std::move(visitor), root_folder_, container_client_, object_store_utils::FlatBucketizer{}, opts);
}

inline void AzureStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    detail::do_remove_impl(std::move(ks), root_folder_, container_client_, object_store_utils::FlatBucketizer{}, connect_to_azurite_);
}



template<class Visitor>
void AzureStorage::do_iterate_type(KeyType key_type, Visitor&& visitor, const std::string& prefix) {
    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };

    detail::do_iterate_type_impl(key_type, std::move(visitor), root_folder_, container_client_, object_store_utils::FlatBucketizer{}, std::move(prefix_handler), prefix);
}

inline bool AzureStorage::do_key_exists(const VariantKey& key) {
    return detail::do_key_exists_impl(key, root_folder_, container_client_, object_store_utils::FlatBucketizer{});
}

} // namespace arcticdb::storage::azure
