/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/azure/azure_storage.hpp>



#include <arcticdb/util/preconditions.hpp>

#include <folly/gen/Base.h>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/composite.hpp>



#include <arcticdb/storage/azure/azure_client_wrapper.hpp>
#include <arcticdb/storage/azure/azure_real_client.hpp>
#include <arcticdb/storage/azure/azure_mock_client.hpp>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace azure {

namespace fg = folly::gen;

namespace detail {

// TODO: fix this temporary workaround to read error code. azure-sdk-cpp client sometimes doesn't properly set the error code.
//  This issue has been raised on the sdk repo https://github.com/Azure/azure-sdk-for-cpp/issues/5369. Once fixed, we should no longer need the following function and would just read e.ErrorCode.
std::string get_error_code(const Azure::Core::RequestFailedException& e) {
    auto error_code = e.ErrorCode;

    if(error_code.empty() && e.RawResponse ) {
        auto headers_map = e.RawResponse->GetHeaders();
        if(auto ec = headers_map.find("x-ms-error-code") ; ec != headers_map.end()) {
            error_code = ec->second;
        }
    }
    return error_code;
}

void raise_azure_exception(const Azure::Core::RequestFailedException& e) {
    auto error_code = get_error_code(e);
    auto status_code = e.StatusCode;
    std::string error_message;

    if(status_code == Azure::Core::Http::HttpStatusCode::NotFound && error_code == AzureErrorCode_to_string(AzureErrorCode::BlobNotFound)) {
        throw KeyNotFoundException(fmt::format("Key Not Found Error: AzureError#{} {}: {}",
                                               static_cast<int>(status_code), error_code, e.ReasonPhrase));
    }

    if(status_code == Azure::Core::Http::HttpStatusCode::Unauthorized || status_code == Azure::Core::Http::HttpStatusCode::Forbidden) {
        raise<ErrorCode::E_PERMISSION>(fmt::format("Permission error: AzureError#{} {}: {}",
                                                   static_cast<int>(status_code), error_code, e.ReasonPhrase));
    }

    if(static_cast<int>(status_code) >= 500) {
        error_message = fmt::format("Unexpected Server Error: AzureError#{} {}: {} {}",
                                    static_cast<int>(status_code), error_code, e.ReasonPhrase, e.what());
    } else {
        error_message = fmt::format("Unexpected Error: AzureError#{} {}: {} {}",
                                    static_cast<int>(status_code), error_code, e.ReasonPhrase, e.what());
    }

    raise<ErrorCode::E_UNEXPECTED_AZURE_ERROR>(error_message);
}

bool is_expected_error_type(const std::string& error_code, Azure::Core::Http::HttpStatusCode status_code) {
    return status_code == Azure::Core::Http::HttpStatusCode::NotFound && (error_code == AzureErrorCode_to_string(AzureErrorCode::BlobNotFound) ||
                                                                          error_code == AzureErrorCode_to_string(AzureErrorCode::ContainerNotFound));
}

void raise_if_unexpected_error(const Azure::Core::RequestFailedException& e) {
    auto error_code = get_error_code(e);
    auto status_code = e.StatusCode;

    if(!is_expected_error_type(error_code, status_code)) {
        raise_azure_exception(e);
    }
}

template<class KeyBucketizer>
void do_write_impl(
    Composite<KeySegmentPair>&& kvs,
    const std::string& root_folder,
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer,
    const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option,
    unsigned int request_timeout) {
        ARCTICDB_SAMPLE(AzureStorageWrite, 0)
        auto fmt_db = [](auto&& kv) { return kv.key_type(); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
            [&azure_client, &root_folder, b=std::move(bucketizer), &upload_option, &request_timeout] (auto&& group) {
                auto key_type_dir = key_type_folder(root_folder, group.key());
                ARCTICDB_TRACE(log::storage(), "Azure key_type_folder is {}", key_type_dir);

                ARCTICDB_SUBSAMPLE(AzureStorageWriteValues, 0)
                for (auto& kv : group.values()) {
                    auto& k = kv.variant_key();
                    auto blob_name = object_path(b.bucketize(key_type_dir, k), k);
                    auto& seg = kv.segment();

                    try {
                        azure_client.write_blob(blob_name, std::move(seg), upload_option, request_timeout);
                    }
                    catch (const Azure::Core::RequestFailedException& e) {
                        raise_azure_exception(e);
                    }

                }
            }
        );
}

template<class KeyBucketizer>
void do_update_impl(
    Composite<KeySegmentPair>&& kvs,
    const std::string& root_folder,
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer,
    const Azure::Storage::Blobs::UploadBlockBlobFromOptions& upload_option,
    unsigned int request_timeout) {
        // azure updates the key if it already exists
        do_write_impl(std::move(kvs), root_folder, azure_client, std::move(bucketizer), upload_option, request_timeout);
}

struct UnexpectedAzureErrorException : public std::exception {};

template<class KeyBucketizer>
void do_read_impl(Composite<VariantKey> && ks,
    const ReadVisitor& visitor,
    const std::string& root_folder,
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer,
    ReadKeyOpts opts,
    const Azure::Storage::Blobs::DownloadBlobToOptions& download_option,
    unsigned int request_timeout) {
        ARCTICDB_SAMPLE(AzureStorageRead, 0)
        auto fmt_db = [](auto&& k) { return variant_key_type(k); };
        std::vector<VariantKey> failed_reads;

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
            [&azure_client, &root_folder, b=std::move(bucketizer), &visitor, &failed_reads,
            opts=opts, &download_option, &request_timeout] (auto&& group) {
            for (auto& k : group.values()) {
                auto key_type_dir = key_type_folder(root_folder, variant_key_type(k));
                auto blob_name = object_path(b.bucketize(key_type_dir, k), k);
                try {
                    visitor(k, azure_client.read_blob(blob_name, download_option, request_timeout));

                    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k), variant_key_view(k));
                }
                catch (const Azure::Core::RequestFailedException& e) {
                    raise_if_unexpected_error(e);
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
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer,
    unsigned int request_timeout) {
        ARCTICDB_SUBSAMPLE(AzureStorageDeleteBatch, 0)
        auto fmt_db = [](auto&& k) { return variant_key_type(k); };
        std::vector<std::string> to_delete;
        static const size_t delete_object_limit =
            std::min(BATCH_SUBREQUEST_LIMIT, static_cast<size_t>(ConfigsMap::instance()->get_int("AzureStorage.DeleteBatchSize", BATCH_SUBREQUEST_LIMIT)));

        auto submit_batch = [&azure_client, &request_timeout](auto &to_delete) {
            try {
                azure_client.delete_blobs(to_delete, request_timeout);
            }
            catch (const Azure::Core::RequestFailedException& e) {
                raise_azure_exception(e);
            }
            to_delete.clear();
        };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach(
            [&root_folder, b=std::move(bucketizer), delete_object_limit=delete_object_limit, &to_delete, &submit_batch] (auto&& group) {//bypass incorrect 'set but no used" error for delete_object_limit
                auto key_type_dir = key_type_folder(root_folder, group.key());
                for (auto k : folly::enumerate(group.values())) {
                    auto blob_name = object_path(b.bucketize(key_type_dir, *k), *k);
                    to_delete.emplace_back(std::move(blob_name));
                    if (to_delete.size() == delete_object_limit) {
                        submit_batch(to_delete);
                    }
                }
            }
        );
        if (!to_delete.empty()) {
            submit_batch(to_delete);
        }
}

std::string prefix_handler(const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor& key_descriptor, KeyType) {
    return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
}

template<class KeyBucketizer>
void do_iterate_type_impl(KeyType key_type,
    const IterateTypeVisitor& visitor,
    const std::string& root_folder,
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer,
    const std::string& prefix = std::string{}) {
        ARCTICDB_SAMPLE(AzureStorageIterateType, 0)
        auto key_type_dir = key_type_folder(root_folder, key_type);
        const auto path_to_key_size = key_type_dir.size() + 1 + bucketizer.bucketize_length(key_type);
        // if prefix is empty, add / to avoid matching both log and logc when key_type_dir is {root_folder}/log
        if (prefix.empty()) {
            key_type_dir += "/";
        }

        KeyDescriptor key_descriptor(prefix,
            is_ref_key_class(key_type) ? IndexDescriptor::UNKNOWN : IndexDescriptor::TIMESTAMP, FormatType::TOKENIZED);
        auto key_prefix = prefix_handler(prefix, key_type_dir, key_descriptor, key_type);

        try {
            for (auto page = azure_client.list_blobs(key_prefix); page.HasPage(); page.MoveToNextPage()) {
                for (const auto& blob : page.Blobs) {
                    auto key = blob.Name.substr(path_to_key_size);
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
        catch (const Azure::Core::RequestFailedException& e) {
            raise_if_unexpected_error(e);
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
    AzureClientWrapper& azure_client,
    KeyBucketizer&& bucketizer) {
        auto key_type_dir = key_type_folder(root_folder, variant_key_type(key));
        auto blob_name = object_path(bucketizer.bucketize(key_type_dir, key), key);
        try {
            return azure_client.blob_exists(blob_name);
        }
        catch (const Azure::Core::RequestFailedException& e) {
            raise_if_unexpected_error(e);
            log::storage().debug("Failed to check azure key '{}' {} {}: {}",
                                key,
                                blob_name,
                                static_cast<int>(e.StatusCode),
                                e.ReasonPhrase);
        }
        return false;
}
} //namespace detail

void AzureStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    detail::do_write_impl(std::move(kvs), root_folder_, *azure_client_, FlatBucketizer{}, upload_option_, request_timeout_);
}

void AzureStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    detail::do_update_impl(std::move(kvs), root_folder_, *azure_client_, FlatBucketizer{}, upload_option_, request_timeout_);
}

void AzureStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
    detail::do_read_impl(std::move(ks), visitor, root_folder_, *azure_client_, FlatBucketizer{}, opts, download_option_, request_timeout_);
}

void AzureStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    detail::do_remove_impl(std::move(ks), root_folder_, *azure_client_, FlatBucketizer{}, request_timeout_);
}

void AzureStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
    detail::do_iterate_type_impl(key_type, visitor, root_folder_, *azure_client_, FlatBucketizer{}, prefix);
}

bool AzureStorage::do_key_exists(const VariantKey& key) {
    return detail::do_key_exists_impl(key, root_folder_, *azure_client_, FlatBucketizer{});
}

} // namespace azure

} // namespace arcticdb::storage


namespace arcticdb::storage::azure{

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;


AzureStorage::AzureStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Storage(library_path, mode),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    request_timeout_(conf.request_timeout() == 0 ? 200000 : conf.request_timeout()) {
        if(conf.use_mock_storage_for_testing()) {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Mock Azure storage");
            azure_client_ = std::make_unique<MockAzureClient>();
        } else {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Real Azure storage");
            azure_client_ = std::make_unique<RealAzureClient>(conf);
        }
        if (conf.ca_cert_path().empty())
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using default CA cert path");
        else
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "CA cert path: {}", conf.ca_cert_path());
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Connecting to Azure Blob Storage: {} Container: {}", conf.endpoint(), conf.container_name());

        if (!conf.prefix().empty()) {
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix found, using: {}", conf.prefix());
            auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
            root_folder_ = object_store_utils::get_root_folder(prefix_path);
        } else
            ARCTICDB_RUNTIME_DEBUG(log::storage(), "Azure prefix not found, will use {}", root_folder_);
        unsigned int max_connections = conf.max_connections() == 0 ? ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", 16) : conf.max_connections();
        upload_option_.TransferOptions.Concurrency = max_connections;
        download_option_.TransferOptions.Concurrency = max_connections;
}

} // namespace arcticdb::storage::azure
