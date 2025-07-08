/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_storage.hpp>

#include <locale>

#include <aws/identity-management/auth/STSProfileCredentialsProvider.h>
#include <aws/sts/STSEndpointProvider.h>

#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/util/buffer_pool.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <arcticdb/storage/s3/s3_client_impl.hpp>
#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

std::string S3Storage::name() const {
    return fmt::format("s3_storage-{}/{}/{}", region_, bucket_name_, root_folder_);
}

std::string S3Storage::get_key_path(const VariantKey& key) const {
    auto b = FlatBucketizer{};
    auto key_type_dir = key_type_folder(root_folder_, variant_key_type(key));
    return object_path(b.bucketize(key_type_dir, key), key);
    // FUTURE: if we can reuse `detail::do_*` below doing polymorphism instead of templating, then this method is useful
    // to most of them
}

void S3Storage::do_write(KeySegmentPair& key_seg) {
    detail::do_write_impl(key_seg, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

void S3Storage::do_write_if_none(KeySegmentPair& kv) {
    detail::do_write_if_none_impl(kv, root_folder_, bucket_name_, *s3_client_, FlatBucketizer{});
}

void S3Storage::do_update(KeySegmentPair& key_seg, UpdateOpts) {
    detail::do_update_impl(key_seg, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

void S3Storage::do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
    auto identity = [](auto&& k) { return k; };		
    detail::do_read_impl(std::move(variant_key), visitor, root_folder_, bucket_name_, client(), FlatBucketizer{}, std::move(identity), opts);
}

KeySegmentPair S3Storage::do_read(VariantKey&& variant_key, ReadKeyOpts opts) {
    auto identity = [](auto&& k) { return k; };		
    return detail::do_read_impl(std::move(variant_key), root_folder_, bucket_name_, client(), FlatBucketizer{}, std::move(identity), opts);
}

folly::Future<folly::Unit> S3Storage::do_async_read(entity::VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
    auto identity = [](auto&& k) { return k; };		
    return detail::do_async_read_impl(std::move(variant_key), root_folder_, bucket_name_, client(), FlatBucketizer{}, std::move(identity), opts).thenValue([&visitor] (auto&& key_seg) {
        visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
        return folly::Unit{};
    });
}

folly::Future<KeySegmentPair> S3Storage::do_async_read(entity::VariantKey&& variant_key, ReadKeyOpts opts) {
    auto identity = [](auto&& k) { return k; };		
    return detail::do_async_read_impl(std::move(variant_key), root_folder_, bucket_name_, client(), FlatBucketizer{}, std::move(identity), opts);
}

void S3Storage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts) {
    detail::do_remove_impl(variant_keys, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

void S3Storage::do_remove(VariantKey&& variant_key, RemoveOpts) {
    detail::do_remove_impl(std::move(variant_key), root_folder_, bucket_name_, client(), FlatBucketizer{});
}

void GCPXMLStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts) {
    // GCP does not support batch deletes
    detail::do_remove_no_batching_impl(variant_keys, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

void GCPXMLStorage::do_remove(VariantKey&& variant_key, RemoveOpts) {
    // GCP does not support batch deletes
    std::span<VariantKey> keys{&variant_key, 1};
    detail::do_remove_no_batching_impl(keys, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

bool S3Storage::do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix) {
    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor& key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };

    return detail::do_iterate_type_impl(key_type, visitor, root_folder_, bucket_name_, client(), FlatBucketizer{}, prefix_handler, prefix);
}

void S3Storage::do_visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) {
    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor& key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };

    detail::do_visit_object_sizes_for_type_impl(key_type, root_folder_, bucket_name_, client(), FlatBucketizer{},
                                                prefix_handler, prefix, visitor);
}

bool S3Storage::do_key_exists(const VariantKey& key) {
    return detail::do_key_exists_impl(key, root_folder_, bucket_name_, client(), FlatBucketizer{});
}

} // namespace s3
} // namespace arcticdb::storage

namespace arcticdb::storage::s3 {

void S3Storage::create_s3_client(const S3Settings &conf, const Aws::Auth::AWSCredentials& creds) {
    if (conf.use_mock_storage_for_testing()){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Mock S3 storage");
        s3_client_ = std::make_unique<MockS3Client>();
    }
    else if (conf.aws_auth() == AWSAuthMethod::STS_PROFILE_CREDENTIALS_PROVIDER){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Load sts profile credentials provider");
        Aws::Config::ReloadCachedConfigFile(); // config files loaded in Aws::InitAPI; It runs once at first S3Storage object construct; reload to get latest
        auto client_config = get_s3_config_and_set_env_var(conf);
        auto sts_client_factory = [conf, this](const Aws::Auth::AWSCredentials& creds) { // Get default allocation tag
            auto sts_config = get_proxy_config(conf.https() ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP);
            auto allocation_tag = Aws::STS::STSClient::GetAllocationTag();
            sts_client_ = std::make_unique<Aws::STS::STSClient>(creds, Aws::MakeShared<Aws::STS::Endpoint::STSEndpointProvider>(allocation_tag), sts_config);
            return sts_client_.get();
        };
        auto cred_provider = Aws::MakeShared<Aws::Auth::STSProfileCredentialsProvider>(
            "DefaultAWSCredentialsProviderChain",
            conf.aws_profile(),
            std::chrono::minutes(static_cast<size_t>(ConfigsMap::instance()->get_int("S3Storage.STSTokenExpiryMin", 60))),
            sts_client_factory
        );
        s3_client_ = std::make_unique<S3ClientImpl>(cred_provider, client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    }
    else if (creds.GetAWSAccessKeyId() == USE_AWS_CRED_PROVIDERS_TOKEN && creds.GetAWSSecretKey() == USE_AWS_CRED_PROVIDERS_TOKEN){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using AWS auth mechanisms");
        s3_client_ = std::make_unique<S3ClientImpl>(get_s3_config_and_set_env_var(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using provided auth credentials");
        s3_client_ = std::make_unique<S3ClientImpl>(creds, get_s3_config_and_set_env_var(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    }

    if (conf.use_internal_client_wrapper_for_testing()){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using internal client wrapper for testing");
        s3_client_ = std::make_unique<S3ClientTestWrapper>(std::move(s3_client_));
    }
}

S3Storage::S3Storage(const LibraryPath &library_path, OpenMode mode, const S3Settings &conf) :
    Storage(library_path, mode),
    s3_api_(S3ApiInstance::instance()),  // make sure we have an initialized AWS SDK
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()),
    region_(conf.region()) {
    auto creds = get_aws_credentials(conf);

    create_s3_client(conf, creds);

    if (conf.prefix().empty()) {
        ARCTICDB_DEBUG(log::version(), "prefix not found, will use {}", root_folder_);
    } else if (conf.use_raw_prefix()) {
            ARCTICDB_DEBUG(log::version(), "raw prefix found, using: {}", conf.prefix());
            root_folder_ = conf.prefix();
    } else {
        auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
        root_folder_ = object_store_utils::get_root_folder(prefix_path);
        ARCTICDB_DEBUG(log::version(), "parsed prefix found, using: {}", root_folder_);
    }

    // When linking against libraries built with pre-GCC5 compilers, the num_put facet is not initalized on the classic locale
    // Rather than change the locale globally, which might cause unexpected behaviour in legacy code, just add the required
    // facet here
    std::locale locale{ std::locale::classic(), new std::num_put<char>()};
    (void)std::locale::global(locale);
    ARCTICDB_DEBUG(log::storage(), "Opened S3 backed storage at {}", root_folder_);
}

bool S3Storage::supports_object_size_calculation() const {
    return true;
}

GCPXMLStorage::GCPXMLStorage(const arcticdb::storage::LibraryPath& lib,
                             arcticdb::storage::OpenMode mode,
                             const arcticdb::storage::s3::GCPXMLSettings& conf) :
                             S3Storage(lib, mode, S3Settings{AWSAuthMethod::DISABLED, "", false}.update(conf)) {

}

} // namespace arcticdb::storage::s3
