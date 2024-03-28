/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_storage.hpp>

#include <locale>


#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/storage/object_store_utils.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/util/composite.hpp>



#include <arcticdb/storage/s3/s3_real_client.hpp>
#include <arcticdb/storage/s3/s3_mock_client.hpp>
#include <arcticdb/storage/s3/detail-inl.hpp>

#undef GetMessage

namespace arcticdb::storage {

using namespace object_store_utils;

namespace s3 {

namespace fg = folly::gen;


std::string S3Storage::get_key_path(const VariantKey& key) const {
    auto b = FlatBucketizer{};
    auto key_type_dir = key_type_folder(root_folder_, variant_key_type(key));
    return object_path(b.bucketize(key_type_dir, key), key);
    // FUTURE: if we can reuse `detail::do_*` below doing polymorphism instead of templating, then this method is useful
    // to most of them
}

void S3Storage::do_write(Composite<KeySegmentPair>&& kvs) {
    detail::do_write_impl(std::move(kvs), root_folder_, bucket_name_, *s3_client_, FlatBucketizer{});
}

void S3Storage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    detail::do_update_impl(std::move(kvs), root_folder_, bucket_name_, *s3_client_, FlatBucketizer{});
}

void S3Storage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
    detail::do_read_impl(std::move(ks), visitor, root_folder_, bucket_name_, *s3_client_, FlatBucketizer{}, opts);
}

void S3Storage::do_remove(Composite<VariantKey>&& ks, RemoveOpts) {
    detail::do_remove_impl(std::move(ks), root_folder_, bucket_name_, *s3_client_, FlatBucketizer{});
}

void S3Storage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
    auto prefix_handler = [] (const std::string& prefix, const std::string& key_type_dir, const KeyDescriptor& key_descriptor, KeyType) {
        return !prefix.empty() ? fmt::format("{}/{}*{}", key_type_dir, key_descriptor, prefix) : key_type_dir;
    };

    detail::do_iterate_type_impl(key_type, visitor, root_folder_, bucket_name_, *s3_client_, FlatBucketizer{}, std::move(prefix_handler), prefix);
}

bool S3Storage::do_key_exists(const VariantKey& key) {
    return detail::do_key_exists_impl(key, root_folder_, bucket_name_, *s3_client_, FlatBucketizer{});
}


} // namespace s3
} // namespace arcticdb::storage


namespace arcticdb::storage::s3 {

S3Storage::S3Storage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Storage(library_path, mode),
    s3_api_(S3ApiInstance::instance()),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()) {

    auto creds = get_aws_credentials(conf);

    if (conf.use_mock_storage_for_testing()){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using Mock S3 storage");
        s3_client_ = std::make_unique<MockS3Client>();
    }
    else if (creds.GetAWSAccessKeyId() == USE_AWS_CRED_PROVIDERS_TOKEN && creds.GetAWSSecretKey() == USE_AWS_CRED_PROVIDERS_TOKEN){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using AWS auth mechanisms");
        s3_client_ = std::make_unique<RealS3Client>(get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using provided auth credentials");
        s3_client_ = std::make_unique<RealS3Client>(creds, get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    }

    if (!conf.prefix().empty()) {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "S3 prefix found, using: {}", conf.prefix());
        auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
        root_folder_ = object_store_utils::get_root_folder(prefix_path);
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "S3 prefix not found, will use {}", root_folder_);
    }
    // When linking against libraries built with pre-GCC5 compilers, the num_put facet is not initalized on the classic locale
    // Rather than change the locale globally, which might cause unexpected behaviour in legacy code, just add the required
    // facet here
    std::locale locale{ std::locale::classic(), new std::num_put<char>()};
    (void)std::locale::global(locale);
    s3_api_.reset();
}

} // namespace arcticdb::storage::s3
