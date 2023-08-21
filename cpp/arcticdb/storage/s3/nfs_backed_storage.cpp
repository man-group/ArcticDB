/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#define ARCTICDB_NFS_BACKED_STORAGE_H_
#include <arcticdb/storage/s3/nfs_backed_storage-inl.hpp>

namespace arcticdb::storage::nfs_backed {

NfsBackedStorage::NfsBackedStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Storage(library_path, mode),
    s3_api_(s3::S3ApiInstance::instance()),
    s3_client_(s3::get_aws_credentials(conf), s3::get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()) {
    if (!conf.prefix().empty()) {
        ARCTICDB_DEBUG(log::version(), "prefix found, using: {}", conf.prefix());
        auto prefix_path = LibraryPath::from_delim_path(conf.prefix(), '.');
        root_folder_ = object_store_utils::get_root_folder(prefix_path);
    } else {
        ARCTICDB_DEBUG(log::version(), "prefix not found, will use {}", root_folder_);
    }
    // When linking against libraries built with pre-GCC5 compilers, the num_put facet is not initalized on the classic locale
    // Rather than change the locale globally, which might cause unexpected behaviour in legacy code, just add the required
    // facet here
    std::locale locale{ std::locale::classic(), new std::num_put<char>()};
    (void)std::locale::global(locale);
    ARCTICDB_DEBUG(log::storage(), "Opened NFS backed storage at {}", root_folder_);
    s3_api_.reset();
}

} //namespace arcticdb::storage::nfs_backed
