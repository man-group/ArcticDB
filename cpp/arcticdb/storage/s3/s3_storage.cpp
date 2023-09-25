/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#define ARCTICDB_S3_STORAGE_H_
#include <arcticdb/storage/s3/s3_storage-inl.hpp>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/log/log.hpp>
#include <locale>

namespace arcticdb::storage::s3 {

namespace detail {
std::streamsize S3StreamBuffer::xsputn(const char_type* s, std::streamsize n) {
    ARCTICDB_DEBUG(log::version(), "xsputn {} pos at {}, {} bytes", uintptr_t(buffer_.get()), pos_, n);
    if(buffer_->bytes() < pos_ + n) {
        ARCTICDB_DEBUG(log::version(), "{} Calling ensure for {}", uintptr_t(buffer_.get()), (pos_ + n) * 2);
        buffer_->ensure((pos_ + n) * 2);
    }

    auto target = buffer_->ptr_cast<char_type>(pos_, n);
    ARCTICDB_DEBUG(log::version(), "Putting {} bytes at {}", n, uintptr_t(target));
    memcpy(target, s, n);
    pos_ += n;
    ARCTICDB_DEBUG(log::version(), "{} pos is now {}, returning {}", uintptr_t(buffer_.get()), pos_, n);
    return n;
}
}

S3Storage::S3Storage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Storage(library_path, mode),
    s3_api_(S3ApiInstance::instance()),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()) {

    auto creds = get_aws_credentials(conf);

    if (creds.GetAWSAccessKeyId() == USE_AWS_CRED_PROVIDERS_TOKEN && creds.GetAWSSecretKey() == USE_AWS_CRED_PROVIDERS_TOKEN){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using AWS auth mechanisms");
        s3_client_ = Aws::S3::S3Client(get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using provided auth credentials");
        s3_client_ = Aws::S3::S3Client(creds, get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, conf.use_virtual_addressing());
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
