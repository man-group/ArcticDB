/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_storage.hpp>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <arcticdb/log/log.hpp>
#include <locale>

namespace arcticdb::storage::s3 {

const std::string USE_AWS_CRED_PROVIDERS_TOKEN = "_RBAC_";

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

void check_ec2_metadata_endpoint(const S3ApiInstance& s3_api) {
    auto disabled_env = Aws::Environment::GetEnv("AWS_EC2_METADATA_DISABLED");
    if (Aws::Utils::StringUtils::ToLower(disabled_env.c_str()) == "true") {
        const char* who_disabled = s3_api.is_ec2_metadata_disabled_by_us() ?
            "EC2 metadata endpoint did not respond in time so was bypassed":
            "AWS_EC2_METADATA_DISABLED environment variable is set to true";
        log::storage().warn("{}. This means machine identity authentication will not work.", who_disabled);
    }
}

bool S3Storage::check_creds_and_bucket() {
    using namespace Aws::S3;
    // We cannot easily change the timeouts of the s3_client_, so use async:
    auto future = s3_client_.HeadBucketCallable(Model::HeadBucketRequest().WithBucket(bucket_name_.c_str()));
    auto wait = std::chrono::milliseconds(ConfigsMap::instance()->get_int("S3Storage.CheckBucketMaxWait", 1000));
    if (future.wait_for(wait) == std::future_status::ready) {
        auto outcome = future.get();
        if (!outcome.IsSuccess()) {
            auto& error = outcome.GetError();

#define BUCKET_LOG(level, msg, ...) log::storage().level(msg "\nHTTP Status: {}. Server response: {}", \
            ## __VA_ARGS__, int(error.GetResponseCode()), error.GetMessage().c_str()); break

            // HEAD request can't return the error details, so can't use the more detailed error codes.
            switch (error.GetResponseCode()) {
            case Aws::Http::HttpResponseCode::UNAUTHORIZED:
            case Aws::Http::HttpResponseCode::FORBIDDEN:
                BUCKET_LOG(warn, "Unable to access bucket. Subsequent operations may fail.");
            case Aws::Http::HttpResponseCode::NOT_FOUND:
                BUCKET_LOG(error, "The specified bucket {} does not exist.", bucket_name_);
            default:
                BUCKET_LOG(info, "Unable to determine if the bucket is accessible.");
            }
#undef BUCKET_LOG
        }
        return outcome.IsSuccess();
    } else {
        log::storage().info("Unable to determine if the bucket is accessible. Request timed out.");
    }
    return false;
}

S3Storage::S3Storage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Parent(library_path, mode),
    s3_api_(S3ApiInstance::instance()),
    root_folder_(object_store_utils::get_root_folder(library_path)),
    bucket_name_(conf.bucket_name()) {

    auto creds = get_aws_credentials(conf);

    if (creds.GetAWSAccessKeyId() == USE_AWS_CRED_PROVIDERS_TOKEN && creds.GetAWSSecretKey() == USE_AWS_CRED_PROVIDERS_TOKEN){
        ARCTICDB_RUNTIME_DEBUG(log::storage(), "Using AWS auth mechanisms");
        check_ec2_metadata_endpoint(*s3_api_);
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