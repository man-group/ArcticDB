/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_storage_tool.hpp>

namespace arcticdb::storage::s3 {

S3StorageTool::S3StorageTool(const Config &conf) :
    s3_api_(S3ApiInstance::instance()),
    s3_client_(get_aws_credentials(conf), get_s3_config(conf), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false),
    bucket_name_(conf.bucket_name()) {
    std::locale locale{ std::locale::classic(), new std::num_put<char>()};
    (void)std::locale::global(locale);
    ARCTICDB_DEBUG(log::storage(), "Created S3 storage tool for bucket {}", bucket_name_);
}

}  //namespace arcticdb::storage::s3
