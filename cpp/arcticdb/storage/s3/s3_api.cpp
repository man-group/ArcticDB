/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/s3/s3_api.hpp>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/s3/ec2_utils.hpp>
#include <cstdlib>

namespace arcticdb::storage::s3 {

S3ApiInstance::S3ApiInstance(Aws::Utils::Logging::LogLevel log_level) :
    log_level_(log_level),
    options_() {
    // Use correct URI encoding rather than legacy compat one in AWS SDK. PURE S3 needs this to handle symbol names
    // that have special characters (eg ':').
    options_.httpOptions.compliantRfc3986Encoding = true;

    if(log_level_ > Aws::Utils::Logging::LogLevel::Off) {
      Aws::Utils::Logging::InitializeAWSLogging(
          Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
              "v", log_level, "aws_sdk_"));
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Begin initializing AWS API");
    Aws::InitAPI(options_);
    // A workaround for https://github.com/aws/aws-sdk-cpp/issues/1410.
    if (is_running_inside_aws_fast()) {
        return;
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(),
        "Does not appear to be using AWS. Will set AWS_EC2_METADATA_DISABLED");
#ifdef WIN32
    _putenv_s("AWS_EC2_METADATA_DISABLED", "true");
#else
    setenv("AWS_EC2_METADATA_DISABLED", "true", true);
#endif
}

S3ApiInstance::~S3ApiInstance() {
    if(log_level_ > Aws::Utils::Logging::LogLevel::Off)
        Aws::Utils::Logging::ShutdownAWSLogging();

    //Aws::ShutdownAPI(options_); This causes a crash on shutdown in Aws::CleanupMonitoring
}

void S3ApiInstance::init() {
  	auto log_level = ConfigsMap::instance()->get_int("AWS.LogLevel", 0);
    S3ApiInstance::instance_ = std::make_shared<S3ApiInstance>(Aws::Utils::Logging::LogLevel(log_level));
}

std::shared_ptr<S3ApiInstance> S3ApiInstance::instance() {
    std::call_once(S3ApiInstance::init_flag_, &S3ApiInstance::init);
    return instance_;
}

void S3ApiInstance::destroy_instance() {
    S3ApiInstance::instance_.reset();
}

std::shared_ptr<S3ApiInstance> S3ApiInstance::instance_;
std::once_flag S3ApiInstance::init_flag_;


} // namespace arcticdb::storage::s3
