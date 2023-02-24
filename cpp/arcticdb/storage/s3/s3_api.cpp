/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/s3/s3_api.hpp>
#include <folly/Singleton.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/log/log.hpp>
#include <cstdlib>

namespace arcticdb::storage::s3 {

S3ApiInstance::S3ApiInstance(Aws::Utils::Logging::LogLevel log_level) :
    log_level_(log_level),
    options_() {
    if(log_level_ > Aws::Utils::Logging::LogLevel::Off) {
      Aws::Utils::Logging::InitializeAWSLogging(
          Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
              "v", log_level, "aws_sdk_"));
    }
    ARCTICDB_RUNTIME_DEBUG(log::storage(), "Begin initializing AWS API");
    Aws::InitAPI(options_);
    // TODO: this is potentially a workaround for https://github.com/aws/aws-sdk-cpp/issues/1410
    //       disabling it on windows.
#ifndef _WIN32
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
