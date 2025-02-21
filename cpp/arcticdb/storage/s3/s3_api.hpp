/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/Aws.h>
#include <memory>
#include <mutex>

namespace arcticdb::storage::s3 {

class S3ApiInstance {
  public:
    S3ApiInstance(Aws::Utils::Logging::LogLevel log_level = Aws::Utils::Logging::LogLevel::Off);
    ~S3ApiInstance();

    static std::shared_ptr<S3ApiInstance> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<S3ApiInstance> instance();
    static void destroy_instance();

  private:
    Aws::Utils::Logging::LogLevel log_level_;
    Aws::SDKOptions options_;
};

} // namespace arcticdb::storage::s3