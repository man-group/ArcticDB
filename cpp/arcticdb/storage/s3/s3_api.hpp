/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <memory>
#include <mutex>

namespace arcticdb::storage::s3 {

class StdErrLogSystem : public Aws::Utils::Logging::FormattedLogSystem {
  public:
    explicit StdErrLogSystem(Aws::Utils::Logging::LogLevel log_level) : FormattedLogSystem(log_level) {}
    void Flush() override;

  protected:
    void ProcessFormattedStatement(Aws::String&& statement) override;
};

class S3ApiInstance {
  public:
    S3ApiInstance(
            Aws::Utils::Logging::LogLevel log_level = Aws::Utils::Logging::LogLevel::Off, bool log_to_file = false
    );
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