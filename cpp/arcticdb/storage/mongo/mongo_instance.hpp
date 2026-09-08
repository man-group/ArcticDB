/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <mongocxx/instance.hpp>
#include <mongocxx/logger.hpp>
#include <mutex>
#include <memory>
#include <arcticdb/log/log.hpp>

namespace arcticdb::storage::mongo {

class logger final : public mongocxx::logger {
  public:
    void operator()(
            mongocxx::log_level level, bsoncxx::stdx::string_view domain, bsoncxx::stdx::string_view message
    ) noexcept override {
        // Downgrade "Couldn't send endSessions" warning to debug level
        // These are harmless intermittent warnings that occur during interpreter exits. The issue persists even when
        // the resource cleanup order follows the driver's manual. Thus it is suppressed.
        // https://jira.mongodb.org/browse/CXX-3379
        if (level == mongocxx::log_level::k_warning && domain == "client" &&
            message.find("Couldn't send \"endSessions\"") != bsoncxx::stdx::string_view::npos) {
            level = mongocxx::log_level::k_debug;
        }

        const std::string msg = fmt::format("MONGO [{}@{}] {}", mongocxx::to_string(level), domain, message);
        auto logger = log::storage();
        switch (level) {
        case mongocxx::log_level::k_error:
            logger.error(msg);
            break;
        case mongocxx::log_level::k_critical:
            logger.critical(msg);
            break;
        case mongocxx::log_level::k_warning:
            logger.warn(msg);
            break;
        case mongocxx::log_level::k_message:
        case mongocxx::log_level::k_info:
            logger.info(msg);
            break;
        case mongocxx::log_level::k_debug:
            logger.debug(msg);
            break;
        case mongocxx::log_level::k_trace:
            logger.trace(msg);
            break;
        default:
            logger.info(msg);
            break;
        }
    }
};

class MongoInstance {
    mongocxx::instance api_instance_{std::make_unique<logger>()};

  public:
    static std::shared_ptr<MongoInstance> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<MongoInstance> instance();
    static void destroy_instance();
};

} // namespace arcticdb::storage::mongo
