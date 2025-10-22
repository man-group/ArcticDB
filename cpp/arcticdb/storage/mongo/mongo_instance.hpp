/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <mongocxx/instance.hpp>
#include <mutex>
#include <memory>

namespace arcticdb::storage::mongo {

class MongoInstance {
    mongocxx::instance api_instance_;

  public:
    static std::shared_ptr<MongoInstance> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<MongoInstance> instance();
    static void destroy_instance();
};

} // namespace arcticdb::storage::mongo