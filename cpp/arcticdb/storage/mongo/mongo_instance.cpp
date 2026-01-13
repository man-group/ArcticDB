/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mongo/mongo_instance.hpp>

namespace arcticdb::storage::mongo {

void MongoInstance::init() { MongoInstance::instance_ = std::make_shared<MongoInstance>(); }

std::shared_ptr<MongoInstance> MongoInstance::instance() {
    std::call_once(MongoInstance::init_flag_, &MongoInstance::init);
    return instance_;
}

void MongoInstance::destroy_instance() { MongoInstance::instance_.reset(); }

std::shared_ptr<MongoInstance> MongoInstance::instance_;
std::once_flag MongoInstance::init_flag_;

} // namespace arcticdb::storage::mongo