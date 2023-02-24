/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/mongo/mongo_instance.hpp>

#include <folly/Singleton.h>

namespace arcticdb::storage::mongo {

void MongoInstance::init() {
    MongoInstance::instance_ = std::make_shared<MongoInstance>();
}

std::shared_ptr<MongoInstance> MongoInstance::instance() {
    std::call_once(MongoInstance::init_flag_, &MongoInstance::init);
    return instance_;
}

void MongoInstance::destroy_instance() {
    MongoInstance::instance_.reset();
}

std::shared_ptr<MongoInstance> MongoInstance::instance_;
std::once_flag MongoInstance::init_flag_;

}