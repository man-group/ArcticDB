/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <mutex>
#include <memory>
#include <mongocxx/instance.hpp>
#include <arcticdb/util/preprocess.hpp>

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

}