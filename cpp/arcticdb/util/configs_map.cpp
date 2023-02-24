/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/util/configs_map.hpp>
#include <folly/Singleton.h>

namespace arcticdb {

std::shared_ptr<ConfigsMap> ConfigsMap::instance(){
    std::call_once(ConfigsMap::init_flag_, &ConfigsMap::init);
    return ConfigsMap::instance_;
}

void ConfigsMap::init() {
    ConfigsMap::instance_ = std::make_shared<ConfigsMap>();
}

std::shared_ptr<ConfigsMap> ConfigsMap::instance_;
std::once_flag ConfigsMap::init_flag_;

void read_runtime_config(const RuntimeConfig& config) {
    ConfigsMap::instance()->read_proto(config);
}

} //namespace arcticdb