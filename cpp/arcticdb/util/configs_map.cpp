/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/configs_map.hpp>

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

} //namespace arcticdb