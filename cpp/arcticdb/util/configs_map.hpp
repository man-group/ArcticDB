/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <unordered_map>
#include <memory>
#include <optional>
#include <mutex>

namespace arcticdb {

using namespace arcticdb::proto::config;

class ConfigsMap {
public:
    static std::shared_ptr<ConfigsMap> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<ConfigsMap> instance();

    ConfigsMap() :
        mutex_(std::make_unique<std::mutex>()) {
    }

#define HANDLE_TYPE(LABEL, TYPE)     \
    void set_##LABEL(const std::string& label, TYPE val) { \
        map_of_##LABEL[boost::to_upper_copy<std::string>(label)] = val; \
    } \
\
    TYPE get_##LABEL(const std::string& label, TYPE default_val) const { \
        auto it = map_of_##LABEL.find(boost::to_upper_copy<std::string>(label)); \
        return it == map_of_##LABEL.cend() ? default_val : it->second; \
    } \
 \
    std::optional<TYPE> get_##LABEL(const std::string& label) const { \
        auto it = map_of_##LABEL.find(boost::to_upper_copy<std::string>(label)); \
        return it == map_of_##LABEL.cend() ? std::nullopt : std::make_optional(it->second); \
    } \
\
    void unset_##LABEL(const std::string& label) { \
        map_of_##LABEL.erase(boost::to_upper_copy<std::string>(label)); \
    } \

    // Also update python_module.cpp::register_configs_map_api() if below is changed:
    HANDLE_TYPE(int, int64_t)
    HANDLE_TYPE(string, std::string)
    HANDLE_TYPE(double, double)
#undef HANDLE_TYPE

private:
    std::unordered_map<std::string, uint64_t> map_of_int;
    std::unordered_map<std::string, std::string> map_of_string;
    std::unordered_map<std::string, double> map_of_double;
    std::unique_ptr<std::mutex> mutex_;
};

struct ScopedConfig {
    std::string name_;
    std::optional<int64_t> original_;

    ScopedConfig(const std::string& name, int64_t val) :
            name_(name),
            original_(ConfigsMap::instance()->get_int(name)) {
        ConfigsMap::instance()->set_int(name_, val);
    }

    ~ScopedConfig() {
        if(original_)
            ConfigsMap::instance()->set_int(name_, original_.value());
        else
            ConfigsMap::instance()->unset_int(name_);
    }
};

} //namespace arcticdb