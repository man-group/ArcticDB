/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/protobufs.hpp>

#include <unordered_map>
#include <memory>
#include <optional>

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
        map_of_##LABEL[label] = val; \
    } \
\
    TYPE get_##LABEL(const std::string& label, TYPE default_val) const { \
        auto it = map_of_##LABEL.find(label); \
        return it == map_of_##LABEL.cend() ? default_val : it->second; \
    } \
 \
    std::optional<TYPE> get_##LABEL(const std::string& label) const { \
        auto it = map_of_##LABEL.find(label); \
        return it == map_of_##LABEL.cend() ? std::nullopt : std::make_optional(it->second); \
    } \
\
    void unset_##LABEL(const std::string& label) { \
        map_of_##LABEL.erase(label); \
    } \

    // Also update python_module.cpp::register_configs_map_api() if below is changed:
    HANDLE_TYPE(int, int64_t)
    HANDLE_TYPE(string, std::string)
    HANDLE_TYPE(double, double)
#undef HANDLE_TYPE

    RuntimeConfig to_proto() {
        RuntimeConfig output{};
        output.mutable_int_values()->insert(std::cbegin(map_of_int), std::cend(map_of_int));
        output.mutable_string_values()->insert(std::cbegin(map_of_string), std::cend(map_of_string));
        output.mutable_double_values()->insert(std::cbegin(map_of_double), std::cend(map_of_double));
        return output;
    }

    void read_proto(const RuntimeConfig& config) {
        for(auto& val : config.int_values())
            map_of_int.insert(std::make_pair(val.first, val.second));

        for(auto& val : config.string_values())
            map_of_string.insert(std::make_pair(val.first, val.second));

        for(auto& val : config.double_values())
            map_of_double.insert(std::make_pair(val.first, val.second));
    }

     static ConfigsMap from_proto(const RuntimeConfig& config) {
        ConfigsMap output{};
        output.read_proto(config);
        return std::move(output);
    }

private:
    std::unordered_map<std::string, uint64_t> map_of_int;
    std::unordered_map<std::string, std::string> map_of_string;
    std::unordered_map<std::string, double> map_of_double;
    std::unique_ptr<std::mutex> mutex_;
};

void read_runtime_config(const RuntimeConfig& config);

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