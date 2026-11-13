/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <arcticdb/entity/protobufs.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <unordered_map>

namespace arcticdb {

using namespace arcticdb::proto::config;

class ConfigsMap {
  public:
    static void init();
    static std::shared_ptr<ConfigsMap>& instance() {
        static auto instance_ = std::make_shared<ConfigsMap>();
        return instance_;
    }

#define HANDLE_TYPE(LABEL, TYPE)                                                                                       \
    void set_##LABEL(const std::string& label, TYPE val) {                                                             \
        map_of_##LABEL[boost::to_upper_copy<std::string>(label)] = val;                                                \
        refresh_watched_if_int<TYPE>();                                                                                \
    }                                                                                                                  \
                                                                                                                       \
    TYPE get_##LABEL(const std::string& label, TYPE default_val) const {                                               \
        auto it = map_of_##LABEL.find(boost::to_upper_copy<std::string>(label));                                       \
        return it == map_of_##LABEL.cend() ? default_val : it->second;                                                 \
    }                                                                                                                  \
                                                                                                                       \
    std::optional<TYPE> get_##LABEL(const std::string& label) const {                                                  \
        auto it = map_of_##LABEL.find(boost::to_upper_copy<std::string>(label));                                       \
        return it == map_of_##LABEL.cend() ? std::nullopt : std::make_optional(it->second);                            \
    }                                                                                                                  \
                                                                                                                       \
    void unset_##LABEL(const std::string& label) {                                                                     \
        map_of_##LABEL.erase(boost::to_upper_copy<std::string>(label));                                                \
        refresh_watched_if_int<TYPE>();                                                                                \
    }                                                                                                                  \
                                                                                                                       \
    const std::unordered_map<std::string, TYPE>& get_all_##LABEL() const { return map_of_##LABEL; }                    \
                                                                                                                       \
    void set_all_##LABEL(const std::unordered_map<std::string, TYPE>& entries) {                                       \
        for (const auto& [k, v] : entries) {                                                                           \
            map_of_##LABEL[boost::to_upper_copy<std::string>(k)] = v;                                                  \
        }                                                                                                              \
        refresh_watched_if_int<TYPE>();                                                                                \
    }

    // Also update python_module.cpp::register_configs_map_api() if below is changed:
    HANDLE_TYPE(int, int64_t)
    HANDLE_TYPE(string, std::string)
    HANDLE_TYPE(double, double)
#undef HANDLE_TYPE

    /// A copy of int option `label` that every int setter, unset and set_all keeps current, for hot paths that can't
    /// afford a map lookup. `default_val` applies while the option is unset; the first call for a label sets it. The
    /// reference stays valid for the lifetime of this ConfigsMap.
    const std::atomic<int64_t>& watch_int(const std::string& label, int64_t default_val) {
        std::lock_guard lock{watched_ints_mutex_};
        auto key = boost::to_upper_copy<std::string>(label);
        const auto current = get_int(key, default_val);
        // unordered_map never moves its values, so the returned reference stays valid as more labels are added
        return watched_ints_.try_emplace(std::move(key), default_val, current).first->second.value;
    }

  private:
    struct WatchedInt {
        WatchedInt(int64_t default_val, int64_t current) : default_val(default_val), value(current) {}
        int64_t default_val;
        std::atomic<int64_t> value;
    };

    template<typename T>
    void refresh_watched_if_int() {
        if constexpr (std::is_same_v<T, int64_t>) {
            std::lock_guard lock{watched_ints_mutex_};
            for (auto& [label, watched] : watched_ints_)
                watched.value.store(get_int(label, watched.default_val), std::memory_order_relaxed);
        }
    }

    std::unordered_map<std::string, int64_t> map_of_int;
    std::unordered_map<std::string, std::string> map_of_string;
    std::unordered_map<std::string, double> map_of_double;
    std::mutex watched_ints_mutex_;
    std::unordered_map<std::string, WatchedInt> watched_ints_;
};

struct ScopedConfig {
    using ConfigOptions = std::vector<std::pair<std::string, std::optional<int64_t>>>;
    ConfigOptions originals;
    ScopedConfig(std::string name, int64_t val) : ScopedConfig({{std::move(name), std::make_optional(val)}}) {}

    static ScopedConfig unset_int(std::string name) { return ScopedConfig({{std::move(name), std::nullopt}}); }

    explicit ScopedConfig(ConfigOptions overrides) {
        for (auto& config : overrides) {
            auto& [name, new_value] = config;
            const auto old_val = ConfigsMap::instance()->get_int(name);
            if (new_value.has_value()) {
                ConfigsMap::instance()->set_int(name, *new_value);
            } else {
                ConfigsMap::instance()->unset_int(name);
            }
            originals.emplace_back(std::move(name), old_val);
        }
    }

    ~ScopedConfig() {
        for (const auto& config : originals) {
            const auto& [name, original_value] = config;
            if (original_value.has_value())
                ConfigsMap::instance()->set_int(name, *original_value);
            else
                ConfigsMap::instance()->unset_int(name);
        }
    }
};

} // namespace arcticdb