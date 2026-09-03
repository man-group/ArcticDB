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

#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace arcticdb {

using namespace arcticdb::proto::config;

/*
 * A process-global singleton that is read from hot paths on many threads while
 * other threads mutate it. VersionMap::has_cached_entry, for example, reads
 * VersionMap.ReloadInterval on every version-map access, so any background
 * thread touching a version map is a continuous reader; set_int/unset_int (via
 * the set_config_int/unset_config_int bindings) are the writers.
 *
 * The maps must therefore be guarded. Unsynchronised, an insert that rehashes
 * relinks every bucket and an erase frees a node, either of which leaves a
 * concurrent find() walking a dangling pointer -- a segfault inside
 * std::unordered_map::find, not an incorrect config value.
 *
 * A shared_mutex suits the access pattern: reads vastly outnumber writes. The
 * uncontended shared lock measures at roughly +33ns on a ~195ns accessor
 * (gcc 11, -O2, aarch64; 20M single-threaded get_int calls), i.e. about 17% of
 * a call that is already dominated by the boost::to_upper_copy allocation and
 * the hash lookup. That is immaterial next to the work has_cached_entry guards
 * -- a version-map entry lookup, and a storage round trip whenever the cache
 * misses -- so the cost is paid where it cannot be measured. Keys are
 * upper-cased outside the critical section to keep the hold time short.
 *
 * If that overhead ever does matter, the alternative is copy-on-write behind an
 * atomic<shared_ptr<const map>>: lock-free for readers, whole-map copy for the
 * rare writer. It was not chosen here because it is materially harder to review
 * for a path where the measured cost is already in the noise.
 */
class ConfigsMap {
  public:
    static void init();
    static std::shared_ptr<ConfigsMap>& instance() {
        static auto instance_ = std::make_shared<ConfigsMap>();
        return instance_;
    }

#define HANDLE_TYPE(LABEL, TYPE)                                                                                       \
    void set_##LABEL(const std::string& label, TYPE val) {                                                             \
        auto key = boost::to_upper_copy<std::string>(label);                                                           \
        std::unique_lock lock(mutex_);                                                                                 \
        map_of_##LABEL[std::move(key)] = std::move(val);                                                               \
    }                                                                                                                  \
                                                                                                                       \
    TYPE get_##LABEL(const std::string& label, TYPE default_val) const {                                               \
        const auto key = boost::to_upper_copy<std::string>(label);                                                     \
        std::shared_lock lock(mutex_);                                                                                 \
        auto it = map_of_##LABEL.find(key);                                                                            \
        return it == map_of_##LABEL.cend() ? default_val : it->second;                                                 \
    }                                                                                                                  \
                                                                                                                       \
    std::optional<TYPE> get_##LABEL(const std::string& label) const {                                                  \
        const auto key = boost::to_upper_copy<std::string>(label);                                                     \
        std::shared_lock lock(mutex_);                                                                                 \
        auto it = map_of_##LABEL.find(key);                                                                            \
        return it == map_of_##LABEL.cend() ? std::nullopt : std::make_optional(it->second);                            \
    }                                                                                                                  \
                                                                                                                       \
    void unset_##LABEL(const std::string& label) {                                                                     \
        const auto key = boost::to_upper_copy<std::string>(label);                                                     \
        std::unique_lock lock(mutex_);                                                                                 \
        map_of_##LABEL.erase(key);                                                                                     \
    }                                                                                                                  \
                                                                                                                       \
    /* Returns a copy: handing out a reference would let the caller read the    */                                     \
    /* map while another thread mutates it, reintroducing the race.             */                                     \
    std::unordered_map<std::string, TYPE> get_all_##LABEL() const {                                                    \
        std::shared_lock lock(mutex_);                                                                                 \
        return map_of_##LABEL;                                                                                         \
    }                                                                                                                  \
                                                                                                                       \
    void set_all_##LABEL(const std::unordered_map<std::string, TYPE>& entries) {                                       \
        std::vector<std::pair<std::string, TYPE>> upper;                                                               \
        upper.reserve(entries.size());                                                                                 \
        for (const auto& [k, v] : entries) {                                                                           \
            upper.emplace_back(boost::to_upper_copy<std::string>(k), v);                                               \
        }                                                                                                              \
        std::unique_lock lock(mutex_);                                                                                 \
        for (auto& [k, v] : upper) {                                                                                   \
            map_of_##LABEL[std::move(k)] = std::move(v);                                                               \
        }                                                                                                              \
    }

    // Also update python_module.cpp::register_configs_map_api() if below is changed:
    HANDLE_TYPE(int, int64_t)
    HANDLE_TYPE(string, std::string)
    HANDLE_TYPE(double, double)
#undef HANDLE_TYPE

  private:
    /* Guards all three maps. mutable so the const getters can take a shared lock. */
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, int64_t> map_of_int;
    std::unordered_map<std::string, std::string> map_of_string;
    std::unordered_map<std::string, double> map_of_double;
};

struct ScopedConfig {
    using ConfigOptions = std::vector<std::pair<std::string, std::optional<int64_t>>>;
    ConfigOptions originals;
    ScopedConfig(std::string name, int64_t val) : ScopedConfig({{std::move(name), std::make_optional(val)}}) {}

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