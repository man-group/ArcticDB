/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

/*
 * Class Structure Diagram:
 *                                    
 *                                    +------------------+                     
 *                                    |    QueryStats    |                         
 * Temp. storing folly thread root    +------------------+                        
 * layer. Will be aggregated to   <...| - child_layers_[]|                    +---------------------+
 * root_layer when func. is finished  | - root_layer_ ---|------------------->|  StatsGroupLayer    |
 *                                    | - is_enabled_    |                    +---------------------+
 *    Ref. pointer to the layer   <...| - current_layer_ |                    | - stats_[]          | .......> Storing non-groupable stats
 *    in use                          +------------------+                    | - next_layer_maps_[]|
 *                                            ^                               +---------------------+                     
 *                                            |                                        |
 *                                            |                                        |
 *                                            |                                        |
 *                                            |               Extend the chain         |
 *                                            |               and temporarily update   |
 *                                    +-------+-------+   QueryStats's current_layer_  |
 *                                    |  StatsGroup   |................................|
 *                                    +---------------+                                v
 *                                    | - prev_layer_ |                       +-------------------+          
 *                                    | - start_      |                       | StatsGroupLayer   |-----> ... (more layers)          
 *                                    | - log_time_   |                       +-------------------+          
 *                                    +---------------+                                 
 *                     
 * Structure:
 * - QueryStats: Singleton manager class holding the stats collection framework
 * - StatsGroupLayer: Hierarchical node containing stats and references to child layers
 * - StatsGroup: RAII wrapper that temporarily extends the layer chain during its lifetime
 *   When created, it adds a new layer and when destroyed, it restores the previous layer state
 * 
 * Note:
 * To make the query stats model works, there are two requirements:
 * 1. All calls from python layer must mark "QUERY_STATS_ADD_GROUP...." at least once in the call stack
 *    so the stats logged in folly threads will be aggregated to the master map
 *    (Checking will be added after all log entries are added)
 * 2. All folly tasks must be submitted through the TaskScheduler::submit_cpu_task/submit_io_task
 * 3. All folly tasks must complete ("collected") before last StatsGroup object is destroyed in the call stack
 */

#include <mutex>
#include <memory>
#include <vector>
#include <list>
#include <map>
#include <atomic>
#include <type_traits>
#include <string>
#include <ctime>
#include <chrono>
#include <array>
#include <fmt/format.h>

namespace arcticdb::util::query_stats {
using StatsGroups = std::vector<std::shared_ptr<std::pair<std::string, std::string>>>;


// process-global stats entry list
enum class StatsGroupName : size_t {
    arcticdb_call = 0,
    key_type = 1,
    storage_ops = 2
};

enum class StatsName : size_t {
    result_count = 0,
    total_time_ms = 1,
    count = 2
};

class StatsGroupLayer {
public:
    std::array<int64_t, 3> stats_ = {0}; // sizeof(StatsName)
    std::array<std::map<std::string, std::shared_ptr<StatsGroupLayer>>, 3> next_layer_maps_; // sizeof(StatsGroupName)
    void reset_stats();
    void merge_from(const StatsGroupLayer& other);
};

struct ThreadLocalQueryStatsVar {
    std::mutex child_layer_creation_mutex_;
    std::vector<std::pair<std::shared_ptr<StatsGroupLayer>, std::shared_ptr<StatsGroupLayer>>> child_layers_;
    std::shared_ptr<StatsGroupLayer> root_layer_ = nullptr;
    std::shared_ptr<StatsGroupLayer> current_layer_ = nullptr;
};


class QueryStats {
public:
    std::shared_ptr<StatsGroupLayer> current_layer();
    std::shared_ptr<StatsGroupLayer> root_layer();
    const std::vector<std::shared_ptr<StatsGroupLayer>>& root_layers() const;
    bool is_root_layer_set();
    void create_child_layer(ThreadLocalQueryStatsVar& parent_thread_local_var);
    void set_root_layer(std::shared_ptr<StatsGroupLayer> &layer);
    void set_layer(std::shared_ptr<StatsGroupLayer> &layer);
    void reset_stats();
    static QueryStats& instance();
    bool is_enabled_ = false;
    void merge_layers();

    thread_local inline static ThreadLocalQueryStatsVar thread_local_var_;
private:
    std::mutex root_layer_mutex_; 
    std::vector<std::shared_ptr<StatsGroupLayer>> root_layers_; 
};
    
    
// function-local object so the additional info will be removed from the stack when the info object gets detroyed
class StatsGroup {
public:
    StatsGroup(bool log_time, StatsGroupName col, const std::string& value);
    ~StatsGroup();
private:
    std::shared_ptr<StatsGroupLayer> prev_layer_;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
    bool log_time_;
};

}

#define STATS_GROUP_VAR_NAME(x) query_stats_info##x

#define QUERY_STATS_ADD_GROUP_IMPL(log_time, col_name, value) \
    std::optional<arcticdb::util::query_stats::StatsGroup> STATS_GROUP_VAR_NAME(col_name); \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled_) { \
        STATS_GROUP_VAR_NAME(col_name).emplace(log_time, arcticdb::util::query_stats::StatsGroupName::col_name, fmt::format("{}", value)); \
    }
#define QUERY_STATS_ADD_GROUP(col_name, value) QUERY_STATS_ADD_GROUP_IMPL(false, col_name, value)
#define QUERY_STATS_ADD_GROUP_WITH_TIME(col_name, value) QUERY_STATS_ADD_GROUP_IMPL(true, col_name, value)

#define QUERY_STATS_ADD(col_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled_) { \
        auto& query_stats_instance = arcticdb::util::query_stats::QueryStats::instance(); \
        auto& stats = query_stats_instance.current_layer()->stats_; \
        stats[static_cast<size_t>(arcticdb::util::query_stats::StatsName::col_name)] += value; \
    }

