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
 *                                    +--------------------+                     
 *                                    |    QueryStats      |                         
 *                                    +--------------------+                        
 *                                    | - is_enabled_      |                    
 *                                    | - root_levels_[]   |
 *                                    | - thread_local_var_|                  
 *                                    +------|-------------+                    
 *                                           |                                
 *                                           v                                
 *                              +-------------------------+                   +---------------------+  
 *                              | ThreadLocalQueryStatsVar|                   |  GroupingLevel    | 
 *                              +-------------------------+                   +---------------------+ 
 *  Temp. storing folly thread<-| - child_levels_[]       |                   | - stats_[]          | --> Storing non-groupable stats
 *  level. Will be aggregated   | - root_level_           |                   | - next_level_maps_[]|
 *  to root_level when          | - current_level_ -------|------------------>+---------------------+ 
 *  function is finished        +-------------------------+                           |
 *                                            |                                       |
 *                                            |               Extend the chain        |
 *                                            |               and temporarily update  |
 *                                            v               current_level_          |
 *                                    +---------------+                               |
 *                                    |  StatsGroup   |-------------------------------|
 *                                    +---------------+                               v
 *                                    | - prev_level_ |                       +-----------------+          
 *                                    | - start_      |                       | GroupingLevel   |          
 *                                    |               |                       +-----------------+          
 *                                    +---------------+                               |          
 *                                                                                    v                         
 *                                                                            .. (more levels)                          
 *                                                                                                      
 *                                                                                                      
 *                                                                                                      
 *                                                                                                      
 * Structure:
 * - QueryStats: Singleton manager class holding the stats collection framework
 * - ThreadLocalQueryStatsVar: Thread-local storage for stats levels and management
 * - GroupingLevel: Hierarchical node containing stats and references to child levels
 * - StatsGroup: RAII wrapper that temporarily extends the level chain during its lifetime
 *   When created, it adds a new level and when destroyed, it restores the previous level state
 * 
 * Note:
 * To make the query stats model work, there are some requirements:
 * 1. All calls from python level must mark "QUERY_STATS_ADD_GROUP...." at least once in the call stack
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
enum class GroupName : size_t {
    arcticdb_call = 0,
    key_type = 1,
    storage_ops = 2
};

enum class StatsName : size_t {
    result_count = 0,
    total_time_ms = 1,
    count = 2
};

struct GroupingLevel {
    std::array<int64_t, 3> stats_ = {0}; // sizeof(StatsName)
    std::array<std::map<std::string, std::shared_ptr<GroupingLevel>>, 3> next_level_maps_; // sizeof(GroupName)
    void reset_stats();
    void merge_from(const GroupingLevel& other);
};

struct ChildLevel{
    std::shared_ptr<GroupingLevel> parent_level_;
    std::shared_ptr<GroupingLevel> root_level_;
};

// Collection of thread-local variables. Resides in class QueryStats simply for the sake of convenience
struct ThreadLocalQueryStatsVar {
    std::mutex child_level_creation_mutex_;
    std::vector<ChildLevel> child_levels_;
    std::shared_ptr<GroupingLevel> root_level_ = nullptr;
    std::shared_ptr<GroupingLevel> current_level_ = nullptr;
};


class QueryStats {
public:
    std::shared_ptr<GroupingLevel> current_level();
    std::shared_ptr<GroupingLevel> root_level();
    const std::vector<std::shared_ptr<GroupingLevel>>& root_levels() const;
    bool is_root_level_set() const;
    void create_child_level(ThreadLocalQueryStatsVar& parent_thread_local_var);
    void set_root_level(std::shared_ptr<GroupingLevel> &level);
    void set_level(std::shared_ptr<GroupingLevel> &level);
    void reset_stats();
    static QueryStats& instance();
    void merge_levels();
    void enable();
    void disable();
    bool is_enabled() const;
    QueryStats(const QueryStats&) = delete;
    QueryStats() = default;

    thread_local inline static ThreadLocalQueryStatsVar thread_local_var_;
private:
    bool is_enabled_ = false;
    std::mutex root_level_mutex_; 
    // As GIL could be released, each python thread will have their own root level.
    std::vector<std::shared_ptr<GroupingLevel>> root_levels_;
};
    
    
// function-local object so the additional info will be removed from the stack when the info object gets detroyed
class StatsGroup {
public:
    StatsGroup(bool log_time, GroupName col, const std::string& value);
    ~StatsGroup();
private:
    std::shared_ptr<GroupingLevel> prev_level_;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
    bool log_time_;
};

std::string format_group_value(GroupName col_value, auto&& value) {
    constexpr bool is_value_key_type = std::is_same_v<std::remove_cv_t<std::remove_reference_t<decltype(value)>>, entity::KeyType>;
    check(
        col_value != util::query_stats::GroupName::key_type || is_value_key_type, 
        "arcticdb_call group name is reserved"
    );
    if constexpr (is_value_key_type) {
        return arcticdb::entity::key_type_long_name(value);
    }
    else {
        return fmt::format("{}", std::forward<decltype(value)>(value));
    }
}

}

#define STATS_GROUP_VAR_NAME(x) query_stats_info##x

#define QUERY_STATS_ADD_GROUP_IMPL(log_time, col_name, value) \
    std::optional<arcticdb::util::query_stats::StatsGroup> STATS_GROUP_VAR_NAME(col_name); \
    namespace arcticdb::util::query_stats {\
        if (QueryStats::instance().is_enabled()) { \
            STATS_GROUP_VAR_NAME(col_name).emplace( \
                log_time, \
                GroupName::col_name, \
                format_group_value(GroupName::col_name, value)); \
        }
    } \
#define QUERY_STATS_ADD_GROUP(col_name, value) QUERY_STATS_ADD_GROUP_IMPL(false, col_name, value)
#define QUERY_STATS_ADD_GROUP_WITH_TIME(col_name, value) QUERY_STATS_ADD_GROUP_IMPL(true, col_name, value)

#define QUERY_STATS_ADD(col_name, value) \
    if (arcticdb::util::query_stats::QueryStats::instance().is_enabled()) { \
        auto& query_stats_instance = arcticdb::util::query_stats::QueryStats::instance(); \
        auto& stats = query_stats_instance.current_level()->stats_; \
        stats[static_cast<size_t>(arcticdb::util::query_stats::StatsName::col_name)] += value; \
    }

