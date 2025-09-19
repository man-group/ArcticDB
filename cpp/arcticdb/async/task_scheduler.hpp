/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <fmt/format.h>
#include <thread>
#include <algorithm>
#include <filesystem>
#include <string>
#include <fstream>
#include <type_traits>

namespace arcticdb::async {
class TaskScheduler;

struct TaskSchedulerPtrWrapper{
    TaskScheduler* ptr_;

    explicit TaskSchedulerPtrWrapper(TaskScheduler* ptr) : ptr_(ptr) {
        util::check(ptr != nullptr, "Null TaskScheduler ptr");
    }

    TaskSchedulerPtrWrapper() : ptr_(nullptr) {
    }

    ~TaskSchedulerPtrWrapper();

    void reset(TaskScheduler* ptr) {
        ptr_ = ptr;
    }

    TaskScheduler* operator->() const {
        return ptr_;
    }

    TaskScheduler& operator*() const {
        return *ptr_;
    }
};

class InstrumentedNamedFactory : public folly::ThreadFactory{
public:
    explicit InstrumentedNamedFactory(folly::StringPiece prefix) : named_factory_(prefix){}

    std::thread newThread(folly::Func&& func) override {
        std::lock_guard lock{mutex_};
        return named_factory_.newThread(
                [func = std::move(func)]() mutable {
                ARCTICDB_SAMPLE_THREAD();
              func();
            });
    
  }

    virtual const std::string& getNamePrefix() const override{
        return named_factory_.getNamePrefix();
    }

private:
    std::mutex mutex_;
    folly::NamedThreadFactory named_factory_;
};

template <typename SchedulerType>
struct SchedulerWrapper : public SchedulerType {

    using SchedulerType::SchedulerType;

    void set_active_threads(size_t n) {
        SchedulerType::activeThreads_.store(n);
    }

    void set_max_threads(size_t n) {
        SchedulerType::maxThreads_.store(n);
    }

    void set_thread_factory(std::shared_ptr<folly::ThreadFactory> factory) {
        SchedulerType::setThreadFactory(std::move(factory));
    }

    void ensure_active_threads() {
        SchedulerType::ensureActiveThreads();
    }

    void stop_orphaned_threads() {
#ifdef _WIN32
        const auto to_remove_copy = SchedulerType::threadList_.get();
        for (const auto& thread : to_remove_copy) {
            const bool is_signaled = WaitForSingleObject(thread->handle.native_handle(), 5000) == WAIT_OBJECT_0;
            if (is_signaled) {
                SchedulerType::threadList_.remove(thread);
            }
        }
#endif
    }
};

struct CGroupValues {
    std::optional<double> cpu_quota = std::nullopt;
    std::optional<double> cpu_period = std::nullopt;
};

inline std::optional<double> get_cgroup_value_v1(const std::string& cgroup_folder, const std::string& cgroup_file) {
    if(const auto path = std::filesystem::path{fmt::format("{}/{}", cgroup_folder, cgroup_file)}; std::filesystem::exists(path)){
        std::ifstream strm(path.string());
        util::check(static_cast<bool>(strm), "Failed to open cgroups v1 cpu file for read at path '{}': {}", path.string(), std::strerror(errno));
        std::string str;
        std::getline(strm, str);
        return std::stod(str);
    }
    return std::nullopt;
}

inline CGroupValues get_cgroup_values_v1(const std::string& cgroup_folder) {
    return CGroupValues{get_cgroup_value_v1(cgroup_folder, "cpu/cpu.cfs_quota_us"), get_cgroup_value_v1(cgroup_folder, "cpu/cpu.cfs_period_us")};
}

// In cgroup v2, the /sys/fs/cgroup/cpu.max file is used and the format is $MAX $PERIOD
// the default is max 100000
inline CGroupValues get_cgroup_values_v2(const std::string& cgroup_folder) {
    if(const auto path = std::filesystem::path{fmt::format("{}/cpu.max", cgroup_folder)}; std::filesystem::exists(path)){
        std::ifstream strm(path.string());
        util::check(static_cast<bool>(strm), "Failed to open cgroups v2 cpu file for read at path '{}': {}", path.string(), std::strerror(errno));
        std::string str;
        std::getline(strm, str);
        auto values = util::split_to_array<2>(str, ' ');

        auto quota = std::string{values[0]};
        auto period = std::string{values[1]};
        if (quota == std::string("max"))
            return CGroupValues{0, std::stod(period)};

        return CGroupValues{std::stod(quota), std::stod(period)};
    }

    return CGroupValues{};
}

inline auto get_default_num_cpus([[maybe_unused]] const std::string& cgroup_folder) {
    int64_t cpu_count = std::thread::hardware_concurrency() == 0 ? 16 : std::thread::hardware_concurrency();
    #ifdef _WIN32
        return static_cast<int64_t>(cpu_count);
    #else
        int64_t quota_count = 0UL;
        auto cgroup_val = get_cgroup_values_v1(cgroup_folder);

        // if cgroup v1 values are not found, try to get values from cgroup v2
        if (!cgroup_val.cpu_quota.has_value() || !cgroup_val.cpu_period.has_value())
            cgroup_val = get_cgroup_values_v2(cgroup_folder);

        if ((cgroup_val.cpu_quota.has_value() && cgroup_val.cpu_period.has_value()) &&
             (cgroup_val.cpu_quota.value() > -1 && cgroup_val.cpu_period.value() > 0))
            quota_count = static_cast<int64_t>(ceil(cgroup_val.cpu_quota.value() / cgroup_val.cpu_period.value()));

        int64_t limit_count = quota_count != 0 ? quota_count : cpu_count;
        return std::min(cpu_count, limit_count);
    #endif
}

/*
 * Possible areas of inprovement in the future:
 * 1/ Task/op decoupling: push task and then use strategy to implement smart batching to
 * amortize costs wherever possible
 * 2/ Worker thread Affinity - would better locality improve throughput by keeping hot structure in
 * hot cachelines and not jumping from one thread to the next (assuming thread/core affinity in hw too) ?
 * 3/ Priority: How to assign priorities to task in order to treat the most pressing first.
 * 4/ Throttling: (similar to priority) how to absorb work spikes and apply memory backpressure
 */
class TaskScheduler {
  public:
    using CPUSchedulerType = folly::FutureExecutor<folly::CPUThreadPoolExecutor>;
    using IOSchedulerType = folly::FutureExecutor<folly::IOThreadPoolExecutor>;

     explicit TaskScheduler(const std::optional<size_t>& cpu_thread_count = std::nullopt, const std::optional<size_t>& io_thread_count = std::nullopt) :
        cgroup_folder_("/sys/fs/cgroup"),
        cpu_thread_count_(cpu_thread_count ? *cpu_thread_count : ConfigsMap::instance()->get_int("VersionStore.NumCPUThreads", get_default_num_cpus(cgroup_folder_))),
        io_thread_count_(io_thread_count ? *io_thread_count : ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", (int) (cpu_thread_count_ * 1.5))),
        cpu_exec_(cpu_thread_count_, std::make_shared<InstrumentedNamedFactory>("CPUPool")) ,
        io_exec_(io_thread_count_,  std::make_shared<InstrumentedNamedFactory>("IOPool")){
        util::check(cpu_thread_count_ > 0 && io_thread_count_ > 0, "Zero IO or CPU threads: {} {}", io_thread_count_, cpu_thread_count_);
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "Task scheduler created with {:d} {:d}", cpu_thread_count_, io_thread_count_);
    }

    template<class Task>
    auto submit_cpu_task(Task &&t) {
        auto task = std::forward<decltype(t)>(t);
        static_assert(std::is_base_of_v<BaseTask, std::decay_t<Task>>, "Only supports Task derived from BaseTask");
        ARCTICDB_DEBUG(log::schedule(), "{} Submitting CPU task {}: {} of {}", uintptr_t(this), typeid(task).name(), cpu_exec_.getTaskQueueSize(), cpu_exec_.kDefaultMaxQueueSize);
        std::lock_guard lock{cpu_mutex_};
        return cpu_exec_.addFuture(std::move(task));
    }

    template<class Task>
    auto submit_io_task(Task &&t) {
        auto task = std::forward<decltype(t)>(t);
        static_assert(std::is_base_of_v<BaseTask, std::decay_t<Task>>, "Only support Tasks derived from BaseTask");
        ARCTICDB_DEBUG(log::schedule(), "{} Submitting IO task {}: {}", uintptr_t(this), typeid(task).name(), io_exec_.getPendingTaskCount());
        std::lock_guard lock{io_mutex_};
        return io_exec_.addFuture(std::move(task));
    }

    static std::shared_ptr<TaskSchedulerPtrWrapper> instance_;
    static std::once_flag init_flag_;
    static std::once_flag shutdown_flag_;

    static void init();

    static TaskScheduler* instance();
    static void reattach_instance();
    static void stop_active_threads();
    static bool forked_;
    static bool is_forked();
    static void set_forked(bool);

    void join() {
        ARCTICDB_DEBUG(log::schedule(), "Joining task scheduler");
        io_exec_.join();
        cpu_exec_.join();
    }

    void stop() {
        ARCTICDB_DEBUG(log::schedule(), "Stopping task scheduler");
        io_exec_.stop();
        cpu_exec_.stop();
    }

    void set_active_threads(size_t n) {
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "Setting CPU and IO thread pools to {} active threads", n);
        cpu_exec_.set_active_threads(n);
        io_exec_.set_active_threads(n);
    }

    void set_max_threads(size_t n) {
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "Setting CPU and IO thread pools to {} max threads", n);
        cpu_exec_.set_max_threads(n);
        io_exec_.set_max_threads(n);
    }

    SchedulerWrapper<CPUSchedulerType>& cpu_exec() {
        ARCTICDB_TRACE(log::schedule(), "Getting CPU executor: {}", cpu_exec_.getTaskQueueSize());
        return cpu_exec_;
    }

    SchedulerWrapper<IOSchedulerType>& io_exec() {
        ARCTICDB_TRACE(log::schedule(), "Getting IO executor: {}", io_exec_.getPendingTaskCount());
        return io_exec_;
    }

    void re_init() {
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "Reinitializing task scheduler: {} {}", cpu_thread_count_, io_thread_count_);
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "IO exec num threads: {}", io_exec_.numActiveThreads());
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "CPU exec num threads: {}", cpu_exec_.numActiveThreads());
        set_active_threads(0);
        set_max_threads(0);
        io_exec_.set_thread_factory(std::make_shared<InstrumentedNamedFactory>("IOPool"));
        cpu_exec_.set_thread_factory(std::make_shared<InstrumentedNamedFactory>("CPUPool"));
        io_exec_.setNumThreads(io_thread_count_);
        cpu_exec_.setNumThreads(cpu_thread_count_);
    }

    size_t cpu_thread_count() const {
        return cpu_thread_count_;
    }

    size_t io_thread_count() const {
        return io_thread_count_;
    }

    void stop_orphaned_threads() {
        io_exec_.stop_orphaned_threads();
        cpu_exec_.stop_orphaned_threads();
    }

private:
    std::string cgroup_folder_;
    size_t cpu_thread_count_;
    size_t io_thread_count_;
    SchedulerWrapper<CPUSchedulerType> cpu_exec_;
    SchedulerWrapper<IOSchedulerType> io_exec_;
    std::mutex cpu_mutex_;
    std::mutex io_mutex_;
};


inline auto& cpu_executor() {
    return TaskScheduler::instance()->cpu_exec();
}

inline auto& io_executor() {
    return TaskScheduler::instance()->io_exec();
}

template <typename Task>
auto submit_cpu_task(Task&& task) {
    return TaskScheduler::instance()->submit_cpu_task(std::forward<decltype(task)>(task));
}


template <typename Task>
auto submit_io_task(Task&& task) {
    return TaskScheduler::instance()->submit_io_task(std::forward<decltype(task)>(task));
}

void print_scheduler_stats();

}
