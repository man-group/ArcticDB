/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/exponential_backoff.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/home_directory.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/entity/performance_tracing.hpp>

#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <thread>
#include <algorithm>
#include <filesystem>
#include <string>
#include <fstream>
#include <fmt/format.h>
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
};

inline int64_t get_cgroup_value(const std::string& cgroup_file) {
    if(const auto path = std::filesystem::path{fmt::format("/sys/fs/cgroup/{}",cgroup_file)}; std::filesystem::exists(path)){
        std::ifstream strm(path.string());
        util::check(static_cast<bool>(strm), "Failed to open cgroups cpu file for read at path '{}': {}", path.string(), std::strerror(errno));
        std::string str;
        std::getline(strm, str);
        return std::stol(str);
    }
    return static_cast<int64_t>(-1);
}

inline auto get_default_num_cpus() {
    auto cpu_count = std::thread::hardware_concurrency() == 0 ? 16 : std::thread::hardware_concurrency();
    #ifdef _WIN32
        return static_cast<int64_t>(cpu_count);
    #else
        auto quota_count = 0UL;
        auto quota = get_cgroup_value("cpu/cpu.cfs_quota_us");
        auto period = get_cgroup_value("cpu/cpu.cfs_period_us");

        if (quota > -1 && period > 0)
            quota_count = static_cast<int64_t>(ceil(static_cast<double>(quota) / static_cast<double>(period)));

        auto limit_count = quota_count != 0 ? quota_count : cpu_count;
        return std::min(static_cast<int64_t>(cpu_count), static_cast<int64_t>(limit_count));
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

inline int64_t default_io_thread_count(uint64_t cpu_count) {
    return std::min(int64_t(100L), static_cast<int64_t>(static_cast<double>(cpu_count) * 1.5));
}

class TaskScheduler {
  public:
    using CPUSchedulerType = folly::FutureExecutor<folly::CPUThreadPoolExecutor>;
    using IOSchedulerType = folly::FutureExecutor<folly::IOThreadPoolExecutor>;

     explicit TaskScheduler(const std::optional<size_t>& cpu_thread_count = std::nullopt, const std::optional<size_t>& io_thread_count = std::nullopt) :
        cpu_thread_count_(cpu_thread_count ? *cpu_thread_count : ConfigsMap::instance()->get_int("VersionStore.NumCPUThreads", get_default_num_cpus())),
        io_thread_count_(io_thread_count ? *io_thread_count : ConfigsMap::instance()->get_int("VersionStore.NumIOThreads", std::min(100, (int) (cpu_thread_count_ * 1.5)))),
        cpu_exec_(cpu_thread_count_, std::make_shared<InstrumentedNamedFactory>("CPUPool")) ,
        io_exec_(io_thread_count_,  std::make_shared<InstrumentedNamedFactory>("IOPool")){
        util::check(cpu_thread_count_ > 0 && io_thread_count_ > 0, "Zero IO or CPU threads: {} {}", io_thread_count_, cpu_thread_count_);
        ARCTICDB_RUNTIME_DEBUG(log::schedule(), "Task scheduler created with {:d} {:d}", cpu_thread_count_, io_thread_count_);
    }

    ~TaskScheduler() = default;

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
    static void destroy_instance();
    static void stop_and_destroy();
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
        cpu_exec_.stop();
        io_exec_.stop();
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
        ARCTICDB_DEBUG(log::schedule(), "Getting CPU executor: {}", cpu_exec_.getTaskQueueSize());
        return cpu_exec_;
    }

    SchedulerWrapper<IOSchedulerType>& io_exec() {
        ARCTICDB_DEBUG(log::schedule(), "Getting IO executor: {}", io_exec_.getPendingTaskCount());
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

private:
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
inline auto submit_cpu_task(Task&& task) {
    return TaskScheduler::instance()->submit_cpu_task(std::forward<decltype(task)>(task));
}


template <typename Task>
inline auto submit_io_task(Task&& task) {
    return TaskScheduler::instance()->submit_io_task(std::forward<decltype(task)>(task));
}

void print_scheduler_stats();

}
