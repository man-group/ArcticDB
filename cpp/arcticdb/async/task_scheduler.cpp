/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/task_scheduler.hpp>

namespace arcticdb::async {

TaskScheduler* TaskScheduler::instance() {
    std::call_once(TaskScheduler::init_flag_, &TaskScheduler::init);
    return instance_->ptr_;
}

bool TaskScheduler::is_initialized() { return static_cast<bool>(instance_); }

std::shared_ptr<TaskSchedulerPtrWrapper> TaskScheduler::instance_;
std::once_flag TaskScheduler::init_flag_;
std::once_flag TaskScheduler::shutdown_flag_;
bool TaskScheduler::forked_ = false;

void TaskScheduler::stop_active_threads() {
    std::call_once(shutdown_flag_, [] {
        if (instance_) {
            instance()->stop();
        }
    });
}

void TaskScheduler::reattach_instance() {
    if (TaskScheduler::instance_) {
        ARCTICDB_DEBUG(
                log::schedule(),
                "Leaking and reattaching task scheduler instance, currently {}",
                uintptr_t(TaskScheduler::instance_->ptr_)
        );
        TaskScheduler::instance_->ptr_ = new TaskScheduler;
        ARCTICDB_DEBUG(
                log::schedule(),
                "Attached new task scheduler instance, now {}",
                uintptr_t(TaskScheduler::instance_->ptr_)
        );
    }
}

bool TaskScheduler::is_forked() { return TaskScheduler::forked_; }

void TaskScheduler::set_forked(bool val) { TaskScheduler::forked_ = val; }

void TaskScheduler::init() {
    TaskScheduler::instance_ = std::make_shared<TaskSchedulerPtrWrapper>(new TaskScheduler);
    query_stats::set_pool_gauge_provider([](query_stats::PoolGaugeSnapshot& cpu, query_stats::PoolGaugeSnapshot& io) {
        if (!TaskScheduler::is_initialized() || TaskScheduler::is_forked() || !TaskScheduler::instance_->ptr_) {
            return;
        }
        auto* ts = TaskScheduler::instance_->ptr_;
        cpu.pending_tasks = static_cast<uint64_t>(ts->cpu_exec().getTaskQueueSize());
        cpu.active_threads = static_cast<uint64_t>(ts->cpu_exec().numActiveThreads());
        cpu.total_threads = static_cast<uint64_t>(ts->cpu_exec().numThreads());
        cpu.max_threads = static_cast<uint64_t>(ts->cpu_thread_count());
        io.pending_tasks = static_cast<uint64_t>(ts->io_exec().getPendingTaskCount());
        io.max_threads = static_cast<uint64_t>(ts->io_thread_count());
        io.active_threads = std::min(ts->io_exec().active_task_count(), io.max_threads);
        io.total_threads = static_cast<uint64_t>(ts->io_exec().numThreads());
    });
}

TaskSchedulerPtrWrapper::~TaskSchedulerPtrWrapper() {
    query_stats::set_pool_gauge_provider(nullptr);
    ptr_->stop_orphaned_threads();
    delete ptr_;
}

void print_scheduler_stats() {
    auto cpu_stats = TaskScheduler::instance()->cpu_exec().getPoolStats();
    log::schedule().info(
            "CPU: Threads: {}\tIdle: {}\tActive: {}\tPending: {}\tTotal: {}\tMaxIdleTime: {}",
            cpu_stats.threadCount,
            cpu_stats.idleThreadCount,
            cpu_stats.activeThreadCount,
            cpu_stats.pendingTaskCount,
            cpu_stats.totalTaskCount,
            cpu_stats.maxIdleTime.count()
    );

    auto io_stats = TaskScheduler::instance()->io_exec().getPoolStats();
    log::schedule().info(
            "IO: Threads: {}\tIdle: {}\tActive: {}\tPending: {}\tTotal: {}\tMaxIdleTime: {}",
            io_stats.threadCount,
            io_stats.idleThreadCount,
            io_stats.activeThreadCount,
            io_stats.pendingTaskCount,
            io_stats.totalTaskCount,
            io_stats.maxIdleTime.count()
    );
}

} // namespace arcticdb::async
