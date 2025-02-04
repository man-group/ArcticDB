/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/task_scheduler.hpp>

namespace arcticdb::async {

TaskScheduler* TaskScheduler::instance() {
    std::call_once(TaskScheduler::init_flag_, &TaskScheduler::init);
    return instance_->ptr_;
}

std::shared_ptr<TaskSchedulerPtrWrapper> TaskScheduler::instance_;
std::once_flag TaskScheduler::init_flag_;
std::once_flag TaskScheduler::shutdown_flag_;
bool TaskScheduler::forked_ = false;

void TaskScheduler::destroy_instance() {
    std::call_once(TaskScheduler::shutdown_flag_, &TaskScheduler::stop_and_destroy);
}

void TaskScheduler::stop_and_destroy() {
    if(TaskScheduler::instance_) {
        TaskScheduler::instance()->stop();

        TaskScheduler::instance_.reset();
    }
}

void TaskScheduler::reattach_instance() {
    if (TaskScheduler::instance_) {
        ARCTICDB_DEBUG(log::schedule(), "Leaking and reattaching task scheduler instance, currently {}",
                              uintptr_t(TaskScheduler::instance_->ptr_));
        TaskScheduler::instance_->ptr_ = new TaskScheduler;
        ARCTICDB_DEBUG(log::schedule(), "Attached new task scheduler instance, now {}",
                              uintptr_t(TaskScheduler::instance_->ptr_));
    }
}

bool TaskScheduler::is_forked() {
    return TaskScheduler::forked_;
}

void TaskScheduler::set_forked(bool val) {
    TaskScheduler::forked_ = val;
}

void TaskScheduler::init(){
    TaskScheduler::instance_ = std::make_shared<TaskSchedulerPtrWrapper>(new TaskScheduler);
}

TaskSchedulerPtrWrapper::~TaskSchedulerPtrWrapper() {
    delete ptr_;
}

void print_scheduler_stats(spdlog::level::level_enum level) {
    auto cpu_stats = TaskScheduler::instance()->cpu_exec().getPoolStats();
    log::schedule().log(level, "CPU: Threads: {}\tIdle: {}\tActive: {}\tPending: {}\tTotal: {}\tMaxIdleTime: {}",
        cpu_stats.threadCount, cpu_stats.idleThreadCount, cpu_stats.activeThreadCount, cpu_stats.pendingTaskCount, cpu_stats.totalTaskCount, cpu_stats.maxIdleTime.count());

    auto io_stats = TaskScheduler::instance()->io_exec().getPoolStats();
    log::schedule().log(level, "IO: Threads: {}\tIdle: {}\tActive: {}\tPending: {}\tTotal: {}\tMaxIdleTime: {}",
        io_stats.threadCount, io_stats.idleThreadCount, io_stats.activeThreadCount, io_stats.pendingTaskCount, io_stats.totalTaskCount, io_stats.maxIdleTime.count());

    auto blocking_cpu_stats = TaskScheduler::instance()->blocking_cpu_exec().getPoolStats();
    auto blocking_queue_size = TaskScheduler::instance()->blocking_cpu_exec().getTaskQueueSize();
    log::schedule().log(level, "Blocking CPU: Threads: {}\tIdle: {}\tActive: {}\tPending: {}\tTotal: {}\tMaxIdleTime: {}\tSize: {}",
                         blocking_cpu_stats.threadCount, blocking_cpu_stats.idleThreadCount, blocking_cpu_stats.activeThreadCount, blocking_cpu_stats.pendingTaskCount, blocking_cpu_stats.totalTaskCount, blocking_cpu_stats.maxIdleTime.count(), blocking_queue_size);
}

} // namespace arcticdb


