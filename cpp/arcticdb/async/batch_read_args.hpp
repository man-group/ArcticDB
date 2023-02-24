/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {
struct BatchReadArgs {
    // The below enum controls where (IO or CPU thread pool) decoding and data processing tasks are executed.
    // If IO is used, all work will be done in the IO thread pool which may prevent context switches - this is therefore
    // suitable for work that is CPU light.
    enum Scheduler {
        IO,
        CPU
    };

    BatchReadArgs() :
        batch_size_(ConfigsMap::instance()->get_int("BatchRead.BatchSize", 100)),
        scheduler_(Scheduler::CPU) {}

    explicit BatchReadArgs(size_t batch_size) :
        batch_size_(batch_size),
        scheduler_(Scheduler::CPU) {}

    explicit BatchReadArgs(Scheduler scheduler) :
        batch_size_(ConfigsMap::instance()->get_int("BatchRead.BatchSize", 100)),
        scheduler_(scheduler) { }

    size_t batch_size_;
    Scheduler scheduler_;
};
}