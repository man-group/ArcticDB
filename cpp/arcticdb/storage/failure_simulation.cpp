/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/failure_simulation.hpp>

namespace arcticdb {
std::shared_ptr<StorageFailureSimulator> StorageFailureSimulator::instance(){
    std::call_once(StorageFailureSimulator::init_flag_, &StorageFailureSimulator::init);
    return StorageFailureSimulator::instance_;
}

std::shared_ptr<StorageFailureSimulator> StorageFailureSimulator::instance_;
std::once_flag StorageFailureSimulator::init_flag_;
}