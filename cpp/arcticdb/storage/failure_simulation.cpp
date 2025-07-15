/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/failure_simulation.hpp>

namespace arcticdb {
std::shared_ptr<StorageFailureSimulator> StorageFailureSimulator::instance(){
    std::call_once(StorageFailureSimulator::init_flag_, &StorageFailureSimulator::reset);
    return StorageFailureSimulator::instance_;
}

std::shared_ptr<StorageFailureSimulator> StorageFailureSimulator::instance_;
std::once_flag StorageFailureSimulator::init_flag_;
}