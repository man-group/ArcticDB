/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {
struct BatchReadArgs {
    BatchReadArgs() = default;

    explicit BatchReadArgs(size_t batch_size) : batch_size_(batch_size) {}

    size_t batch_size_ = ConfigsMap::instance()->get_int("BatchRead.BatchSize", 200);
};
} // namespace arcticdb