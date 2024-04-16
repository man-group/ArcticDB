/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <vector>

namespace arcticdb {
struct BufferHolder {
    std::vector<std::shared_ptr<Column>> columns_;
    std::mutex mutex_;

    std::shared_ptr<Column> get_buffer(const TypeDescriptor& td, bool allow_sparse) {
        std::lock_guard lock(mutex_);
        auto column = std::make_shared<Column>(td, allow_sparse);
        columns_.emplace_back(column);
        return column;
    }
};
}
