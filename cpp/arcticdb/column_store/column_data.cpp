/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include "column_data.hpp"

namespace arcticdb {
    bool ColumnData::current_tensor_is_empty() const {
        return shape_pos_ < shapes_->bytes() && *shapes_->ptr_cast<shape_t>(shape_pos_, sizeof(shape_t)) == 0;
    }

    const Buffer* ColumnData::shapes() const noexcept {
        return shapes_;
    }
}
