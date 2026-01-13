/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct NumpyBufferHolder {
    entity::TypeDescriptor type_{make_scalar_type(entity::DataType::UNKNOWN)};
    uint8_t* ptr_{nullptr};
    size_t row_count_{0};
    size_t allocated_bytes_{0};

    NumpyBufferHolder(entity::TypeDescriptor type, uint8_t* ptr, size_t row_count, size_t allocated_bytes);
    explicit NumpyBufferHolder(NumpyBufferHolder&& other);
    ~NumpyBufferHolder();
};

} // namespace arcticdb