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

struct SliceDataSink {
    // Just an interface for Column / ...
    SliceDataSink(uint8_t* data, std::size_t size) : data_(data), shape_(0), current_size_(0), total_size_(size) {}

    shape_t* allocate_shapes(std::size_t s) ARCTICDB_UNUSED {
        if (s == 0)
            return nullptr;
        util::check_arg(s == 8, "expected exactly one shape, actual {}", s / sizeof(shape_t));
        return &shape_;
    }

    uint8_t* allocate_data(std::size_t size) ARCTICDB_UNUSED {
        util::check_arg(
                current_size_ + size <= total_size_,
                "Data sink overflow trying to allocate {} bytes in a buffer of {} with {} remaining",
                size,
                total_size_,
                total_size_ - current_size_
        );

        return data_ + current_size_;
    }

    void advance_data(std::size_t) ARCTICDB_UNUSED {}

    void advance_shapes(std::size_t) ARCTICDB_UNUSED {}

    void set_allow_sparse(Sparsity) ARCTICDB_UNUSED {}

  private:
    uint8_t* data_;
    shape_t shape_;
    std::size_t current_size_;
    std::size_t total_size_;
};
} // namespace arcticdb