/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct NumpyBufferHolder {
    TypeDescriptor type_{make_scalar_type(DataType::UNKNOWN)};
    uint8_t* ptr_{nullptr};
    size_t row_count_{0};

    NumpyBufferHolder(TypeDescriptor type, uint8_t* ptr, size_t row_count):
    type_(type),
    ptr_(ptr),
    row_count_(row_count){
    }

    explicit NumpyBufferHolder(NumpyBufferHolder&& other) {
        type_ = other.type_;
        ptr_ = other.ptr_;
        row_count_ = other.row_count_;
        other.type_ = make_scalar_type(DataType::UNKNOWN);
        other.ptr_ = nullptr;
        other.row_count_ = 0;
    }

    ~NumpyBufferHolder() {
        if (ptr_) {
            if (is_dynamic_string_type(type_.data_type())) {
                if (is_arrow_output_only_type(type_)) {
                    log::version().error("Unexpected arrow output format seen in NumpyBufferHolder");
                } else {
                    // pybind11 has taken the GIL for us so this is safe
                    auto py_ptr = reinterpret_cast<PyObject**>(ptr_);
                    for (size_t idx = 0; idx < row_count_; ++idx, ++py_ptr) {
                        if (*py_ptr != nullptr) {
                            Py_DECREF(*py_ptr);
                        } else {
                            log::version().error("Unexpected nullptr to DecRef in NumpyBufferHolder destructor");
                        }
                    }
                }
            }
            delete[] ptr_;
        }
    }
};

} //namespace arcticdb