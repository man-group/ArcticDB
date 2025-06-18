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
    size_t bytes_{0};

    NumpyBufferHolder(TypeDescriptor type, uint8_t* ptr, size_t bytes):
    type_(type),
    ptr_(ptr),
    bytes_(bytes){
        util::check(bytes_ % get_type_size(type_.data_type()) == 0,
                    "Buffer of size {} not a multiple of type {} size {}", bytes_, type_, get_type_size(type_.data_type()));
    }

    explicit NumpyBufferHolder(NumpyBufferHolder&& other) {
        type_ = other.type_;
        ptr_ = other.ptr_;
        bytes_ = other.bytes_;
        other.type_ = make_scalar_type(DataType::UNKNOWN);
        other.ptr_ = nullptr;
        other.bytes_ = 0;
    }

    ~NumpyBufferHolder() {
        if (ptr_) {
            if (is_dynamic_string_type(type_.data_type())) {
                if (is_object_type(type_)) {
                    if (is_array_type(type_) || !is_arrow_output_only_type(type_)) {
                        auto py_ptr = reinterpret_cast<PyObject**>(ptr_);
                        auto num_values = bytes_ / get_type_size(type_.data_type());
                        for (size_t idx = 0; idx < num_values; ++idx, ++py_ptr) {
                            if (*py_ptr != nullptr) {
                                Py_DECREF(*py_ptr);
                            } else {
                                log::version().error("Unexpected nullptr to DecRef in PandasOutputFrame destructor");
                            }
                        }
                    } else {
                        log::version().error("Unexpected arrow output format seen in PandasOutputFrame");
                    }
                }
            }
            delete[] ptr_;
        }
    }
};

} //namespace arcticdb