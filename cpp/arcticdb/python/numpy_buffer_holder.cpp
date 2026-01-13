/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/python/numpy_buffer_holder.hpp>
#include <arcticdb/util/allocator.hpp>
#include <pybind11/pybind11.h>

namespace arcticdb {
NumpyBufferHolder::NumpyBufferHolder(
        entity::TypeDescriptor type, uint8_t* ptr, size_t row_count, size_t allocated_bytes
) :
    type_(type),
    ptr_(ptr),
    row_count_(row_count),
    allocated_bytes_(allocated_bytes) {}

NumpyBufferHolder::NumpyBufferHolder(NumpyBufferHolder&& other) :
    type_(other.type_),
    ptr_(other.ptr_),
    row_count_(other.row_count_),
    allocated_bytes_(other.allocated_bytes_) {
    other.type_ = make_scalar_type(entity::DataType::UNKNOWN);
    other.ptr_ = nullptr;
    other.row_count_ = 0;
    other.allocated_bytes_ = 0;
}

NumpyBufferHolder::~NumpyBufferHolder() {
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
        // See comment on allocate_detachable_memory declaration for why this cannot just be a call to delete[]
        free_detachable_memory(ptr_, allocated_bytes_);
    }
}
} // namespace arcticdb
