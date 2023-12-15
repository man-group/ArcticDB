#pragma once

#include <arcticdb/arrow/arrow_array.hpp>
#include <memory>

namespace arcticdb {
class ArrayView;
}

namespace arcticdb::arrow {

class BufferView {
private:
    std::shared_ptr<const ArrayView> parent_;
    ArrowBufferView* ptr_;
    ArrowType buffer_data_type_;
    ssize_t element_size_bits_;
    ssize_t strides_;
    ssize_t shape_;

public:
    BufferView(
        std::shared_ptr<const ArrayView> parent, 
        uintptr_t addr, 
        ArrowType buffer_data_type,
        ssize_t element_size_bits) :
            parent_(std::move(parent)),
            ptr_(reinterpret_cast<ArrowBufferView*>(addr)),
            buffer_data_type_(buffer_data_type),
            element_size_bits_(element_size_bits),
            strides_(item_size()),
            shape_(ptr_->size_bytes / strides_) {
    }

    const uint8_t* ptr() const {
        return reinterpret_cast<const uint8_t*>(ptr_->data.data);
    }

    ssize_t strides() const {
        return strides_;
    }

    ssize_t shapes() const {
        return shape_;
    }

    ssize_t item_size() {
        if (buffer_data_type_ == NANOARROW_TYPE_BOOL || buffer_data_type_ == NANOARROW_TYPE_STRING || buffer_data_type_ == NANOARROW_TYPE_BINARY) {
            return 1;
        } else {
            return element_size_bits_ / 8;
        }
    }

    const char* get_format() {
        if (buffer_data_type_ == NANOARROW_TYPE_INT8) {
            return "b";
        } else if (buffer_data_type_ == NANOARROW_TYPE_UINT8) {
            return "B";
        } else if (buffer_data_type_ == NANOARROW_TYPE_INT16) {
            return "h";
        } else if (buffer_data_type_ == NANOARROW_TYPE_UINT16) {
            return "H";
        } else if (buffer_data_type_ == NANOARROW_TYPE_INT32) {
            return "i";
        } else if (buffer_data_type_ == NANOARROW_TYPE_UINT32) {
            return "I";
        } else if (buffer_data_type_ == NANOARROW_TYPE_INT64) {
            return "l";
        } else if (buffer_data_type_ == NANOARROW_TYPE_UINT64) {
            return "L";
        } else if (buffer_data_type_ == NANOARROW_TYPE_FLOAT) {
            return "f";
        } else if (buffer_data_type_ == NANOARROW_TYPE_DOUBLE) {
            return "d";
        } else if (buffer_data_type_ == NANOARROW_TYPE_STRING) {
            return "c";
        } else {
            return "B";
        }
    }
};
}