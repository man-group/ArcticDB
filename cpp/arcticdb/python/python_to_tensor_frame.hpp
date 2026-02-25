/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <string>

namespace arcticdb::convert {

namespace py = pybind11;
using namespace arcticdb::entity;

// py::tuple for Pandas data, record batches for Arrow data
// Use shared_ptr for RecordBatchData since it has a deleted copy constructor
// and pybind11 requires copyable types in std::variant
using InputItem = std::variant<py::tuple, std::vector<std::shared_ptr<RecordBatchData>>>;

struct ARCTICDB_VISIBILITY_HIDDEN PyStringWrapper {
    char* buffer_;
    size_t length_;
    PyObject* obj_;

    // If the underlying Python string is ASCII or UTF-8, we can use the input object's underlying buffer, and obj will
    // be nullptr in this ctor. For unicode, the Python C API method is used to construct a new Python object which must
    // be DECREFFed on destruction to free the underlying memory.
    PyStringWrapper(char* buf, ssize_t len, PyObject* obj = nullptr) : buffer_(buf), length_(size_t(len)), obj_(obj) {}

    ARCTICDB_NO_COPY(PyStringWrapper)

    PyStringWrapper(PyStringWrapper&& other) : buffer_(other.buffer_), length_(other.length_), obj_(other.obj_) {
        other.obj_ = nullptr;
    }

    PyStringWrapper& operator=(PyStringWrapper&& other) {
        buffer_ = other.buffer_;
        length_ = other.length_;
        obj_ = other.obj_;
        other.obj_ = nullptr;
        return *this;
    }

    ~PyStringWrapper() {
        if (obj_)
            Py_DECREF(obj_);
    }
};

struct ARCTICDB_VISIBILITY_HIDDEN StringEncodingError {
    StringEncodingError() = default;
    explicit StringEncodingError(std::string_view error_message) : error_message_(error_message) {}

    void raise(std::string_view column_name, size_t offset_in_frame = 0) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "String encoding failed in column '{}', row {}, error '{}'",
                column_name,
                row_index_in_slice_ + offset_in_frame,
                error_message_
        );
    }

    size_t row_index_in_slice_ = 0UL;
    std::string error_message_;
};

std::variant<StringEncodingError, PyStringWrapper> pystring_to_buffer(PyObject* obj, bool is_owned);

std::variant<StringEncodingError, PyStringWrapper> py_unicode_to_buffer(
        PyObject* obj, std::optional<ScopedGILLock>& scoped_gil_lock
);

NativeTensor obj_to_tensor(PyObject* ptr, bool empty_types, std::optional<std::string_view> column_name = std::nullopt);

std::shared_ptr<pipelines::InputFrame> py_ndf_to_frame(
        const StreamId& stream_name, const InputItem& item, const py::object& norm_meta, const py::object& user_meta,
        bool empty_types
);

std::shared_ptr<pipelines::InputFrame> py_none_to_frame();

} // namespace arcticdb::convert
