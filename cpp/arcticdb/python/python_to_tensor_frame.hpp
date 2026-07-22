/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/arrow/arrow_c_interface.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <pybind11/numpy.h>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace arcticdb::convert {

namespace py = pybind11;
using namespace arcticdb::entity;

using PandasColumn = std::variant<py::array, std::vector<std::shared_ptr<RecordBatchData>>>;

struct ARCTICDB_VISIBILITY_HIDDEN PandasData {
    PandasData(
            std::vector<std::string>&& index_names, std::vector<std::string>&& column_names,
            std::vector<PandasColumn>&& index_values, std::vector<PandasColumn>&& columns_values, SortedValue sorted
    ) :
        index_names(std::move(index_names)),
        column_names(std::move(column_names)),
        index_values(std::move(index_values)),
        columns_values(std::move(columns_values)),
        sorted(sorted) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(PandasData)

    std::vector<std::string> index_names;
    std::vector<std::string> column_names;
    std::vector<PandasColumn> index_values;
    std::vector<PandasColumn> columns_values;
    SortedValue sorted{SortedValue::UNKNOWN};
};

// PandasData for Pandas data, record batches for Arrow data
using InputItem = std::variant<std::shared_ptr<PandasData>, std::vector<std::shared_ptr<RecordBatchData>>>;

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

    [[noreturn]] void raise(std::string_view column_name, size_t offset_in_frame = 0) {
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

void record_batches_to_frame(
        const std::vector<std::shared_ptr<RecordBatchData>>& record_batches, pipelines::InputFrame& frame,
        pipelines::SortednessScan sortedness_scan
);

std::shared_ptr<pipelines::InputFrame> py_input_item_to_frame(
        const StreamId& stream_name, const InputItem& item, const py::object& norm_meta, const py::object& user_meta,
        bool empty_types, pipelines::SortednessScan sortedness_scan
);

std::shared_ptr<pipelines::InputFrame> py_none_to_frame();

} // namespace arcticdb::convert
