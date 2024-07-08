/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/python/python_strings.hpp>

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/frame_slice_map.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/util/buffer_holder.hpp>

namespace arcticdb {

EmptyDynamicStringReducer::EmptyDynamicStringReducer(
    Column &column,
    SegmentInMemory frame,
    const entity::Field &,
    size_t alloc_width,
    std::shared_ptr<SpinLock> spinlock) :
    column_(column),
    frame_(std::move(frame)),
    row_(0),
    src_buffer_(column.data().buffer()),
    column_width_(alloc_width),
    dest_buffer_(ChunkedBuffer::presized(frame_.row_count() * column_width_)),
    dst_(dest_buffer_.data()),
    ptr_dest_(reinterpret_cast<PyObject **>(dst_)),
    lock_(std::move(spinlock)),
    py_nan_(create_py_nan(lock_), [lock = lock_](PyObject *py_obj) {
        lock->lock();
        Py_DECREF(py_obj);
        lock->unlock();
    }) {
}

EmptyDynamicStringReducer::~EmptyDynamicStringReducer() {
    src_buffer_ = std::move(dest_buffer_);

}

void EmptyDynamicStringReducer::reduce(size_t end) {
    auto none = py::none{};
    auto ptr_src = get_offset_ptr_at(row_, src_buffer_);
    auto non_counter = 0u;
    for (; row_ < end; ++row_, ++ptr_src, ++ptr_dest_) {
        auto offset = *ptr_src;
        if (offset == not_a_string()) {
            ++non_counter;
            *ptr_dest_ = none.ptr();
        } else if (offset == nan_placeholder()) {
            *ptr_dest_ = py_nan_.get();
            Py_INCREF(py_nan_.get());
        } else {
            util::raise_rte("Got unexpected offset in default initialization column");
        }
    }

    if (non_counter != 0u) {
        lock_->lock();
        for (auto j = 0u; j < non_counter; ++j)
            none.inc_ref();
        lock_->unlock();
    }
}

namespace {

enum class PyStringConstructor {
    Unicode_FromUnicode,
    Unicode_FromStringAndSize,
    Bytes_FromStringAndSize
};

inline PyStringConstructor get_string_constructor(bool has_type_conversion, bool is_utf) {
    if (is_utf) {
        if (has_type_conversion) {
            return PyStringConstructor::Unicode_FromUnicode;

        } else {
            return PyStringConstructor::Unicode_FromStringAndSize;

        }
    } else {
        return PyStringConstructor::Bytes_FromStringAndSize;
    }
}
} //namespace

inline void DynamicStringReducer::process_string_views(
    bool has_type_conversion,
    bool is_utf,
    size_t end,
    const entity::position_t *ptr_src,
    const StringPool &string_pool) {
    auto string_constructor = get_string_constructor(has_type_conversion, is_utf);

    switch (string_constructor) {
    case PyStringConstructor::Unicode_FromUnicode:
        if (shared_data_.unique_string_map())
            assign_strings_shared<UnicodeFromUnicodeCreator>(end, ptr_src, has_type_conversion, string_pool);
        else
            assign_strings_local<UnicodeFromUnicodeCreator>(end, ptr_src, has_type_conversion, string_pool);
        break;
    case PyStringConstructor::Unicode_FromStringAndSize:
        if (shared_data_.unique_string_map())
            assign_strings_shared<UnicodeFromStringAndSizeCreator>(end, ptr_src, has_type_conversion, string_pool);
        else
            assign_strings_local<UnicodeFromStringAndSizeCreator>(end, ptr_src, has_type_conversion, string_pool);
        break;
    case PyStringConstructor::Bytes_FromStringAndSize:
        if (shared_data_.unique_string_map())
            assign_strings_shared<BytesFromStringAndSizeCreator>(end, ptr_src, has_type_conversion, string_pool);
        else
            assign_strings_local<BytesFromStringAndSizeCreator>(end, ptr_src, has_type_conversion, string_pool);
        break;
    }
}

DynamicStringReducer::DynamicStringReducer(
    DecodePathData shared_data,
    PyObject** ptr_dest,
    size_t total_rows) :
        shared_data_(std::move(shared_data)),
        ptr_dest_(ptr_dest),
    py_nan_(std::shared_ptr<PyObject>(create_py_nan(shared_data_.spin_lock()), [spinlock=shared_data_.spin_lock()](PyObject *py_obj) {
        spinlock->lock();
        Py_DECREF(py_obj);
        spinlock->unlock();
    })),

    total_rows_(total_rows) {
    util::check(static_cast<bool>(py_nan_), "Got null nan in string reducer");
}

void DynamicStringReducer::reduce(TypeDescriptor source_type,
                                  TypeDescriptor target_type,
                                  size_t num_rows,
                                  const StringPool &string_pool,
                                  const position_t *ptr_src) {

    const bool trivially_compatible = trivially_compatible_types(source_type, target_type);
    // In case the segment type is EMPTYVAL the empty type handler should have run and set all missing values
    // to not_a_string()
    util::check(trivially_compatible || is_empty_type(target_type.data_type()),
                "String types are not trivially compatible. Cannot convert from type {} to {} in frame field.",
                source_type,
                target_type
    );

    auto is_utf = is_utf_type(slice_value_type(source_type.data_type()));

    const auto is_type_different = source_type != target_type;
    process_string_views(is_type_different, is_utf, num_rows, ptr_src, string_pool);
}

void DynamicStringReducer::finalize() {
    if (row_ != total_rows_) {
        auto none = py::none{};
        const auto diff = total_rows_ - row_;
        for (; row_ < total_rows_; ++row_, ++ptr_dest_) {
            *ptr_dest_ = none.ptr();
        }

        lock().lock();
        for (auto i = 0u; i < diff; ++i) {
            none.inc_ref();
        }
        lock().unlock();
    }
}

} // namespace arcticdb