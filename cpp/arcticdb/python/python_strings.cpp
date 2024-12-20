/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/python/python_strings.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/frame_slice_map.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/util/buffer_holder.hpp>
#include <arcticdb/python/python_handlers.hpp>

namespace arcticdb {

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
    size_t num_rows,
    const Column& source_column,
    const StringPool &string_pool,
    const std::optional<util::BitSet>& bitset
    ) {
    auto string_constructor = get_string_constructor(has_type_conversion, is_utf);

    switch (string_constructor) {
    case PyStringConstructor::Unicode_FromUnicode:
        process_string_views_for_type<UnicodeFromUnicodeCreator>(num_rows, source_column, has_type_conversion, string_pool, bitset, shared_data_.optimize_for_memory());
        break;
    case PyStringConstructor::Unicode_FromStringAndSize:
        process_string_views_for_type<UnicodeFromStringAndSizeCreator>(num_rows, source_column, has_type_conversion, string_pool, bitset, shared_data_.optimize_for_memory());
        break;
    case PyStringConstructor::Bytes_FromStringAndSize:
        process_string_views_for_type<BytesFromStringAndSizeCreator>(num_rows, source_column, has_type_conversion, string_pool, bitset, shared_data_.optimize_for_memory());
    }
}

DynamicStringReducer::DynamicStringReducer(
    DecodePathData shared_data,
    PythonHandlerData& handler_data,
    PyObject** ptr_dest,
    size_t total_rows) :
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data),
        ptr_dest_(ptr_dest),
        total_rows_(total_rows) {
    util::check(static_cast<bool>(handler_data_.py_nan_), "Got null nan in string reducer");
    util::check(is_py_nan(handler_data_.py_nan_->ptr()), "Got the wrong value in global nan");
}

void DynamicStringReducer::reduce(const Column& source_column,
                                  TypeDescriptor source_type,
                                  TypeDescriptor target_type,
                                  size_t num_rows,
                                  const StringPool &string_pool,
                                  const std::optional<util::BitSet>& bitset) {

    const bool trivially_compatible = trivially_compatible_types(source_type, target_type);
    util::check(trivially_compatible || is_empty_type(target_type.data_type()),
                "String types are not trivially compatible. Cannot convert from type {} to {} in frame field.",
                source_type,
                target_type
    );

    auto is_utf = is_utf_type(slice_value_type(source_type.data_type()));
    const auto has_type_conversion = source_type != target_type;
    process_string_views(has_type_conversion, is_utf, num_rows, source_column, string_pool, bitset);
}

void DynamicStringReducer::finalize() {
    if (row_ != total_rows_) {
        auto none = GilSafePyNone::instance();;
        const auto diff = total_rows_ - row_;
        for (; row_ < total_rows_; ++row_, ++ptr_dest_) {
            *ptr_dest_ = none->ptr();
        }

        increment_none_refcount(diff, none);
    }
}

} // namespace arcticdb