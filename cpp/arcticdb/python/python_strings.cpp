/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/python/python_strings.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>

namespace arcticdb {

struct UnicodeFromUnicodeCreator {
    static PyObject* create(std::string_view sv, bool) {
        const auto size = sv.size() + 4;
        auto* buffer = reinterpret_cast<char*>(alloca(size));
        memset(buffer, 0, size);
        memcpy(buffer, sv.data(), sv.size());

        const auto actual_length =
                std::min(sv.size() / UNICODE_WIDTH, wcslen(reinterpret_cast<const wchar_t*>(buffer)));
        return PyUnicode_FromKindAndData(
                PyUnicode_4BYTE_KIND, reinterpret_cast<const UnicodeType*>(sv.data()), actual_length
        );
    }
};

struct UnicodeFromStringAndSizeCreator {
    static PyObject* create(std::string_view sv, bool) {
        const auto actual_length = sv.size();
        return PyUnicode_FromStringAndSize(sv.data(), actual_length);
    }
};

struct BytesFromStringAndSizeCreator {
    static PyObject* create(std::string_view sv, bool has_type_conversion) {
        const auto actual_length = has_type_conversion ? std::min(sv.size(), strlen(sv.data())) : sv.size();
        return PYBIND11_BYTES_FROM_STRING_AND_SIZE(sv.data(), actual_length);
    }
};

static void inc_ref(PyObject* obj) { Py_INCREF(obj); }

static auto get_unique_counts(const Column& column) {
    ankerl::unordered_dense::map<entity::position_t, size_t> unique_counts;
    unique_counts.reserve(column.row_count());
    auto data = column.data();
    auto it = data.begin<ScalarTagType<DataTypeTag<DataType::UINT64>>, IteratorType::REGULAR, IteratorDensity::DENSE>();
    auto end = data.end<ScalarTagType<DataTypeTag<DataType::UINT64>>, IteratorType::REGULAR, IteratorDensity::DENSE>();
    for (; it != end; ++it) {
        const auto offset = *it;
        if (offset != not_a_string() && offset != nan_placeholder()) {
            ++unique_counts[offset];
        }
    }
    return unique_counts;
}

static std::pair<size_t, size_t> write_strings_to_destination(
        PyObject** ptr_dest, size_t num_rows, const Column& source_column,
        const ankerl::unordered_dense::map<entity::position_t, PyObject*>& py_strings, PythonHandlerData& handler_data
) {
    if (source_column.is_sparse()) {
        python_util::prefill_with_none(ptr_dest, num_rows, 0, handler_data, python_util::IncrementRefCount::OFF);
    }
    auto none_count = 0UL;
    auto nan_count = 0UL;
    details::visit_type(source_column.type().data_type(), [&](auto source_tag) {
        using source_type_info = ScalarTypeInfo<decltype(source_tag)>;
        if constexpr (is_sequence_type(source_type_info::data_type)) {
            for_each_enumerated<typename source_type_info::TDT>(source_column, [&](const auto& en) {
                const auto offset = en.value();
                if (offset == not_a_string()) {
                    ptr_dest[en.idx()] = Py_None;
                    ++none_count;
                } else if (offset == nan_placeholder()) {
                    ptr_dest[en.idx()] = handler_data.non_owning_nan_handle();
                    ++nan_count;
                } else {
                    ptr_dest[en.idx()] = py_strings.at(offset);
                }
            });
        } else {
            util::raise_rte("Expected column to have a string type but got {}", source_type_info::data_type);
        }
    });
    if (source_column.is_sparse()) {
        none_count += num_rows - source_column.sparse_map().count();
    }
    return {none_count, nan_count};
}

template<typename StringCreator>
static auto assign_python_strings(
        const ankerl::unordered_dense::map<entity::position_t, size_t>& unique_counts, bool has_type_conversion,
        const StringPool& string_pool
) {
    ankerl::unordered_dense::map<entity::position_t, PyObject*> py_strings;
    py_strings.reserve(unique_counts.size());

    {
        ARCTICDB_SUBSAMPLE(CreatePythonStrings, 0)
        py::gil_scoped_acquire gil_lock;
        for (const auto& [offset, count] : unique_counts) {
            const auto sv = get_string_from_pool(offset, string_pool);
            auto obj = StringCreator::create(sv, has_type_conversion);
            for (auto c = 1U; c < count; ++c)
                inc_ref(obj);

            py_strings.insert(std::make_pair(offset, obj));
        }
    }
    return py_strings;
}

template<typename StringCreator>
static void assign_strings_local(
        PyObject** ptr_dest, size_t num_rows, const Column& source_column, bool has_type_conversion,
        const StringPool& string_pool, PythonHandlerData& handler_data
) {
    ARCTICDB_SAMPLE(AssignStringsLocal, 0)
    auto unique_counts = get_unique_counts(source_column);
    auto py_strings = assign_python_strings<StringCreator>(unique_counts, has_type_conversion, string_pool);

    ARCTICDB_SUBSAMPLE(WriteStringsToColumn, 0)
    auto [none_count, nan_count] =
            write_strings_to_destination(ptr_dest, num_rows, source_column, py_strings, handler_data);
    handler_data.increment_none_refcount(none_count);
    handler_data.increment_nan_refcount(nan_count);
}

static ankerl::unordered_dense::map<entity::position_t, PyObject*> get_allocated_strings(
        const ankerl::unordered_dense::map<entity::position_t, size_t>& unique_counts,
        const DecodePathData& shared_data, const StringPool& string_pool
) {
    ankerl::unordered_dense::map<entity::position_t, PyObject*> output;
    const auto& shared_map = *shared_data.unique_string_map();
    for (auto& pair : unique_counts) {
        auto offset = pair.first;

        if (auto it = shared_map.find(get_string_from_pool(offset, string_pool)); it != shared_map.end())
            output.try_emplace(offset, it->second);
    }
    return output;
}

template<typename StringCreator>
static void assign_strings_shared(
        PyObject** ptr_dest, size_t num_rows, const Column& source_column, bool has_type_conversion,
        const StringPool& string_pool, const DecodePathData& shared_data, PythonHandlerData& handler_data
) {
    ARCTICDB_SAMPLE(AssignStringsShared, 0)
    auto unique_counts = get_unique_counts(source_column);
    auto allocated = get_allocated_strings(unique_counts, shared_data, string_pool);
    auto& shared_map = *shared_data.unique_string_map();
    {
        py::gil_scoped_acquire acquire_gil;
        PyObject* obj{};
        for (auto [offset, count] : unique_counts) {
            if (auto it = allocated.find(offset); it == allocated.end()) {
                const auto sv = get_string_from_pool(offset, string_pool);
                if (auto shared = shared_map.find(get_string_from_pool(offset, string_pool));
                    shared != shared_map.end()) {
                    obj = StringCreator::create(sv, has_type_conversion);
                    shared_map.try_emplace(sv, obj);
                } else {
                    obj = shared->second;
                }

                allocated.try_emplace(offset, obj);
                for (auto c = 1U; c < count; ++c)
                    inc_ref(obj);
            }
        }
    }
    auto [none_count, nan_count] =
            write_strings_to_destination(ptr_dest, num_rows, source_column, allocated, handler_data);
    handler_data.increment_none_refcount(none_count);
    handler_data.increment_nan_refcount(nan_count);
}

template<typename StringCreator>
static void process_string_views_for_type(
        PyObject** ptr_dest, size_t num_rows, const Column& source_column, bool has_type_conversion,
        const StringPool& string_pool, const DecodePathData& shared_data, PythonHandlerData& handler_data
) {
    if (shared_data.optimize_for_memory())
        assign_strings_shared<StringCreator>(
                ptr_dest, num_rows, source_column, has_type_conversion, string_pool, shared_data, handler_data
        );
    else
        assign_strings_local<StringCreator>(
                ptr_dest, num_rows, source_column, has_type_conversion, string_pool, handler_data
        );
}

enum class PyStringConstructor { Unicode_FromUnicode, Unicode_FromStringAndSize, Bytes_FromStringAndSize };

static inline PyStringConstructor get_string_constructor(bool has_type_conversion, bool is_utf) {
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

static void process_string_views(
        PyObject** ptr_dest, bool has_type_conversion, bool is_utf, size_t num_rows, const Column& source_column,
        const StringPool& string_pool, const DecodePathData& shared_data, PythonHandlerData& handler_data
) {
    auto string_constructor = get_string_constructor(has_type_conversion, is_utf);

    switch (string_constructor) {
    case PyStringConstructor::Unicode_FromUnicode:
        process_string_views_for_type<UnicodeFromUnicodeCreator>(
                ptr_dest, num_rows, source_column, has_type_conversion, string_pool, shared_data, handler_data
        );
        break;
    case PyStringConstructor::Unicode_FromStringAndSize:
        process_string_views_for_type<UnicodeFromStringAndSizeCreator>(
                ptr_dest, num_rows, source_column, has_type_conversion, string_pool, shared_data, handler_data
        );
        break;
    case PyStringConstructor::Bytes_FromStringAndSize:
        process_string_views_for_type<BytesFromStringAndSizeCreator>(
                ptr_dest, num_rows, source_column, has_type_conversion, string_pool, shared_data, handler_data
        );
    }
}

void write_python_dynamic_strings_to_dest(
        PyObject** ptr_dest, const Column& source_column, const ColumnMapping& mapping, const StringPool& string_pool,
        const DecodePathData& shared_data, PythonHandlerData& handler_data
) {
    util::check(handler_data.is_nan_initialized(), "Got null nan in string reducer");
    util::check(is_py_nan(handler_data.non_owning_nan_handle()), "Got the wrong value in global nan");

    const auto& source_type = mapping.source_type_desc_;
    const auto& target_type = mapping.dest_type_desc_;

    const bool trivially_compatible = trivially_compatible_types(source_type, target_type);
    util::check(
            trivially_compatible || is_empty_type(target_type.data_type()),
            "String types are not trivially compatible. Cannot convert from type {} to {} in frame field.",
            source_type,
            target_type
    );

    auto is_utf = is_utf_type(slice_value_type(source_type.data_type()));
    const auto has_type_conversion = source_type != target_type;
    process_string_views(
            ptr_dest,
            has_type_conversion,
            is_utf,
            mapping.num_rows_,
            source_column,
            string_pool,
            shared_data,
            handler_data
    );
}

} // namespace arcticdb
