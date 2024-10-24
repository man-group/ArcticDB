/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/spinlock.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/util/buffer_holder.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <util/gil_safe_py_none.hpp>

namespace arcticdb {

inline PythonHandlerData& get_handler_data(std::any& any) {
    return std::any_cast<PythonHandlerData&>(any);
}

class DynamicStringReducer  {
    size_t row_ = 0U;
    DecodePathData shared_data_;
    PythonHandlerData& handler_data_;
    PyObject** ptr_dest_;
    size_t total_rows_;
public:
    DynamicStringReducer(
        DecodePathData shared_data,
        PythonHandlerData& handler_data,
        PyObject** ptr_dest_,
        size_t total_rows);

    void reduce(
        const Column& source_column,
        TypeDescriptor source_type,
        TypeDescriptor target_type,
        size_t num_rows,
        const StringPool &string_pool,
        const std::optional<util::BitSet>& bitset);

    void finalize();

private:
    struct UnicodeFromUnicodeCreator {
        static PyObject* create(std::string_view sv, bool) {
            const auto size = sv.size() + 4;
            auto* buffer = reinterpret_cast<char*>(alloca(size));
            memset(buffer, 0, size);
            memcpy(buffer, sv.data(), sv.size());

            const auto actual_length = std::min(sv.size() / UNICODE_WIDTH, wcslen(reinterpret_cast<const wchar_t *>(buffer)));
            return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, reinterpret_cast<const UnicodeType*>(sv.data()), actual_length);
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

    static void inc_ref(PyObject* obj) {
        Py_INCREF(obj);
    }

    auto get_unique_counts(
        const Column& column
    ) {
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

    std::pair<size_t, size_t> write_strings_to_destination(
        size_t num_rows,
        const Column& source_column,
        std::shared_ptr<py::none>& none,
        const ankerl::unordered_dense::map<entity::position_t, PyObject*> py_strings,
        const std::optional<util::BitSet>& sparse_map) {
        std::pair<size_t, size_t> counts;
        if(sparse_map) {
            prefill_with_none(ptr_dest_, num_rows, 0, handler_data_.spin_lock(), python_util::IncrementRefCount::OFF);
            counts = write_strings_to_column_sparse(num_rows, source_column, none, py_strings, *sparse_map);
        } else {
            counts = write_strings_to_column_dense(num_rows, source_column, none, py_strings);
        }
        return counts;
    }

    template<typename StringCreator>
    void assign_strings_local(
            size_t num_rows,
            const Column& source_column,
            bool has_type_conversion,
            const StringPool& string_pool,
            const std::optional<util::BitSet>& sparse_map) {
        ARCTICDB_SAMPLE(AssignStringsLocal, 0)
        using namespace python_util;
        auto unique_counts = get_unique_counts(source_column);
        auto py_strings = assign_python_strings<StringCreator>(unique_counts, has_type_conversion, string_pool);

        ARCTICDB_SUBSAMPLE(WriteStringsToColumn, 0)
        auto none = GilSafePyNone::instance();
        auto [none_count, nan_count] = write_strings_to_destination(num_rows, source_column, none, py_strings, sparse_map);
        increment_none_refcount(none_count, none);
        increment_nan_refcount(nan_count);
    }

    ankerl::unordered_dense::map<entity::position_t, PyObject*> get_allocated_strings(
        const ankerl::unordered_dense::map<entity::position_t, size_t>& unique_counts,
        const DecodePathData& shared_data,
        const StringPool& string_pool
        ) {
        ankerl::unordered_dense::map<entity::position_t, PyObject *> output;
        const auto& shared_map = *shared_data.unique_string_map();
        for(auto& pair : unique_counts) {
            auto offset = pair.first;

            if(auto it = shared_map.find(get_string_from_pool(offset, string_pool)); it != shared_map.end())
                output.try_emplace(offset, it->second);
        }
        return output;
    }

    template<typename StringCreator>
    void assign_strings_shared(
        size_t num_rows,
        const Column& source_column,
        bool has_type_conversion,
        const StringPool& string_pool,
        const std::optional<util::BitSet>&) {
        ARCTICDB_SAMPLE(AssignStringsShared, 0)
        auto unique_counts = get_unique_counts(source_column);
        auto allocated = get_allocated_strings(unique_counts, shared_data_, string_pool);
        auto &shared_map = *shared_data_.unique_string_map();
        {
            py::gil_scoped_acquire acquire_gil;
            PyObject *obj{};
            for (auto [offset, count] : unique_counts) {
                if (auto it = allocated.find(offset); it == allocated.end()) {
                    const auto sv = get_string_from_pool(offset, string_pool);
                    if (auto shared = shared_map.find(get_string_from_pool(offset, string_pool)); shared
                        != shared_map.end()) {
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
        auto none = GilSafePyNone::instance();
        auto [none_count, nan_count] = write_strings_to_destination(num_rows, source_column, none, allocated, source_column.opt_sparse_map());
        increment_none_refcount(none_count, none);
        increment_nan_refcount(nan_count);
    }

    template<typename StringCreator>
    auto assign_python_strings(
        const ankerl::unordered_dense::map<entity::position_t, size_t>& unique_counts,
        bool has_type_conversion,
        const StringPool& string_pool) {
        ankerl::unordered_dense::map<entity::position_t, PyObject *> py_strings;
        py_strings.reserve(unique_counts.size());

        {
            ARCTICDB_SUBSAMPLE(CreatePythonStrings, 0)
            py::gil_scoped_acquire gil_lock;
            for (const auto &[offset, count] : unique_counts) {
                const auto sv = get_string_from_pool(offset, string_pool);
                auto obj = StringCreator::create(sv, has_type_conversion);
                for (auto c = 1U; c < count; ++c)
                    inc_ref(obj);

                py_strings.insert(std::make_pair(offset, obj));
            }
        }
        return py_strings;
    }

    void increment_none_refcount(size_t none_count, std::shared_ptr<py::none>& none) {
        util::check(none, "Got null pointer to py::none in increment_none_refcount");
        std::lock_guard lock(handler_data_.spin_lock());
        for(auto i = 0u; i < none_count; ++i) {
            Py_INCREF(none->ptr());
        }
    }

    void increment_nan_refcount(size_t none_count) {
        std::lock_guard lock(handler_data_.spin_lock());
        for(auto i = 0u; i < none_count; ++i)
            Py_INCREF(handler_data_.py_nan_->ptr());
    }

    std::pair<size_t, size_t> write_strings_to_column_dense(
            size_t ,
            const Column& source_column,
            const std::shared_ptr<py::none>& none,
            const ankerl::unordered_dense::map<entity::position_t, PyObject*>& py_strings) {
        auto data = source_column.data();
        auto src = data.cbegin<ScalarTagType<DataTypeTag<DataType::UINT64>>, IteratorType::REGULAR, IteratorDensity::DENSE>();
        auto end = data.cend<ScalarTagType<DataTypeTag<DataType::UINT64>>, IteratorType::REGULAR, IteratorDensity::DENSE>();
        size_t none_count = 0u;
        size_t nan_count = 0u;
        for (; src != end; ++src, ++ptr_dest_, ++row_) {
            const auto offset = *src;
            if(offset == not_a_string()) {
                *ptr_dest_ = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = handler_data_.py_nan_->ptr();
                ++nan_count;
            } else {
                *ptr_dest_ = py_strings.at(offset);
            }
        }
        return {none_count, nan_count};
    }

    std::pair<size_t, size_t> write_strings_to_column_sparse(
        size_t num_rows,
        const Column& source_column,
        const std::shared_ptr<py::none>& none,
        const ankerl::unordered_dense::map<entity::position_t, PyObject*>& py_strings,
        const util::BitSet& sparse_map
    ) {
        auto data = source_column.data();
        auto src = data.begin<ScalarTagType<DataTypeTag<DataType::UINT64>>, IteratorType::REGULAR, IteratorDensity::DENSE>();
        auto en = sparse_map.first();
        auto en_end = sparse_map.end();
        auto none_count = 0UL;
        auto nan_count = 0UL;
        while(en != en_end) {
            const auto offset = *src;
            if(offset == not_a_string()) {
                ptr_dest_[*en] = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                ptr_dest_[*en] = handler_data_.py_nan_->ptr();
                ++nan_count;
            } else {
                ptr_dest_[*en] = py_strings.at(offset);
            }
            ++src;
            ++en;
        }
        ptr_dest_ += num_rows;
        row_ += num_rows;
        none_count += num_rows - sparse_map.count();
        return {none_count, nan_count};
    }

    inline void process_string_views(
        bool has_type_conversion,
        bool is_utf,
        size_t end,
        const Column& source_column,
        const StringPool& string_pool,
        const std::optional<util::BitSet>& bitset);


    template<typename StringCreator>
    inline void process_string_views_for_type(
        size_t num_rows,
        const Column& source_column,
        bool has_type_conversion,
        const StringPool& string_pool,
        const std::optional<util::BitSet>& bitset,
        bool optimize_for_memory
    ) {
        if (optimize_for_memory)
            assign_strings_shared<StringCreator>(num_rows, source_column, has_type_conversion, string_pool, bitset);
        else
            assign_strings_local<StringCreator>(num_rows, source_column, has_type_conversion, string_pool, bitset);
    }
};

} // namespace arcticdb