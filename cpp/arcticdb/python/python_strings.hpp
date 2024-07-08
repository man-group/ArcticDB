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

namespace arcticdb {

class EmptyDynamicStringReducer {
protected:
    Column& column_;
    SegmentInMemory frame_;
    size_t row_ ;
    ChunkedBuffer& src_buffer_;
    size_t column_width_;
    ChunkedBuffer dest_buffer_;
    uint8_t *dst_;
    PyObject** ptr_dest_;
    std::shared_ptr<SpinLock> lock_;
    std::shared_ptr<PyObject> py_nan_;

public:
    EmptyDynamicStringReducer(
        Column& column,
        SegmentInMemory frame,
        const entity::Field&,
        size_t alloc_width,
        std::shared_ptr<SpinLock> spinlock);

    ~EmptyDynamicStringReducer();

    void reduce(size_t end);
};


class DynamicStringReducer  {
    size_t row_ = 0U;
    DecodePathData shared_data_;
    PyObject ** ptr_dest_;
    std::shared_ptr<PyObject> py_nan_;
    size_t total_rows_;
public:
    DynamicStringReducer(
        DecodePathData shared_data,
        PyObject** ptr_dest_,
        size_t total_rows);

    void reduce(
        TypeDescriptor source_type,
        TypeDescriptor target_type,
        size_t num_rows,
        const StringPool& string_pool,
        const position_t* ptr_src);

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

    void safe_incref(PyObject* obj) {
        lock().lock();
        inc_ref(obj);
        lock().unlock();
    }

    [[nodiscard]] std::unique_ptr<py::none> get_py_none() {
        lock().lock();
        auto none = std::make_unique<py::none>(py::none{});
        lock().unlock();
        return none;
    }

    template<typename StringCreator>
    void assign_strings_shared(size_t end, const entity::position_t* ptr_src, bool has_type_conversion, const StringPool& string_pool) {
        ARCTICDB_SAMPLE(AssignStringsShared, 0)
        auto none = get_py_none();
        size_t none_count = 0u;
        for (; row_ < end; ++row_, ++ptr_src, ++ptr_dest_) {
            auto offset = *ptr_src;
            if(offset == not_a_string()) {
                *ptr_dest_ = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = py_nan_.get();
                inc_ref(py_nan_.get());
            } else {
                const auto sv = get_string_from_pool(offset, string_pool);
                if (auto it = shared_data_.unique_string_map()->find(sv); it != shared_data_.unique_string_map()->end()) {
                    *ptr_dest_ = it->second;
                    safe_incref(*ptr_dest_);
                } else {
                    lock().lock();
                    *ptr_dest_ = StringCreator::create(sv, has_type_conversion) ;
                    Py_INCREF(*ptr_dest_);
                    lock().unlock();
                    shared_data_.unique_string_map()->emplace(sv, *ptr_dest_);
                }
            }
        }
        lock().lock();
        for(auto i = 0u; i < none_count; ++i)
            Py_INCREF(none->ptr());

        none.reset();
        lock().unlock();
    }


    template<typename StringCreator>
    void assign_strings_local(size_t end, const entity::position_t* ptr_src, bool has_type_conversion, const StringPool& string_pool) {
        ARCTICDB_SAMPLE(AssignStringsLocal, 0)
        auto none = get_py_none();
        size_t none_count = 0u;

        ankerl::unordered_dense::map<entity::position_t, size_t> unique_counts;
        unique_counts.reserve(end - row_);
        auto row = row_;
        auto src = ptr_src;
        for (; row < end; ++row, ++src) {
            const auto offset = *src;
            if(offset != not_a_string() && offset != nan_placeholder()) {
                ++unique_counts[offset];
            }
        }

        ankerl::unordered_dense::map<entity::position_t, PyObject*> py_strings;
        py_strings.reserve(unique_counts.size());

        {
            ARCTICDB_SUBSAMPLE(CreatePythonStrings, 0)
            py::gil_scoped_acquire gil_lock;
            for (const auto &[offset, count] : unique_counts) {
                const auto sv = get_string_from_pool(offset, string_pool);
                auto obj = StringCreator::create(sv, has_type_conversion);
                for (auto c = 0U; c < count; ++c)
                    inc_ref(obj);

                py_strings.insert(std::make_pair(offset, obj));
            }
        }

        ARCTICDB_SUBSAMPLE(WriteStringsToColumn, 0)
        for (; row_ < end; ++row_, ++ptr_dest_, ++ptr_src) {
            const auto offset = *ptr_src;
            if(offset == not_a_string()) {
                *ptr_dest_ = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = py_nan_.get();
                inc_ref(py_nan_.get());
            } else {
                *ptr_dest_ = py_strings[offset];
            }
        }

        lock().lock();
        for(auto i = 0u; i < none_count; ++i)
            Py_INCREF(none->ptr());

        none.reset();
        lock().unlock();
    }

    SpinLock& lock() {
        return *shared_data_.spin_lock();
    }

    inline void process_string_views(
        bool has_type_conversion,
        bool is_utf,
        size_t end,
        const entity::position_t* ptr_src,
        const StringPool& string_pool);
};

} // namespace arcticdb