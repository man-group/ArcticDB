/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_utils_python.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/python/numpy_buffer_holder.hpp>

namespace arcticdb::detail {
py::array array_at(const SegmentInMemory& frame, std::size_t col_pos) {
    ARCTICDB_SAMPLE_DEFAULT(PythonOutputFrameArrayAt)
    if (frame.empty()) {
        return visit_field(frame.field(col_pos), [](auto tag) {
            using TypeTag = std::decay_t<decltype(tag)>;
            constexpr auto data_type = TypeTag::DataTypeTag::data_type;
            std::string dtype;
            ssize_t esize =
                    is_sequence_type(data_type) && is_fixed_string_type(data_type) ? 1 : get_type_size(data_type);
            if constexpr (is_sequence_type(data_type)) {
                if constexpr (is_fixed_string_type(data_type)) {
                    dtype = data_type == DataType::ASCII_FIXED64 ? "<S0" : "<U0";
                } else {
                    dtype = "O";
                }
            } else if constexpr ((is_numeric_type(data_type) || is_bool_type(data_type)) &&
                                 tag.dimension() == Dimension::Dim0) {
                constexpr auto dim = TypeTag::DimensionTag::value;
                util::check(dim == Dimension::Dim0, "Only scalars supported, {}", data_type);
                if constexpr (data_type == DataType::NANOSECONDS_UTC64) {
                    // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond
                    // resolution, i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other
                    // resolution. Yet, this has changed in Pandas 2.0 and other resolution can be used, i.e. Pandas
                    // >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`. See:
                    // https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
                    // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and
                    // do not rely uniquely on the resolution-less 'M' specifier if it this doable.
                    dtype = "datetime64[ns]";
                } else {
                    dtype = fmt::format("{}{:d}", get_dtype_specifier(data_type), esize);
                }
            } else if constexpr (is_empty_type(data_type) || is_bool_object_type(data_type) ||
                                 is_array_type(TypeDescriptor(tag))) {
                dtype = "O";
                esize = data_type_size(TypeDescriptor{tag});
            } else if constexpr (tag.dimension() == Dimension::Dim2) {
                util::raise_rte("Read resulted in two dimensional type. This is not supported.");
            } else {
                static_assert(!sizeof(data_type), "Unhandled data type");
            }
            return py::array{py::dtype{dtype}, py::array::ShapeContainer{0}, py::array::StridesContainer{esize}};
        });
    }
    return visit_field(frame.field(col_pos), [&, frame = frame, col_pos = col_pos](auto tag) {
        using TypeTag = std::decay_t<decltype(tag)>;
        constexpr auto data_type = TypeTag::DataTypeTag::data_type;
        auto column_data = frame.column(col_pos).data();
        const auto& buffer = column_data.buffer();
        util::check(buffer.num_blocks() == 1, "Expected 1 block when creating ndarray, got {}", buffer.num_blocks());
        auto* block = buffer.blocks().at(0);
        size_t allocated_bytes = block->bytes();
        uint8_t* ptr = block->release();
        NumpyBufferHolder numpy_buffer_holder(TypeDescriptor{tag}, ptr, frame.row_count(), allocated_bytes);
        auto base_obj = pybind11::cast(std::move(numpy_buffer_holder));
        std::string dtype;
        ssize_t esize = get_type_size(data_type);
        if constexpr (is_sequence_type(data_type)) {
            if (is_fixed_string_type(data_type)) {
                esize = buffer.bytes() / frame.row_count();
                auto char_count = esize;
                if (data_type == DataType::UTF_FIXED64) {
                    char_count /= UNICODE_WIDTH;
                }
                dtype = fmt::format((data_type == DataType::ASCII_FIXED64 ? "<S{:d}" : "<U{:d}"), char_count);
            } else {
                dtype = "O";
            }
        } else if constexpr ((is_numeric_type(data_type) || is_bool_type(data_type)) &&
                             tag.dimension() == Dimension::Dim0) {
            constexpr auto dim = TypeTag::DimensionTag::value;
            util::check(dim == Dimension::Dim0, "Only scalars supported, {}", frame.field(col_pos));
            if constexpr (data_type == DataType::NANOSECONDS_UTC64) {
                // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond
                // resolution, i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
                // Yet, this has changed in Pandas 2.0 and other resolution can be used,
                // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
                // See:
                // https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
                // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do
                // not rely uniquely on the resolution-less 'M' specifier if it this doable.
                dtype = "datetime64[ns]";
            } else {
                dtype = fmt::format("{}{:d}", get_dtype_specifier(data_type), esize);
            }
        } else if constexpr (is_empty_type(data_type) || is_bool_object_type(data_type)) {
            dtype = "O";
            esize = data_type_size(TypeDescriptor{tag});
        } else if constexpr (is_array_type(TypeDescriptor(tag))) {
            dtype = "O";
            esize = data_type_size(TypeDescriptor{tag});
            // The python representation of multidimensional columns differs from the in-memory/on-storage. In memory,
            // we hold all scalars in a contiguous buffer with the shapes buffer telling us how many elements are there
            // per array. Each element is of size sizeof(DataTypeTag::raw_type). For the python representation the
            // column is represented as an array of (numpy) arrays. Each nested arrays is represented as a pointer to
            // the (numpy) array, thus the size of the element is not the size of the raw type, but the size of a
            // pointer. This also affects how we allocate columns. Check cpp/arcticdb/column_store/column.hpp::Column
            // and cpp/arcticdb/pipeline/column_mapping.hpp::external_datatype_size
            auto& api = py::detail::npy_api::get();
            auto py_ptr = reinterpret_cast<PyObject**>(ptr);
            for (size_t idx = 0; idx < frame.row_count(); ++idx, ++py_ptr) {
                util::check(py_ptr != nullptr, "Can't set base object on null item");
                if (!is_py_none(*py_ptr)) {
                    api.PyArray_SetBaseObject_(*py_ptr, base_obj.inc_ref().ptr());
                }
            }
        } else if constexpr (tag.dimension() == Dimension::Dim2) {
            util::raise_rte("Read resulted in two dimensional type. This is not supported.");
        } else {
            static_assert(!sizeof(data_type), "Unhandled data type");
        }
        return py::array(py::dtype{dtype}, {frame.row_count()}, {esize}, ptr, base_obj);
    });
}
} // namespace arcticdb::detail
