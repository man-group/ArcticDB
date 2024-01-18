/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/python_output_frame.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/memory_tracing.hpp>

namespace arcticdb::detail {

inline py::array array_at(const SegmentInMemory& frame, std::size_t col_pos, py::object &anchor) {
    ARCTICDB_SAMPLE_DEFAULT(PythonOutputFrameArrayAt)
    if (frame.empty()) {
        return visit_field(frame.field(col_pos), [] (auto tag) {
            using TypeTag = std::decay_t<decltype(tag)>;
            constexpr auto data_type = TypeTag::DataTypeTag::data_type;
            std::string dtype;
            ssize_t esize = is_sequence_type(data_type) && is_fixed_string_type(data_type) ? 1 : get_type_size(data_type);
            if constexpr (is_sequence_type(data_type)) {
                if constexpr (is_fixed_string_type(data_type)) {
                    dtype = data_type == DataType::ASCII_FIXED64 ? "<S0" : "<U0";
                } else {
                    dtype = "O";
                }
            } else if constexpr((is_numeric_type(data_type) || is_bool_type(data_type)) && tag.dimension() == Dimension::Dim0) {
                constexpr auto dim = TypeTag::DimensionTag::value;
                util::check(dim == Dimension::Dim0, "Only scalars supported, {}", data_type);
                if constexpr (data_type == DataType::NANOSECONDS_UTC64) {
                    // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond resolution,
                    // i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
                    // Yet, this has changed in Pandas 2.0 and other resolution can be used,
                    // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
                    // See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
                    // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
                    // rely uniquely on the resolution-less 'M' specifier if it this doable.
                    dtype = "datetime64[ns]";
                } else {
                    dtype = fmt::format("{}{:d}", get_dtype_specifier(data_type), esize);
                }
            } else if constexpr (is_empty_type(data_type) || is_py_bool_type(data_type) || is_numpy_array(TypeDescriptor(tag))) {
                dtype= "O";
                // The python representation of multidimensional columns differs from the in-memory/on-storage. In memory,
                // we hold all scalars in a contiguous buffer with the shapes buffer telling us how many elements are there
                // per array. Each element is of size sizeof(DataTypeTag::raw_type). For the python representation the column
                // is represented as an array of (numpy) arrays. Each nested arrays is represented as a pointer to the
                // (numpy) array, thus the size of the element is not the size of the raw type, but the size of a pointer.
                // This also affects how we allocate columns. Check cpp/arcticdb/column_store/column.hpp::Column and
                // cpp/arcticdb/pipeline/column_mapping.hpp::sizeof_datatype
                if constexpr(tag.dimension() > Dimension::Dim0) {
                    esize = sizeof(PyObject*);
                }
            } else if constexpr(tag.dimension() == Dimension::Dim2) {
                util::raise_rte("Read resulted in two dimensional type. This is not supported.");
            } else {
                static_assert(!sizeof(data_type), "Unhandled data type");
            }
            return py::array{py::dtype{dtype}, py::array::ShapeContainer{0}, py::array::StridesContainer{esize}};
        });
    }
    return visit_field(frame.field(col_pos), [&, frame=frame, col_pos=col_pos] (auto tag) {
        using TypeTag = std::decay_t<decltype(tag)>;
        constexpr auto data_type = TypeTag::DataTypeTag::data_type;
        const auto& buffer = frame.column(col_pos).data().buffer();
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
        } else if constexpr((is_numeric_type(data_type) || is_bool_type(data_type)) && tag.dimension() == Dimension::Dim0) {
            constexpr auto dim = TypeTag::DimensionTag::value;
            util::check(dim == Dimension::Dim0, "Only scalars supported, {}", frame.field(col_pos));
            if constexpr (data_type == DataType::NANOSECONDS_UTC64) {
                // NOTE: this is safe as of Pandas < 2.0 because `datetime64` _always_ has been using nanosecond resolution,
                // i.e. Pandas < 2.0 _always_ provides `datetime64[ns]` and ignores any other resolution.
                // Yet, this has changed in Pandas 2.0 and other resolution can be used,
                // i.e. Pandas >= 2.0 will also provides `datetime64[us]`, `datetime64[ms]` and `datetime64[s]`.
                // See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution
                // TODO: for the support of Pandas>=2.0, convert any `datetime` to `datetime64[ns]` before-hand and do not
                // rely uniquely on the resolution-less 'M' specifier if it this doable.
                dtype = "datetime64[ns]";
            } else {
                dtype = fmt::format("{}{:d}", get_dtype_specifier(data_type), esize);
            }
        } else if constexpr (is_empty_type(data_type) || is_py_bool_type(data_type) || is_numpy_array(TypeDescriptor(tag))){
            dtype = "O";
            // The python representation of multidimensional columns differs from the in-memory/on-storage. In memory,
            // we hold all scalars in a contiguous buffer with the shapes buffer telling us how many elements are there
            // per array. Each element is of size sizeof(DataTypeTag::raw_type). For the python representation the column
            // is represented as an array of (numpy) arrays. Each nested arrays is represented as a pointer to the
            // (numpy) array, thus the size of the element is not the size of the raw type, but the size of a pointer.
            // This also affects how we allocate columns. Check cpp/arcticdb/column_store/column.hpp::Column and
            // cpp/arcticdb/pipeline/column_mapping.hpp::sizeof_datatype
            if constexpr(tag.dimension() > Dimension::Dim0) {
                esize = sizeof(PyObject*);
            }
        } else if constexpr(tag.dimension() == Dimension::Dim2) {
            util::raise_rte("Read resulted in two dimensional type. This is not supported.");
        } else {
            static_assert(!sizeof(data_type), "Unhandled data type");
        }
        // Note how base is passed to the array to register the data owner.
        // It's especially important to keep the frame data object alive for as long as the array is alive
        // so that regular python ref counting logic handles the liveness
        return py::array(py::dtype{dtype}, {frame.row_count()}, {esize}, buffer.data(), anchor);
    });
}

inline std::shared_ptr<pipelines::FrameDataWrapper> initialize_array(const SegmentInMemory& frame, py::object &ref) {
    auto output = std::make_shared<pipelines::FrameDataWrapper>(frame.fields().size());
    ARCTICDB_SAMPLE(InitializeArrays, 0);
    ARCTICDB_DEBUG(log::memory(), "Initializing arrays");
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    for (std::size_t c = 0; c < static_cast<size_t>(frame.fields().size()); ++c) {
        output->data_[c] = array_at(frame, c, ref);
    }
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    return output;
}

}

namespace arcticdb::pipelines {

PythonOutputFrame::PythonOutputFrame(const SegmentInMemory &frame, std::shared_ptr<BufferHolder> buffers) :
        module_data_(ModuleData::instance()),
        frame_(frame),
        names_(frame.fields().size() - frame.descriptor().index().field_count()),
        index_columns_(frame.descriptor().index().field_count()),
        buffers_(std::move(buffers)) {
    ARCTICDB_SAMPLE_DEFAULT(PythonOutputFrameCtor)
    const auto field_count = frame.descriptor().index().field_count();
    // works because we ensure that the index must be fetched
    for (std::size_t c = 0; c < field_count; ++c) {
        index_columns_[c] = frame.field(c).name();
    }
    for (std::size_t c = frame.descriptor().index().field_count(); c < static_cast<size_t>(frame.fields().size()); ++c) {
        names_[c - field_count] = frame.field(c).name();
    }
}

PythonOutputFrame::~PythonOutputFrame() {
    if(frame_.is_null())
        return;

    for (auto &column : frame_.columns()) {
        auto dt = column->type().data_type();
        if (is_dynamic_string_type(dt) && column->is_inflated()) {
            auto column_data = column->data();
            column_data.type().visit_tag([&](auto type_desc_tag) {
                using TDT = decltype(type_desc_tag);
                constexpr auto td = TypeDescriptor(type_desc_tag);
                if constexpr (is_pyobject_type(TypeDescriptor(type_desc_tag))) {
                    if constexpr(is_numpy_array(td)) {
                        auto it = column_data.buffer().iterator(sizeof(PyObject*));
                        while(!it.finished()) {
                            util::check(reinterpret_cast<PyObject*>(it.value()) != nullptr, "Can't delete null item");
                            Py_DECREF(reinterpret_cast<PyObject*>(it.value()));
                            it.next();
                        }
                    } else {
                        while (auto block = column_data.next<TDT>()) {
                            for (auto item : *block) {
                                util::check(reinterpret_cast<PyObject *>(item) != nullptr, "Can't delete null item");
                                Py_DECREF(reinterpret_cast<PyObject *>(item));
                            }
                        }
                    }
                }
            });
        }
    }
}


std::shared_ptr<FrameDataWrapper> PythonOutputFrame::arrays(py::object &ref) {
    if(auto cached = arrays_.lock())
        return cached;

    auto generated = arcticdb::detail::initialize_array(frame_, ref);
    arrays_  = generated;
    return generated;
}

py::array PythonOutputFrame::array_at(std::size_t col_pos, py::object &anchor) {
    return arcticdb::detail::array_at(frame_, col_pos, anchor);
}

}
