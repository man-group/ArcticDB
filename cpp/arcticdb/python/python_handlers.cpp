/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <python/python_handlers.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/sparse_utils.hpp>
#include <arcticdb/python/python_strings.hpp>
#include <arcticdb/python/python_utils.hpp>

namespace arcticdb {

static PyObject** fill_with_none(ChunkedBuffer& buffer, size_t offset, size_t count, PythonHandlerData& handler_data) {
    auto dest = buffer.ptr_cast<PyObject*>(offset, count * sizeof(PyObject*));
    return python_util::fill_with_none(dest, count, handler_data);
}

void PythonEmptyHandler::handle_type(
        const uint8_t*& input, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& mapping,
        const DecodePathData&, std::any& handler_data, EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE(HandleEmpty, 0)
    ARCTICDB_TRACE(
            log::version(),
            "Empty type handler invoked for source type: {}, destination type: {}, num rows: {}",
            mapping.source_type_desc_,
            mapping.dest_type_desc_,
            mapping.num_rows_
    );
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject*));

    if (encoding_version == EncodingVersion::V2)
        util::check_magic<ColumnMagic>(input);

    if (field.encoding_case() == EncodedFieldType::NDARRAY) {
        const auto& ndarray_field = field.ndarray();
        const auto num_blocks = ndarray_field.values_size();
        util::check(num_blocks <= 1, "Unexpected number of empty type blocks: {}", num_blocks);
        for (auto block_num = 0; block_num < num_blocks; ++block_num) {
            const auto& block_info = ndarray_field.values(block_num);
            input += block_info.out_bytes();
        }
    } else {
        util::raise_rte("Unsupported encoding {}", field);
    }
    convert_type({}, dest_column, mapping, {}, handler_data, string_pool, read_options);
}

void PythonEmptyHandler::
        convert_type(const Column&, Column& dest_column, const ColumnMapping& mapping, const DecodePathData& shared_data, std::any& handler_data, const std::shared_ptr<StringPool>&, const ReadOptions&)
                const {
    auto dest_data = dest_column.bytes_at(
            mapping.offset_bytes_, mapping.num_rows_ * get_type_size(mapping.dest_type_desc_.data_type())
    );
    util::check(dest_data != nullptr, "Got null destination pointer");
    ARCTICDB_TRACE(
            log::version(),
            "Empty type handler invoked for source type: {}, destination type: {}, num rows: {}",
            mapping.source_type_desc_,
            mapping.dest_type_desc_,
            mapping.num_rows_
    );
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject*));

    if (is_object_type(mapping.dest_type_desc_) || is_empty_type(mapping.dest_type_desc_.data_type())) {
        default_initialize(
                dest_column.buffer(), mapping.offset_bytes_, mapping.num_rows_ * type_size(), shared_data, handler_data
        );
    } else {
        mapping.dest_type_desc_.visit_tag([&mapping, dest_data](auto tdt) {
            using TagType = decltype(tdt);
            using RawType = typename TagType::DataTypeTag::raw_type;
            const auto dest_bytes = mapping.num_rows_ * sizeof(RawType);
            util::default_initialize<TagType>(dest_data, dest_bytes);
        });
    }
}

int PythonEmptyHandler::type_size() const { return sizeof(PyObject*); }

std::pair<TypeDescriptor, size_t> PythonEmptyHandler::
        output_type_and_extra_bytes(const TypeDescriptor&, std::string_view, const ReadOptions&) const {
    return {make_scalar_type(DataType::EMPTYVAL), 0};
}

void PythonEmptyHandler::default_initialize(
        ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any
) const {
    auto& handler_data = cast_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data);
}

void PythonBoolHandler::handle_type(
        const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
        const DecodePathData& shared_data, std::any& handler_data, EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE(HandleBool, 0)
    util::check(field.has_ndarray(), "Bool handler expected array");
    ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", field.DebugString());
    const auto& ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
    Column decoded_data =
            Column(m.source_type_desc_,
                   bytes / get_type_size(m.source_type_desc_.data_type()),
                   AllocationType::DYNAMIC,
                   Sparsity::PERMITTED);
    data += decode_field(
            m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version
    );

    convert_type(decoded_data, dest_column, m, shared_data, handler_data, string_pool, read_options);
}

void PythonBoolHandler::
        convert_type(const Column& source_column, Column& dest_column, const ColumnMapping& mapping, const arcticdb::DecodePathData&, std::any& any, const std::shared_ptr<StringPool>&, const ReadOptions&)
                const {
    const auto& sparse_map = source_column.opt_sparse_map();
    const auto num_bools = sparse_map.has_value() ? sparse_map->count() : mapping.num_rows_;
    auto ptr_src = source_column.template ptr_cast<uint8_t>(0, num_bools * sizeof(uint8_t));
    auto dest_data = dest_column.bytes_at(mapping.offset_bytes_, mapping.num_rows_ * sizeof(PyObject*));
    util::check(dest_data != nullptr, "Got null destination pointer");
    auto ptr_dest = reinterpret_cast<PyObject**>(dest_data);
    if (sparse_map.has_value()) {
        auto& handler_data = cast_handler_data(any);
        ARCTICDB_TRACE(log::codec(), "Bool handler using a sparse map");
        unsigned last_row = 0u;
        for (auto en = sparse_map->first(); en < sparse_map->end(); ++en, ++last_row) {
            const auto current_pos = *en;
            ptr_dest = python_util::fill_with_none(ptr_dest, current_pos - last_row, handler_data);
            last_row = current_pos;
            *ptr_dest++ = py::bool_(static_cast<bool>(*ptr_src++)).release().ptr();
        }
        python_util::fill_with_none(ptr_dest, mapping.num_rows_ - last_row, handler_data);
    } else {
        ARCTICDB_TRACE(log::codec(), "Bool handler didn't find a sparse map. Assuming dense array.");
        std::transform(ptr_src, ptr_src + num_bools, ptr_dest, [](uint8_t value) {
            return py::bool_(static_cast<bool>(value)).release().ptr();
        });
    }
}

int PythonBoolHandler::type_size() const { return sizeof(PyObject*); }

std::pair<TypeDescriptor, size_t> PythonBoolHandler::
        output_type_and_extra_bytes(const TypeDescriptor&, std::string_view, const ReadOptions&) const {
    return {make_scalar_type(DataType::BOOL_OBJECT8), 0};
}

void PythonBoolHandler::default_initialize(
        ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any
) const {
    auto& handler_data = cast_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data);
}

void PythonStringHandler::handle_type(
        const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
        const DecodePathData& shared_data, std::any& handler_data, EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE(PythonHandleString, 0)
    util::check(field.has_ndarray(), "String handler expected array");
    ARCTICDB_DEBUG(log::version(), "String handler got encoded field: {}", field.DebugString());
    const auto& ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);

    auto decoded_data = [&m, &ndarray, bytes, &dest_column]() {
        if (ndarray.sparse_map_bytes() > 0) {
            return Column(
                    m.source_type_desc_,
                    bytes / get_type_size(m.source_type_desc_.data_type()),
                    AllocationType::DYNAMIC,
                    Sparsity::PERMITTED
            );
        } else {
            Column column(m.source_type_desc_, Sparsity::NOT_PERMITTED);
            column.buffer().add_external_block(
                    dest_column.bytes_at(m.offset_bytes_, m.num_rows_ * sizeof(PyObject*)), bytes
            );
            return column;
        }
    }();

    data += decode_field(
            m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version
    );

    if (is_dynamic_string_type(m.dest_type_desc_.data_type())) {
        convert_type(decoded_data, dest_column, m, shared_data, handler_data, string_pool, read_options);
    }
}

void PythonStringHandler::
        convert_type(const Column& source_column, Column& dest_column, const ColumnMapping& mapping, const DecodePathData& shared_data, std::any& handler_data, const std::shared_ptr<StringPool>& string_pool, const ReadOptions&)
                const {
    auto dest_data = dest_column.bytes_at(mapping.offset_bytes_, mapping.num_rows_ * sizeof(PyObject*));
    auto ptr_dest = reinterpret_cast<PyObject**>(dest_data);
    DynamicStringReducer string_reducer{shared_data, cast_handler_data(handler_data), ptr_dest, mapping.num_rows_};
    string_reducer.reduce(
            source_column,
            mapping.source_type_desc_,
            mapping.dest_type_desc_,
            mapping.num_rows_,
            *string_pool,
            source_column.opt_sparse_map()
    );
    string_reducer.finalize();
}

int PythonStringHandler::type_size() const { return sizeof(PyObject*); }

std::pair<TypeDescriptor, size_t> PythonStringHandler::
        output_type_and_extra_bytes(const TypeDescriptor& input_type, std::string_view, const ReadOptions&) const {
    return {input_type, 0};
}

void PythonStringHandler::default_initialize(
        ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any
) const {
    auto& handler_data = cast_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data);
}

[[nodiscard]] static inline py::dtype generate_python_dtype(const TypeDescriptor& td, stride_t type_byte_size) {
    if (is_empty_type(td.data_type())) {
        return py::dtype{"f8"};
    }
    return py::dtype{fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), type_byte_size)};
}

void PythonArrayHandler::handle_type(
        const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
        const DecodePathData& shared_data, std::any& any, EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE(HandleArray, 0)
    util::check(field.has_ndarray(), "Expected ndarray in array object handler");
    Column column{m.source_type_desc_, Sparsity::PERMITTED};
    data += decode_field(m.source_type_desc_, field, data, column, column.opt_sparse_map(), encoding_version);

    convert_type(column, dest_column, m, shared_data, any, string_pool, read_options);
}

[[nodiscard]] static inline PyObject* initialize_array(
        pybind11::dtype& descr, const shape_t* shape_ptr, const void* source_ptr
) {
    descr.inc_ref();
    auto& api = pybind11::detail::npy_api::get();
    constexpr int ndim = 1;
    constexpr int flags = 0;
    return api.PyArray_NewFromDescr_(
            api.PyArray_Type_,
            descr.ptr(),
            ndim,
            reinterpret_cast<const Py_intptr_t*>(shape_ptr),
            nullptr,
            const_cast<void*>(source_ptr),
            flags,
            nullptr
    );
}

void PythonArrayHandler::convert_type(
    const Column& source_column,
    Column& dest_column,
    const ColumnMapping& mapping,
    const arcticdb::DecodePathData&,
    std::any& any,
    const std::shared_ptr<StringPool>&,
    const ReadOptions&) const { //TODO we don't handle string arrays at the moment
    auto* ptr_dest =
            dest_column.ptr_cast<PyObject*>(mapping.offset_bytes_ / type_size(), mapping.num_rows_ * type_size());
    ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
    py::gil_scoped_acquire acquire_gil;
    const auto& sparse_map = source_column.opt_sparse_map();
    const auto strides = static_cast<stride_t>(get_type_size(mapping.source_type_desc_.data_type()));
    py::dtype py_dtype = generate_python_dtype(mapping.source_type_desc_, strides);

    if (source_column.empty())
        return;

    auto column_data = source_column.data();
    if (source_column.is_sparse()) {
        auto& handler_data = cast_handler_data(any);
        python_util::prefill_with_none(ptr_dest, mapping.num_rows_, source_column.sparse_map().count(), handler_data);

        auto en = sparse_map->first();

        column_data.type().visit_tag([&en, &column_data, &py_dtype, &dest_column](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            while (auto block = column_data.next<TDT>()) {
                auto block_pos = 0U;
                for (auto i = 0U; i < block->row_count(); ++i) {
                    *dest_column.ptr_cast<PyObject*>(*en, sizeof(PyObject)) =
                            initialize_array(py_dtype, block->shapes() + i, block->data() + block_pos);
                    block_pos += block->shapes()[i];
                    ++en;
                }
            }
        });
    } else {
        column_data.type().visit_tag([&ptr_dest, &column_data, &py_dtype](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            while (auto block = column_data.next<TDT>()) {
                auto block_pos = 0U;
                for (auto i = 0U; i < block->row_count(); ++i) {
                    *ptr_dest++ = initialize_array(py_dtype, block->shapes() + i, block->data() + block_pos);
                    block_pos += block->shapes()[i];
                }
            }
        });
    }
    dest_column.set_extra_buffer(
            mapping.offset_bytes_, ExtraBufferType::ARRAY, std::move(source_column.data().buffer())
    );
}

std::pair<TypeDescriptor, size_t> PythonArrayHandler::
        output_type_and_extra_bytes(const TypeDescriptor& input_type, std::string_view, const ReadOptions&) const {
    return {input_type, 0};
}

int PythonArrayHandler::type_size() const { return sizeof(PyObject*); }

void PythonArrayHandler::default_initialize(
        ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData&, std::any& any
) const {
    auto& handler_data = cast_handler_data(any);
    fill_with_none(buffer, offset, byte_size / type_size(), handler_data);
}

} // namespace arcticdb