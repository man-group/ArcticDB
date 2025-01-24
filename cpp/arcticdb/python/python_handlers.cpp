/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <python/python_handlers.hpp>
#include <arcticdb/codec/slice_data_sink.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/buffer_holder.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/sparse_utils.hpp>
#include <arcticdb/python/python_strings.hpp>
#include <arcticdb/python/python_utils.hpp>

namespace arcticdb {

/// @brief Generate numpy.dtype object from ArcticDB type descriptor
/// The dtype is used as type specifier for numpy arrays stored as column elements
/// @note There is special handling for ArcticDB's empty type
/// When numpy creates an empty array its type is float64. We want to mimic this because:
/// i) There is no equivalent to empty value
/// ii) We want input dataframes to be exact match of the output and that includes the type


static inline PyObject** fill_with_none(PyObject** ptr_dest, size_t count, SpinLock& spin_lock) {
    auto none = GilSafePyNone::instance();
    for(auto i = 0U; i < count; ++i)
        *ptr_dest++ = none->ptr();

    spin_lock.lock();
    for(auto i = 0U; i < count; ++i)
        Py_INCREF(none->ptr());
    spin_lock.unlock();
    return ptr_dest;
}

static inline PyObject** fill_with_none(ChunkedBuffer& buffer, size_t offset, size_t count, SpinLock& spin_lock) {
    auto dest = buffer.ptr_cast<PyObject*>(offset, count * sizeof(PyObject*));
    return fill_with_none(dest, count, spin_lock);
}

void EmptyHandler::handle_type(
        const uint8_t *& input,
        ChunkedBuffer& dest_buffer,
        const EncodedFieldImpl &field,
        const ColumnMapping& mapping,
        const DecodePathData&,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>&) {
    ARCTICDB_SAMPLE(HandleEmpty, 0)
    ARCTICDB_TRACE(log::version(), "Empty type handler invoked for source type: {}, destination type: {}, num rows: {}", mapping.source_type_desc_, mapping.dest_type_desc_,mapping.num_rows_);
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject *));

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
    convert_type(
        {},
        dest_buffer,
        mapping.num_rows_,
        mapping.offset_bytes_,
        mapping.source_type_desc_,
        mapping.dest_type_desc_, {}, handler_data, {});
}

void EmptyHandler::convert_type(
        const Column&,
        ChunkedBuffer& dest_buffer,
        size_t num_rows,
        size_t offset_bytes,
        TypeDescriptor source_type_desc ARCTICDB_UNUSED,
        TypeDescriptor dest_type_desc,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>&) {
    auto dest_data = dest_buffer.data() + offset_bytes;
    util::check(dest_data != nullptr, "Got null destination pointer");
    ARCTICDB_TRACE(
        log::version(),
        "Empty type handler invoked for source type: {}, destination type: {}, num rows: {}",
        source_type_desc,
        dest_type_desc,
        num_rows
    );
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject*));

    if(is_object_type(dest_type_desc) || is_empty_type(dest_type_desc.data_type())) {
        default_initialize(dest_buffer, offset_bytes, num_rows * type_size(), shared_data, handler_data);
    } else {
        dest_type_desc.visit_tag([num_rows, dest_data] (const auto tdt) {
            using TagType = decltype(tdt);
            using RawType = typename TagType::DataTypeTag::raw_type;
            const auto dest_bytes = num_rows * sizeof(RawType);
            util::default_initialize<TagType>(dest_data, dest_bytes);
        });
    }
}

int EmptyHandler::type_size() const {
    return sizeof(PyObject *);
}

void EmptyHandler::default_initialize(ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any) const {
    auto& handler_data = get_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data.spin_lock());
}

void BoolHandler::handle_type(
        const uint8_t *&data,
        ChunkedBuffer& dest_buffer,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>&) {
    ARCTICDB_SAMPLE(HandleBool, 0)
    util::check(field.has_ndarray(), "Bool handler expected array");
    ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", field.DebugString());
    const auto &ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
    Column decoded_data = Column(m.source_type_desc_, bytes / get_type_size(m.source_type_desc_.data_type()), AllocationType::DYNAMIC, Sparsity::PERMITTED);
    data += decode_field(m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version);

    convert_type(
        decoded_data,
        dest_buffer,
        m.num_rows_,
        m.offset_bytes_,
        m.source_type_desc_,
        m.dest_type_desc_,
        shared_data,
        handler_data,
        {});
}

void BoolHandler::convert_type(
        const Column& source_column,
        ChunkedBuffer& dest_buffer,
        size_t num_rows,
        size_t offset_bytes,
        arcticdb::entity::TypeDescriptor,
        arcticdb::entity::TypeDescriptor,
        const arcticdb::DecodePathData &,
        std::any& any,
        const std::shared_ptr<StringPool> &) {
    const auto& sparse_map = source_column.opt_sparse_map();
    const auto num_bools = sparse_map.has_value() ? sparse_map->count() : num_rows;
    auto ptr_src = source_column.template ptr_cast<uint8_t>(0, num_bools * sizeof(uint8_t));
    auto dest_data = dest_buffer.data() + offset_bytes;
    util::check(dest_data != nullptr, "Got null destination pointer");
    auto ptr_dest = reinterpret_cast<PyObject**>(dest_data);
    if (sparse_map.has_value()) {
        auto& handler_data = get_handler_data(any);
        ARCTICDB_TRACE(log::codec(), "Bool handler using a sparse map");
        unsigned last_row = 0u;
        for (auto en = sparse_map->first(); en < sparse_map->end(); ++en, ++last_row) {
            const auto current_pos = *en;
            ptr_dest = fill_with_none(ptr_dest, current_pos - last_row, handler_data.spin_lock());
            last_row = current_pos;
            *ptr_dest++ = py::bool_(static_cast<bool>(*ptr_src++)).release().ptr();
        }
        fill_with_none(ptr_dest, num_rows - last_row, handler_data.spin_lock());
    } else {
        ARCTICDB_TRACE(log::codec(), "Bool handler didn't find a sparse map. Assuming dense array.");
        std::transform(ptr_src, ptr_src + num_bools, ptr_dest, [](uint8_t value) {
            return py::bool_(static_cast<bool>(value)).release().ptr();
        });
    }
}

int BoolHandler::type_size() const {
    return sizeof(PyObject *);
}

void BoolHandler::default_initialize(ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any) const {
    auto& handler_data = get_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data.spin_lock());
}

void StringHandler::handle_type(
        const uint8_t *&data,
        ChunkedBuffer& dest_buffer,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool) {
    ARCTICDB_SAMPLE(HandleString, 0)
    util::check(field.has_ndarray(), "String handler expected array");
    ARCTICDB_DEBUG(log::version(), "String handler got encoded field: {}", field.DebugString());
    const auto &ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);

    auto decoded_data = [&m, &ndarray, bytes, &dest_buffer]() {
        if(ndarray.sparse_map_bytes() > 0) {
            return Column(m.source_type_desc_, bytes / get_type_size(m.source_type_desc_.data_type()), AllocationType::DYNAMIC, Sparsity::PERMITTED);
        } else {
            Column column(m.source_type_desc_, Sparsity::NOT_PERMITTED);
            column.buffer().add_external_block(&dest_buffer[m.offset_bytes_], bytes, 0UL);
            return column;
        }
    }();

    data += decode_field(m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version);

    if(is_dynamic_string_type(m.dest_type_desc_.data_type())) {
        convert_type(
            decoded_data,
            dest_buffer,
            m.num_rows_,
            m.offset_bytes_,
            m.source_type_desc_,
            m.dest_type_desc_,
            shared_data,
            handler_data,
            string_pool);
    }
}

void StringHandler::convert_type(
        const Column& source_column,
        ChunkedBuffer& dest_buffer,
        size_t num_rows,
        size_t offset_bytes,
        TypeDescriptor source_type_desc,
        TypeDescriptor dest_type_desc,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>& string_pool) {
    auto dest_data = &dest_buffer[offset_bytes];
    auto ptr_dest = reinterpret_cast<PyObject**>(dest_data);
    DynamicStringReducer string_reducer{shared_data, get_handler_data(handler_data), ptr_dest, num_rows};
    string_reducer.reduce(source_column, source_type_desc, dest_type_desc, num_rows, *string_pool, source_column.opt_sparse_map());
    string_reducer.finalize();
}

int StringHandler::type_size() const {
    return sizeof(PyObject *);
}

void StringHandler::default_initialize(ChunkedBuffer& buffer, size_t bytes_offset, size_t byte_size, const DecodePathData&, std::any& any) const {
    auto& handler_data = get_handler_data(any);
    fill_with_none(buffer, bytes_offset, byte_size / type_size(), handler_data.spin_lock());
}

[[nodiscard]] static inline py::dtype generate_python_dtype(const TypeDescriptor &td, stride_t type_byte_size) {
    if (is_empty_type(td.data_type())) {
        return py::dtype{"f8"};
    }
    return py::dtype{fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), type_byte_size)};
}

void ArrayHandler::handle_type(
    const uint8_t *&data,
    ChunkedBuffer& dest_buffer,
    const EncodedFieldImpl &field,
    const ColumnMapping& m,
    const DecodePathData& shared_data,
    std::any& any,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>&
) {
    ARCTICDB_SAMPLE(HandleArray, 0)
    util::check(field.has_ndarray(), "Expected ndarray in array object handler");
    std::shared_ptr<Column> column = shared_data.buffers()->get_buffer(m.source_type_desc_, Sparsity::PERMITTED);
    column->check_magic();
    ARCTICDB_DEBUG(log::version(), "Column got buffer at {}", uintptr_t(column.get()));
    data += decode_field(m.source_type_desc_, field, data, *column, column->opt_sparse_map(), encoding_version);

    convert_type(*column, dest_buffer, m.num_rows_, m.offset_bytes_, m.source_type_desc_, m.dest_type_desc_, shared_data, any, {});
}

[[nodiscard]] static inline PyObject* initialize_array(
    pybind11::dtype &descr,
    const shape_t* shape_ptr,
    const void *source_ptr
) {
    descr.inc_ref();
    auto &api = pybind11::detail::npy_api::get();
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

void ArrayHandler::convert_type(
    const Column& source_column,
    ChunkedBuffer& dest_buffer,
    size_t num_rows,
    size_t offset_bytes,
    arcticdb::entity::TypeDescriptor source_type_desc,
    arcticdb::entity::TypeDescriptor,
    const arcticdb::DecodePathData&,
    std::any& any,
    const std::shared_ptr<StringPool> &) { //TODO we don't handle string arrays at the moment
    auto* ptr_dest = dest_buffer.ptr_cast<PyObject *>(offset_bytes / type_size(), num_rows * type_size());
    ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
    py::gil_scoped_acquire acquire_gil;
    const auto &sparse_map = source_column.opt_sparse_map();
    const auto strides = static_cast<stride_t>(get_type_size(source_type_desc.data_type()));
    py::dtype py_dtype = generate_python_dtype(source_type_desc, strides);

    if (source_column.empty())
        return;

    auto column_data = source_column.data();
    if (source_column.is_sparse()) {
        auto& handler_data = get_handler_data(any);
        python_util::prefill_with_none(ptr_dest, num_rows, source_column.sparse_map().count(), handler_data.spin_lock());

        auto en = sparse_map->first();

        column_data.type().visit_tag([&en, &column_data, &py_dtype, &dest_buffer](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            while (auto block = column_data.next<TDT>()) {
                auto block_pos = 0U;
                for (auto i = 0U; i < block->row_count(); ++i) {
                    dest_buffer.cast<PyObject *>(*en) = initialize_array(
                        py_dtype,
                        block->shapes() + i,
                        block->data() + block_pos
                    );
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
                    *ptr_dest++ = initialize_array(
                        py_dtype,
                        block->shapes() + i,
                        block->data() + block_pos
                    );
                    block_pos += block->shapes()[i];
                }
            }
        });
    }
}

int ArrayHandler::type_size() const {
    return sizeof(PyObject *);
}

void ArrayHandler::default_initialize(ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData&, std::any& any) const {
    auto& handler_data = get_handler_data(any);
    fill_with_none(buffer, offset, byte_size / type_size(), handler_data.spin_lock());
}

} //namespace arcticdb