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

namespace arcticdb {

/// @brief Generate numpy.dtype object from ArcticDB type descriptor
/// The dtype is used as type specifier for numpy arrays stored as column elements
/// @note There is special handling for ArcticDB's empty type
/// When numpy creates an empty array its type is float64. We want to mimic this because:
/// i) There is no equivalent to empty value
/// ii) We want input dataframes to be exact match of the output and that includes the type
[[nodiscard]] static inline py::dtype generate_python_dtype(const TypeDescriptor &td, stride_t type_byte_size) {
    if (is_empty_type(td.data_type())) {
        return py::dtype{"f8"};
    }
    return py::dtype{fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), type_byte_size)};
}

/// @important This calls pybind's initialize array function which is NOT thread safe. Moreover, numpy arrays can
/// be created only by the thread holding the GIL. In practice we can get away with allocating arrays only from
/// a single thread (even if it's not the one holding the GIL). This, however, is not guaranteed to work.
/// @todo Allocate numpy arrays only from the thread holding the GIL
[[nodiscard]] static inline PyObject* initialize_array(
    const pybind11::dtype &descr,
    const shape_t shapes,
    const stride_t strides,
    const void *source_ptr,
    std::shared_ptr<Column> owner,
    std::mutex &creation_mutex
) {
    std::lock_guard creation_guard{creation_mutex};
    // TODO: Py capsule can take only void ptr as input. We need a better way to handle destruction
    //  Allocating shared ptr on the heap is sad.
    auto *object = new std::shared_ptr<Column>(std::move(owner));
    auto arr = py::array(descr, {shapes}, {strides}, source_ptr, py::capsule(object, [](void *obj) {
        delete reinterpret_cast<std::shared_ptr<Column> *>(obj);
    }));
    return arr.release().ptr();
}

static inline const PyObject** fill_with_none(const PyObject** dest, size_t count) {
    auto none = py::none();
    std::generate_n(dest, count, [&none]() { return none.inc_ref().ptr(); });
    return dest + count;
}

void EmptyHandler::handle_type(
    const uint8_t *&input,
    uint8_t *dest,
    const EncodedFieldImpl& field,
    const ColumnMapping& mapping,
    size_t dest_bytes,
    const DecodePathData&,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>&
) {
    ARCTICDB_SAMPLE(HandleEmpty, 0)
    util::check(dest != nullptr, "Got null destination pointer");
    ARCTICDB_TRACE(
        log::version(),
        "Empty type handler invoked for source type: {}, destination type: {}, num rows: {}",
        mapping.source_type_desc_,
        mapping.dest_type_desc_,
        mapping.num_rows_
    );
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject *));

    if (encoding_version == EncodingVersion::V2)
        util::check_magic<ColumnMagic>(input);

   // const auto num_rows = dest_bytes / get_type_size(DataType::EMPTYVAL);
  //  auto* target = reinterpret_cast<const PyObject**>(dest);

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
    mapping.dest_type_desc_.visit_tag([dest, dest_bytes] (auto tdt) {
        using DataType = decltype(tdt);
        util::default_initialize<DataType>(dest, dest_bytes);
    });
}

int EmptyHandler::type_size() const {
    return sizeof(PyObject *);
}

void EmptyHandler::default_initialize(void *dest, size_t byte_size) const {
    fill_with_none(reinterpret_cast<const PyObject**>(dest), byte_size / type_size());
}

void BoolHandler::handle_type(
    const uint8_t *&data,
    uint8_t *dest,
    const EncodedFieldImpl &field,
    const ColumnMapping& m,
    size_t,
    const DecodePathData&,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>& ) {
    ARCTICDB_SAMPLE(HandleBool, 0)
    util::check(dest != nullptr, "Got null destination pointer");
    util::check(field.has_ndarray(), "Bool handler expected array");
    ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", field.DebugString());
    auto ptr_dest = reinterpret_cast<const PyObject**>(dest);
    const auto &ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
    ChunkedBuffer decoded_data = ChunkedBuffer::presized(bytes);
    SliceDataSink decoded_data_sink{decoded_data.data(), bytes};
    std::optional<util::BitSet> sparse_map;
    data += decode_field(m.source_type_desc_, field, data, decoded_data_sink, sparse_map, encoding_version);
    const auto num_bools = sparse_map.has_value() ? sparse_map->count() : m.num_rows_;
    auto ptr_src = decoded_data.template ptr_cast<uint8_t>(0, num_bools * sizeof(uint8_t));
    if (sparse_map.has_value()) {
        ARCTICDB_TRACE(log::codec(), "Bool handler using a sparse map");
        unsigned last_row = 0u;
        for (auto en = sparse_map->first(); en < sparse_map->end(); ++en, last_row++) {
            const auto current_pos = *en;
            ptr_dest = fill_with_none(ptr_dest, current_pos - last_row);
            last_row = current_pos;
            *ptr_dest++ = py::bool_(static_cast<bool>(*ptr_src++)).release().ptr();
        }
        fill_with_none(ptr_dest, m.num_rows_ - last_row);
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

void BoolHandler::default_initialize(void *dest, size_t byte_size) const {
    fill_with_none(reinterpret_cast<const PyObject**>(dest), byte_size / type_size());
}

void StringHandler::handle_type(
    const uint8_t *&data,
    uint8_t *dest,
    const EncodedFieldImpl &field,
    const ColumnMapping& m,
    size_t,
    const DecodePathData& shared_data,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>& string_pool) {
    ARCTICDB_SAMPLE(HandleString, 0)
    util::check(dest != nullptr, "Got null destination pointer in string handler");
    util::check(field.has_ndarray(), "String handler expected array");
    ARCTICDB_DEBUG(log::version(), "String handler got encoded field: {}", field.DebugString());
    const auto &ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
    ChunkedBuffer decoded_data = ChunkedBuffer::presized(bytes);
    SliceDataSink decoded_data_sink{decoded_data.data(), bytes};
    std::optional<util::BitSet> sparse_map;
    data += decode_field(m.source_type_desc_, field, data, decoded_data_sink, sparse_map, encoding_version);
    auto ptr_dest = reinterpret_cast<PyObject**>(dest);
    DynamicStringReducer string_reducer{shared_data, ptr_dest, m.num_rows_};
    if (sparse_map.has_value()) {
        ARCTICDB_TRACE(log::codec(), "String handler using a sparse map");
       // unsigned last_row = 0u;
       // for (auto en = sparse_map->first(); en < sparse_map->end(); ++en, last_row++) {

        //}
        //fill_with_none(ptr_dest, m.num_rows_ - last_row);
        util::raise_rte("Sparse not handled");
    } else {
        ARCTICDB_TRACE(log::codec(), "String handler didn't find a sparse map. Assuming dense array.");
        string_reducer.reduce(m.source_type_desc_, m.dest_type_desc_, m.num_rows_, *string_pool, reinterpret_cast<const position_t*>(decoded_data.data()));
    }
    string_reducer.finalize();
}

int StringHandler::type_size() const {
    return sizeof(PyObject *);
}

void StringHandler::default_initialize(void *dest, size_t byte_size) const {
    fill_with_none(reinterpret_cast<const PyObject**>(dest), byte_size / type_size());
}

std::mutex ArrayHandler::initialize_array_mutex;

void ArrayHandler::handle_type(
    const uint8_t *&data,
    uint8_t *dest,
    const EncodedFieldImpl &field,
    const ColumnMapping& m,
    size_t,
    const DecodePathData& shared_data,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>&
) {
    ARCTICDB_SAMPLE(HandleArray, 0)
    util::check(field.has_ndarray(), "Expected ndarray in array object handler");

    auto ptr_dest = reinterpret_cast<const PyObject**>(dest);
    if (!field.ndarray().sparse_map_bytes()) {
        ARCTICDB_DEBUG(log::version(), "Array handler has no values");
        fill_with_none(ptr_dest, m.num_rows_);
        return;
    }
    std::shared_ptr<Column> column = shared_data.buffers()->get_buffer(m.source_type_desc_, true);
    column->check_magic();
    ARCTICDB_DEBUG(log::version(), "Column got buffer at {}", uintptr_t(column.get()));
    auto bv = std::make_optional(util::BitSet{});
    data += decode_field(m.source_type_desc_, field, data, *column, bv, encoding_version);

    auto last_row = 0u;
    ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
    const auto strides = static_cast<stride_t>(get_type_size(m.source_type_desc_.data_type()));
    const py::dtype py_dtype = generate_python_dtype(m.source_type_desc_, strides);
    m.source_type_desc_.visit_tag([&](auto tdt) {
        const auto &blocks = column->blocks();
        if (blocks.empty())
            return;

        auto block_it = blocks.begin();
        const auto *shapes = column->shape_ptr();
        auto block_pos = 0u;
        const auto *ptr_src = (*block_it)->data();
        constexpr stride_t stride = static_cast<TypeDescriptor>(tdt).get_type_byte_size();
        for (auto en = bv->first(); en < bv->end(); ++en) {
            const shape_t shape = shapes ? *shapes : 0;
            const auto offset = *en;
            ptr_dest = fill_with_none(ptr_dest, offset - last_row);
            last_row = offset;
            *ptr_dest++ = initialize_array(py_dtype,
                                           shape,
                                           stride,
                                           ptr_src + block_pos,
                                           column,
                                           initialize_array_mutex);
            block_pos += shape * stride;
            if (shapes) {
                ++shapes;
            }
            if (block_it != blocks.end() && block_pos == (*block_it)->bytes() && ++block_it != blocks.end()) {
                ptr_src = (*block_it)->data();
                block_pos = 0;
            }

            ++last_row;
        }
        if (block_it != blocks.end() && block_pos == (*block_it)->bytes() && ++block_it != blocks.end()) {
            ptr_src = (*block_it)->data();
            block_pos = 0;
        }

        ++last_row;
    });

    ARCTICDB_SUBSAMPLE(ArrayIncNones, 0)
    fill_with_none(ptr_dest, m.num_rows_ - last_row);
}

int ArrayHandler::type_size() const {
    return sizeof(PyObject *);
}

void ArrayHandler::default_initialize(void *dest, size_t byte_size) const {
    fill_with_none(reinterpret_cast<const PyObject**>(dest), byte_size / type_size());
}

} //namespace arcticdb