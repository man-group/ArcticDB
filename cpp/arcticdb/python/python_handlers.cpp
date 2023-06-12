#include <python/python_handlers.hpp>
#include <python/pybind11.hpp>
#include <proto/encoding.pb.h>
#include <python/python_handlers.hpp>
#include <core/types.hpp>
#include <column_store/chunked_buffer.hpp>
#include <util/bitset.hpp>
#include <codec/encoding_sizes.hpp>
#include <codec/slice_data_sink.hpp>
#include <codec/codec.hpp>
#include <python/python_to_tensor_frame.hpp>
#include <pybind11/numpy.h>
#include <core/performance_tracing.hpp>
#include <python/gil_lock.hpp>
#include <util/buffer_holder.hpp>

namespace arcticdb {

namespace py = pybind11;

void DecimalHandler::handle_type(
    const uint8_t*& data,
    uint8_t* dest,
    const pb2::encoding_pb2::EncodedField& encoded_field_info,
    const entity::TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder>
    ) {
    ARCTICDB_SAMPLE(HandleDecimals, 0)
    util::check(dest != nullptr, "Got null destination pointer");
    util::check(encoded_field_info.has_ndarray(), "Decimal handler expected array");
    auto none = py::none{};
    size_t num_rows = dest_bytes / get_type_size(DataType::DECIMAL64);
    auto ptr_dest = reinterpret_cast<PyObject**>(dest);
    if(!encoded_field_info.ndarray().sparse_map_bytes()) {
        log::version().debug("Decimal handler has no values");
        for(auto r = 0u; r < num_rows; ++r)
            *ptr_dest++ = none.inc_ref().ptr();

        return;
    }

    std::optional<util::BitSet> bv;
    const auto& ndarray = encoded_field_info.ndarray();
    const auto bytes = pb_enc_sizes::data_uncompressed_size(ndarray);
    auto sparse = ChunkedBuffer::presized(bytes);
    SliceDataSink sparse_sink{sparse.data(), bytes};
    data += decode(type_descriptor, encoded_field_info, data, sparse_sink, bv);
    const auto num_decimals = bv->count();
    auto ptr_src = sparse.ptr_cast<Decimal>(0, num_decimals * sizeof(Decimal));
    auto last_row = 0u;
    const auto ptr_begin = ptr_dest;

    auto en = bv->first();
    const auto en_end = bv->end();
    ARCTICDB_SUBSAMPLE(DecimalAcquireGIL, 0)
    ScopedGILLock lock;
    while(en < en_end) {
        const auto current_pos = *en;
        for(; last_row < current_pos; ++last_row) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
        }

        ARCTICDB_SUBSAMPLE(DecimalConvert, 0)
        auto str = decimal_to_string(ptr_src++);
        auto decimal = convert::decimal_from_string(*decimal_, std::move(str));
        *ptr_dest++ = decimal.release().ptr();
        ++last_row;
        ++en;
        util::debug_check(ptr_begin + last_row == ptr_dest, "Decimal pointer out of alignment {} != {}", last_row, uintptr_t(ptr_begin + last_row));
    }
    ARCTICDB_SUBSAMPLE(DecimalIncrefNone, 0)
    for(; last_row < num_rows; ++last_row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }
    util::check(ptr_begin + last_row == ptr_dest, "Decimal pointer out of alignment at end: {} != {}", last_row, uintptr_t(ptr_begin + last_row));
}
PyObject* initialize_array_old(pybind11::dtype &dt, const shape_t* shapes, const shape_t* strides, size_t ndim, const void* ptr) {
    ARCTICDB_SAMPLE(InitializeArray, 0)
    util::check(ptr != nullptr, "Null pointer passed in");

    auto descr = dt;
    auto flags = py::detail::npy_api::NPY_ARRAY_WRITEABLE_;

    auto &api = py::detail::npy_api::get();

    ARCTICDB_SUBSAMPLE(InitArrayCreateArray, 0)
    auto tmp = py::reinterpret_steal<py::object>(api.PyArray_NewFromDescr_(
        api.PyArray_Type_,
        descr.release().ptr(),
        static_cast<int>(ndim),
        reinterpret_cast<Py_intptr_t*>(const_cast<shape_t*>(shapes)),
        reinterpret_cast<Py_intptr_t*>(const_cast<shape_t*>(strides)),
        const_cast<void *>(ptr), flags, nullptr));

    ARCTICDB_SUBSAMPLE(ArrayCopy, 0)
    tmp = py::reinterpret_steal<py::object>(api.PyArray_NewCopy_(tmp.ptr(), -1));
    util::check(static_cast<bool>(tmp), "Got null pointer in array allocation");
    return tmp.release().ptr();
}

PyObject* initialize_array(pybind11::dtype &dt, const shape_t* shapes, const shape_t* strides, size_t ndim, const void* ptr) {
        ARCTICDB_SAMPLE(InitializeArray, 0)
        util::check(ptr != nullptr, "Null pointer passed in");

        auto descr = dt;
        auto flags = py::detail::npy_api::NPY_ARRAY_WRITEABLE_;

        auto &api = py::detail::npy_api::get();

        ARCTICDB_SUBSAMPLE(InitArrayCreateArray, 0)
        auto tmp = py::reinterpret_steal<py::object>(api.PyArray_NewFromDescr_(
            api.PyArray_Type_,
            descr.release().ptr(),
            static_cast<int>(ndim),
            reinterpret_cast<Py_intptr_t*>(const_cast<shape_t*>(shapes)),
            reinterpret_cast<Py_intptr_t*>(const_cast<shape_t*>(strides)),
            const_cast<void *>(ptr), flags, nullptr));

        ARCTICDB_SUBSAMPLE(ArrayCopy, 0)
        tmp = py::reinterpret_steal<py::object>(api.PyArray_NewCopy_(tmp.ptr(), -1));
        util::check(static_cast<bool>(tmp), "Got null pointer in array allocation");
        return tmp.release().ptr();
    }

struct DtypeCache {
    std::unordered_map<std::pair<DataType, shape_t>, py::dtype> cache_;

    py::dtype get_dtype(const TypeDescriptor& td, shape_t strides) {
        ARCTICDB_SAMPLE(ArrayDtype, 0)

        const auto spec = std::make_pair(td.data_type(), strides);
        if(auto it = cache_.find(spec); it != cache_.end())
            return it->second;

        auto specifier = fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), strides);
        ScopedGILLock lock;
        py::dtype py_dtype{specifier};
        cache_.try_emplace(spec, py_dtype);
        return py_dtype;
    }
};

void ArrayHandlerOld::handle_type(
    const uint8_t*& data,
    uint8_t* dest,
    const pb2::encoding_pb2::EncodedField& encoded_field,
    const entity::TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers ARCTICDB_UNUSED
    ){
    ARCTICDB_SAMPLE(HandleArray, 0)
    util::check(encoded_field.has_ndarray(), "Expected ndarray in array object handler");
    auto none = py::none();
    const auto row_count = dest_bytes / sizeof(PyObject*);
    util::check(row_count >= encoded_field.ndarray().items_count(),
                "Item count mismatch: {} < {}", row_count, encoded_field.ndarray().items_count());

    auto ptr_dest = reinterpret_cast<PyObject**>(dest);
    if(!encoded_field.ndarray().sparse_map_bytes()) {
        log::version().info("Array handler has no values");
        for(auto r = 0u; r < row_count; ++r)
            ptr_dest[r] = none.inc_ref().ptr();

        return;
    }
    util::check(encoded_field.ndarray().has_arr_desc(), "Array type descriptor required in array object handler");

    auto bv = std::make_optional(util::BitSet{});
    const auto td = type_desc_from_proto(encoded_field.ndarray().arr_desc());
    Column data_sink(td, true);
    data += decode(type_descriptor, encoded_field, data, data_sink, bv);

    auto en = bv->first();
    auto en_end = bv->end();
    auto last_row = 0u;
    auto src_pos = 0u;
    ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
    ScopedGILLock gil;
    DtypeCache dtypes;
    while(en < en_end) {
        const auto offset = *en;
        while(last_row < offset) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
            ++last_row;
        }


        shape_t strides = get_type_size(td.data_type());
        auto py_dtype = dtypes.get_dtype(td, strides);
        td.visit_tag([&ptr_dest, &data_sink, &src_pos, &py_dtype, &td] (auto tdt) {
            using RawType = typename decltype(tdt)::DataTypeTag::raw_type;
            auto tensor = data_sink.tensor_at<RawType>(src_pos);
            auto arr_ptr = initialize_array_old(py_dtype, tensor->shapes_.data(), tensor->strides_.data(),
                                            static_cast<size_t>(td.dimension()), tensor->ptr);
            *ptr_dest++ = arr_ptr;
        });

        ++last_row;
        ++src_pos;
        ++en;
    }

    ARCTICDB_SUBSAMPLE(ArrayIncNones, 0)
    for(; last_row < row_count; ++last_row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }
}

void ArrayHandler::handle_type(
    const uint8_t*& data,
    uint8_t* dest,
    const pb2::encoding_pb2::EncodedField& encoded_field,
    const entity::TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers
    ){
    ARCTICDB_SAMPLE(HandleArray, 0)
    util::check(encoded_field.has_ndarray(), "Expected ndarray in array object handler");
    auto none = py::none();
    const auto row_count = dest_bytes / sizeof(PyObject*);
    util::check(row_count >= encoded_field.ndarray().items_count(),
                "Item count mismatch: {} < {}", row_count, encoded_field.ndarray().items_count());

    auto ptr_dest = reinterpret_cast<PyObject**>(dest);
    if(!encoded_field.ndarray().sparse_map_bytes()) {
        log::version().info("Array handler has no values");
        for(auto r = 0u; r < row_count; ++r)
           ptr_dest[r] = none.inc_ref().ptr();

       return;
    }
    util::check(encoded_field.ndarray().has_arr_desc(), "Array type descriptor required in array object handler");

    auto bv = std::make_optional(util::BitSet{});
    const auto td = type_desc_from_proto(encoded_field.ndarray().arr_desc());
    auto data_sink = buffers->get_buffer(td, true);
    data_sink->check_magic();
    log::version().info("Column got buffer at {}", uintptr_t(data_sink.get()));
    data += decode(type_descriptor, encoded_field, data, *data_sink, bv);

    auto en = bv->first();
    auto en_end = bv->end();
    auto last_row = 0u;
    auto src_pos = 0u;
    ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
    ScopedGILLock gil;
    DtypeCache dtypes;
    shape_t strides = get_type_size(td.data_type());
    auto py_dtype = dtypes.get_dtype(td, strides);
    td.visit_tag([&en, &en_end, &last_row, &none, &ptr_dest, &data_sink, &src_pos, &py_dtype, &td] (auto tdt) {
        using RawType = typename decltype(tdt)::DataTypeTag::raw_type;
        const auto& blocks = data_sink->blocks();
        if(blocks.empty())
            return;

        auto block_it = blocks.begin();
        auto shape = data_sink->shape_ptr();
        auto block_pos = 0u;
        const auto* ptr_src = (*block_it)->data();
        const shape_t stride = sizeof(RawType);
        while(en < en_end) {
            const auto offset = *en;
            while(last_row < offset) {
                none.inc_ref();
                *ptr_dest++ = none.ptr();
                ++last_row;
            }

            auto arr_ptr = initialize_array(
                py_dtype,
                shape,
                &stride,
                static_cast<size_t>(td.dimension()),
                ptr_src + block_pos);

            *ptr_dest++ = arr_ptr;
            block_pos += *shape * stride;
            ++shape;
            if(block_pos == (*block_it)->bytes() && ++block_it != blocks.end()) {
                ptr_src = (*block_it)->data();
                block_pos = 0;
            }

        ++last_row;
        ++src_pos;
        ++en;
    }

    });
    ARCTICDB_SUBSAMPLE(ArrayIncNones, 0)
    for(; last_row < row_count; ++last_row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }
}

void EmptyHandler::handle_type(
    const uint8_t*& data ARCTICDB_UNUSED,
    uint8_t* dest,
    const pb2::encoding_pb2::EncodedField& encoded_field_info ARCTICDB_UNUSED,
    const entity::TypeDescriptor& type_descriptor ARCTICDB_UNUSED,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers ARCTICDB_UNUSED
    ) {
    ARCTICDB_SAMPLE(HandleEmpty, 0)
    util::check(dest != nullptr, "Got null destination pointer");
    auto none = py::none{};
    size_t num_rows = dest_bytes / get_type_size(DataType::PYBOOL64);
    auto ptr_dest = reinterpret_cast<PyObject**>(dest);
    ScopedGILLock lock;
    for(auto row = 0u; row < num_rows; ++row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }
}

void BoolHandler::handle_type(
    const uint8_t*& data,
    uint8_t* dest,
    const pb2::encoding_pb2::EncodedField& encoded_field_info,
    const entity::TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers ARCTICDB_UNUSED
    ) {
    ARCTICDB_SAMPLE(HandleBool, 0)
    util::check(dest != nullptr, "Got null destination pointer");
    util::check(encoded_field_info.has_ndarray(), "Bool handler expected array");
    ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", encoded_field_info.DebugString());
    auto none = py::none{};
    size_t num_rows = dest_bytes / get_type_size(DataType::PYBOOL64);
    auto ptr_dest = reinterpret_cast<PyObject**>(dest);

    if(!encoded_field_info.ndarray().sparse_map_bytes()) {
        ScopedGILLock lock;
        ARCTICDB_DEBUG(log::version(), "Bool handler has no values");
        for(auto r = 0u; r < num_rows; ++r)
            *ptr_dest++ = none.inc_ref().ptr();

        return;
    }

    std::optional<util::BitSet> bv;
    const auto& ndarray = encoded_field_info.ndarray();
    const auto bytes = pb_enc_sizes::data_uncompressed_size(ndarray);
    auto sparse = ChunkedBuffer::presized(bytes);
    SliceDataSink sparse_sink{sparse.data(), bytes};
    data += decode(type_descriptor, encoded_field_info, data, sparse_sink, bv);
    const auto num_decimals = bv->count();
    auto ptr_src = sparse.ptr_cast<uint8_t>(0, num_decimals * sizeof(uint8_t));
    auto last_row = 0u;
    const auto ptr_begin = ptr_dest;

    auto en = bv->first();
    const auto en_end = bv->end();

    ScopedGILLock lock;
    while(en < en_end) {
        const auto current_pos = *en;
        for(; last_row < current_pos; ++last_row) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
        }

        auto py_bool = py::bool_(static_cast<bool>(*ptr_src++));
        *ptr_dest++ = py_bool.release().ptr();
        ++last_row;
        ++en;
        util::debug_check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment {} != {}", last_row, uintptr_t(ptr_begin + last_row));
    }
    for(; last_row < num_rows; ++last_row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }
    util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment at end: {} != {}", last_row, uintptr_t(ptr_begin + last_row));
}

} //namespace arcticc
