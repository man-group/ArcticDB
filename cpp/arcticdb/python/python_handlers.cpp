/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <python/python_handlers.hpp>
#include <python/gil_lock.hpp>
#include <arcticdb/codec/slice_data_sink.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>

namespace arcticdb {

    struct DtypeCache {
        std::unordered_map<std::pair<DataType, shape_t>, py::dtype> cache_;

        py::dtype get_dtype(const TypeDescriptor& td, shape_t strides) {
            ARCTICDB_SAMPLE(ArrayDtype, 0)

            const auto spec = std::make_pair(td.data_type(), strides);
            if(auto it = cache_.find(spec); it != cache_.end())
                return it->second;

            auto specifier = fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), strides);
            // TODO: scoped lock led to a deadlock in the empty handler
            // ScopedGILLock lock;
            py::dtype py_dtype{specifier};
            cache_.try_emplace(spec, py_dtype);
            return py_dtype;
        }
    };

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


    void EmptyHandler::handle_type(
        const uint8_t *&input,
        uint8_t *dest,
        const VariantField &variant_field,
        const entity::TypeDescriptor &,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder>
    ) {
        ARCTICDB_SAMPLE(HandleEmpty, 0)
        util::check(dest != nullptr, "Got null destination pointer");
        const size_t num_rows = dest_bytes / get_type_size(DataType::EMPTYVAL);
        static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject*));
        auto ptr_dest = reinterpret_cast<const PyObject**>(dest);
        py::none none = {};
        for (auto row = 0u; row < num_rows; ++row) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
        }

        util::variant_match(variant_field, [&input](const auto& field) {
            using EncodedFieldType = std::decay_t<decltype(*field)>;
            if constexpr(std::is_same_v<EncodedFieldType, arcticdb::EncodedField>)
                check_magic<ColumnMagic>(input);

            if(field->encoding_case() == EncodedFieldType::kNdarray) {
                const auto& ndarray_field = field->ndarray();
                const auto num_blocks = ndarray_field.values_size();
                util::check(num_blocks <= 1, "Unexpected number of empty type blocks: {}", num_blocks);
                for (auto block_num = 0; block_num < num_blocks; ++block_num) {
                    const auto& block_info = ndarray_field.values(block_num);
                    input += block_info.out_bytes();
                }
            } else {
                util::raise_error_msg("Unsupported encoding {}", *field);
            }
        });
    }

    void BoolHandler::handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field_info,
            const entity::TypeDescriptor& type_descriptor,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder>
    ) {
        std::visit([&](const auto& field){
            ARCTICDB_SAMPLE(HandleBool, 0)
            util::check(dest != nullptr, "Got null destination pointer");
            util::check(field->has_ndarray(), "Bool handler expected array");
            ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", field->DebugString());
            auto none = py::none{};
            size_t num_rows = dest_bytes / get_type_size(DataType::PYBOOL64);
            auto ptr_dest = reinterpret_cast<PyObject**>(dest);

            if(!field->ndarray().sparse_map_bytes()) {
                // TODO: scoped lock led to a deadlock in the empty handler
                // ScopedGILLock lock;
                ARCTICDB_DEBUG(log::version(), "Bool handler has no values");
                for(auto r = 0u; r < num_rows; ++r)
                    *ptr_dest++ = none.inc_ref().ptr();

                return;
            }

            std::optional<util::BitSet> bv;
            const auto& ndarray = field->ndarray();
            const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
            auto sparse = ChunkedBuffer::presized(bytes);
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(type_descriptor, *field, data, sparse_sink, bv);
            const auto num_bools = bv->count();
            auto ptr_src = sparse.template ptr_cast<uint8_t>(0, num_bools * sizeof(uint8_t));
            auto last_row = 0u;
            const auto ptr_begin = ptr_dest;

            auto en = bv->first();
            const auto en_end = bv->end();

            // TODO: scoped lock led to a deadlock in the empty handler
            // ScopedGILLock lock;
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
                util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment {} != {}", last_row, uintptr_t(ptr_begin + last_row));
            }
            for(; last_row < num_rows; ++last_row) {
                none.inc_ref();
                *ptr_dest++ = none.ptr();
            }
            util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment at end: {} != {}", last_row, uintptr_t(ptr_begin + last_row));
        }, encoded_field_info);
    }

    void ArrayHandler::handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field_info,
            const entity::TypeDescriptor& type_descriptor,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers
    ){
        util::variant_match(encoded_field_info, [&](auto field){
            ARCTICDB_SAMPLE(HandleArray, 0)
            util::check(field->has_ndarray(), "Expected ndarray in array object handler");
            auto none = py::none();
            const auto row_count = dest_bytes / sizeof(PyObject*);
            util::check(row_count >= field->ndarray().items_count(),
                        "Item count mismatch: {} < {}", row_count, field->ndarray().items_count());

            auto ptr_dest = reinterpret_cast<PyObject**>(dest);
            if(!field->ndarray().sparse_map_bytes()) {
                log::version().info("Array handler has no values");
                for(auto r = 0u; r < row_count; ++r)
                    ptr_dest[r] = none.inc_ref().ptr();

                return;
            }
            util::check(field->ndarray().has_arr_desc(), "Array type descriptor required in array object handler");

            auto bv = std::make_optional(util::BitSet{});
            const auto td = [](auto field){
                if constexpr(std::is_same_v<decltype(field), const EncodedField*>) {
                    return field->ndarray().arr_desc();
                } else if constexpr(std::is_same_v<decltype(field), const arcticdb::proto::encoding::EncodedField*>) {
                    return type_desc_from_proto(field->ndarray().arr_desc());
                } else {
                    static_assert(sizeof(field) == 0, "Unexpected encoded field type");
                }
            }(field);
            auto data_sink = buffers->get_buffer(td, true);
            data_sink->check_magic();
            log::version().info("Column got buffer at {}", uintptr_t(data_sink.get()));
            data += decode_field(type_descriptor, *field, data, *data_sink, bv);

            auto en = bv->first();
            auto en_end = bv->end();
            auto last_row = 0u;
            auto src_pos = 0u;
            ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
            // TODO: scoped lock led to a deadlock in the empty handler
            // ScopedGILLock lock;
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
        });

    }
}

} //namespace arcticdb
