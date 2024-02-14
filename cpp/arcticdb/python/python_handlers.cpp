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

    /// @brief Generate numpy.dtype object from ArcticDB type descriptor
    /// The dtype is used as type specifier for numpy arrays stored as column elements
    /// @note There is special handling for ArcticDB's empty type
    /// When numpy creates an empty array its type is float64. We want to mimic this because:
    /// i) There is no equivalent to empty value
    /// ii) We want input dataframes to be exact match of the output and that includes the type
    [[nodiscard]] static inline py::dtype generate_python_dtype(const TypeDescriptor& td, stride_t type_byte_size) {
        if(is_empty_type(td.data_type())) {
            return py::dtype{"f8"};
        }
        return py::dtype{fmt::format("{}{:d}", get_dtype_specifier(td.data_type()), type_byte_size)};
    }

    /// @important This calls pybind's initialize array function which is NOT thread safe. Moreover, numpy arrays can
    /// be created only by the thread holindg the GIL. In practice we can get away with allocating arrays only from
    /// a sigle thread (enve if it's not the one holding the GIL). This, however, is not guranteed to work.
    /// @todo Allocate numpy arrays only from the thread holding the GIL
    [[nodiscard]] static inline PyObject* initialize_array(
        const pybind11::dtype& descr,
        const shape_t shapes,
        const stride_t strides,
        const void* source_ptr,
        std::shared_ptr<Column> owner,
        std::mutex& creation_mutex
    ) {
        std::lock_guard creation_guard{creation_mutex};
        // TODO: Py capsule can take only void ptr as input. We need a better way to handle destruction
        //  Allocating shared ptr on the heap is sad.
        auto* object = new std::shared_ptr<Column>(std::move(owner));
        auto arr = py::array(descr, {shapes}, {strides}, source_ptr, py::capsule(object, [](void* obj){
            delete reinterpret_cast<std::shared_ptr<Column>*>(obj);
        }));
        return arr.release().ptr();
    }

    static inline const PyObject** fill_with_none(const PyObject** dest, size_t count) {
        auto none = py::none();
        std::generate(dest, dest + count, [&none]() { return none.inc_ref().ptr(); });
        return dest + count;
    }

    void EmptyHandler::handle_type(
        const uint8_t*& input,
        uint8_t* dest,
        const VariantField& variant_field,
        const entity::TypeDescriptor&,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder>,
        EncodingVersion
    ) {
        ARCTICDB_SAMPLE(HandleEmpty, 0)
        util::check(dest != nullptr, "Got null destination pointer");
        const size_t num_rows = dest_bytes / get_type_size(DataType::EMPTYVAL);
        static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject *));
        auto ptr_dest = reinterpret_cast<const PyObject **>(dest);
        fill_with_none(ptr_dest, num_rows);

        util::variant_match(variant_field, [&input](const auto &field) {
            using EncodedFieldType = std::decay_t<decltype(*field)>;
            if constexpr (std::is_same_v<EncodedFieldType, arcticdb::EncodedField>)
                util::check_magic<ColumnMagic>(input);

            if (field->encoding_case() == EncodedFieldType::NDARRAY) {
                const auto &ndarray_field = field->ndarray();
                const auto num_blocks = ndarray_field.values_size();
                util::check(num_blocks <= 1, "Unexpected number of empty type blocks: {}", num_blocks);
                for (auto block_num = 0; block_num < num_blocks; ++block_num) {
                    const auto &block_info = ndarray_field.values(block_num);
                    input += block_info.out_bytes();
                }
            } else {
                util::raise_rte("Unsupported encoding {}", *field);
            }
        });
    }

    void BoolHandler::handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field_info,
            const entity::TypeDescriptor& type_descriptor,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder>,
            EncodingVersion encding_version
    ) {
        std::visit([&](const auto& field){
            ARCTICDB_SAMPLE(HandleBool, 0)
            util::check(dest != nullptr, "Got null destination pointer");
            util::check(field->has_ndarray(), "Bool handler expected array");
            ARCTICDB_DEBUG(log::version(), "Bool handler got encoded field: {}", field->DebugString());
            size_t row_count = dest_bytes / get_type_size(DataType::PYBOOL64);
            auto ptr_dest = reinterpret_cast<const PyObject**>(dest);

            if(!field->ndarray().sparse_map_bytes()) {
                ARCTICDB_DEBUG(log::version(), "Bool handler has no values");
                fill_with_none(ptr_dest, row_count);
                return;
            }

            std::optional<util::BitSet> bv;
            const auto& ndarray = field->ndarray();
            const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
            auto sparse = ChunkedBuffer::presized(bytes);
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(type_descriptor, *field, data, sparse_sink, bv, encding_version);
            const auto num_bools = bv->count();
            auto ptr_src = sparse.template ptr_cast<uint8_t>(0, num_bools * sizeof(uint8_t));
            auto last_row = 0u;
            const auto ptr_begin = ptr_dest;

            for(auto en = bv->first(); en < bv->end(); ++en) {
                const auto current_pos = *en;
                ptr_dest = fill_with_none(ptr_dest, current_pos - last_row);
                last_row = current_pos;
                auto py_bool = py::bool_(static_cast<bool>(*ptr_src++));
                *ptr_dest++ = py_bool.release().ptr();
                ++last_row;
                util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment {} != {}", last_row, uintptr_t(ptr_begin + last_row));
            }
            fill_with_none(ptr_dest, row_count - last_row);
            util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment at end: {} != {}", last_row, uintptr_t(ptr_begin + last_row));
        }, encoded_field_info);
    }

    std::mutex ArrayHandler::initialize_array_mutex;

    void ArrayHandler::handle_type(
        const uint8_t*& data,
        uint8_t* dest,
        const VariantField& encoded_field_info,
        const entity::TypeDescriptor& type_descriptor,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder> buffers,
		EncodingVersion encoding_version
    ) {
        util::variant_match(encoded_field_info, [&](auto field){
            ARCTICDB_SAMPLE(HandleArray, 0)
            util::check(field->has_ndarray(), "Expected ndarray in array object handler");
            const auto row_count = dest_bytes / sizeof(PyObject*);

            auto ptr_dest = reinterpret_cast<const PyObject**>(dest);
            if(!field->ndarray().sparse_map_bytes()) {
                log::version().info("Array handler has no values");
                fill_with_none(ptr_dest, row_count);
                return;
            }
            std::shared_ptr<Column> column = buffers->get_buffer(type_descriptor, true);
            column->check_magic();
            log::version().info("Column got buffer at {}", uintptr_t(column.get()));
            auto bv = std::make_optional(util::BitSet{});
            data += decode_field(type_descriptor, *field, data, *column, bv, encoding_version);

            auto last_row = 0u;
            ARCTICDB_SUBSAMPLE(InitArrayAcquireGIL, 0)
            const auto strides = static_cast<stride_t>(get_type_size(type_descriptor.data_type()));
            const py::dtype py_dtype = generate_python_dtype(type_descriptor, strides);
            type_descriptor.visit_tag([&] (auto tdt) {
                const auto& blocks = column->blocks();
                if(blocks.empty())
                    return;

                auto block_it = blocks.begin();
                const auto* shapes = column->shape_ptr();
                auto block_pos = 0u;
                const auto* ptr_src = (*block_it)->data();
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
                    if(shapes) {
                        ++shapes;
                    }
                    if(block_it != blocks.end() && block_pos == (*block_it)->bytes() && ++block_it != blocks.end()) {
                        ptr_src = (*block_it)->data();
                        block_pos = 0;
                    }

                    ++last_row;
                }
            });

            ARCTICDB_SUBSAMPLE(ArrayIncNones, 0)
            fill_with_none(ptr_dest, row_count - last_row);
        });
    }
}
