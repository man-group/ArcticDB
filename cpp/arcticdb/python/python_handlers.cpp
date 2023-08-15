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
    static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject *));
    auto ptr_dest = reinterpret_cast<const PyObject **>(dest);
    py::none none = {};
    for (auto row = 0u; row < num_rows; ++row) {
        none.inc_ref();
        *ptr_dest++ = none.ptr();
    }


    util::variant_match(variant_field, [&input](const auto &field) {
        using EncodedFieldType = std::decay_t<decltype(*field)>;
        if constexpr (std::is_same_v<EncodedFieldType, arcticdb::EncodedField>)
            check_magic<ColumnMagic>(input);

        if (field->encoding_case() == EncodedFieldType::kNdarray) {
            const auto &ndarray_field = field->ndarray();
            const auto num_blocks = ndarray_field.values_size();
            util::check(num_blocks <= 1, "Unexpected number of empty type blocks: {}", num_blocks);
            for (auto block_num = 0; block_num < num_blocks; ++block_num) {
                const auto &block_info = ndarray_field.values(block_num);
                input += block_info.out_bytes();
            }
        } else {
            util::raise_error_msg("Unsupported encoding {}", *field);
        }
    });

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
                ScopedGILLock lock;
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
                util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment {} != {}", last_row, uintptr_t(ptr_begin + last_row));
            }
            for(; last_row < num_rows; ++last_row) {
                none.inc_ref();
                *ptr_dest++ = none.ptr();
            }
            util::check(ptr_begin + last_row == ptr_dest, "Boolean pointer out of alignment at end: {} != {}", last_row, uintptr_t(ptr_begin + last_row));
        }, encoded_field_info);
    }
}

} //namespace arcticdb
