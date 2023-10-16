/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <python/python_handlers.hpp>
#include <python/gil_lock.hpp>

namespace arcticdb {
    void EmptyHandler::handle_type(
        const uint8_t*& input,
        uint8_t* dest,
        const VariantField& variant_field,
        const entity::TypeDescriptor&,
        size_t dest_bytes,
        std::shared_ptr<BufferHolder>
    ) {
        ARCTICDB_SAMPLE(HandleEmpty, 0);
        util::check(dest != nullptr, "Got null destination pointer");
        const size_t num_rows = dest_bytes / get_type_size(DataType::EMPTYVAL);
        static_assert(get_type_size(DataType::EMPTYVAL) == sizeof(PyObject*));
        const PyObject** ptr_dest = reinterpret_cast<const PyObject**>(dest);
        py::none none = {};
        for(auto row = 0u; row < num_rows; ++row) {
            none.inc_ref();
            *ptr_dest++ = none.ptr();
        }

        util::variant_match(variant_field, [&input] (const auto& field) {
            using EncodedFieldType = std::decay_t<decltype(*field)>;
                switch (field->encoding_case()) {
                    case EncodedFieldType::kNdarray: {
                        const auto& ndarray_field = field->ndarray();
                        const auto num_blocks = ndarray_field.values_size();
                        util::check(num_blocks <= 1, "Unexpected number of empty type blocks: {}", num_blocks);
                        for (auto block_num = 0; block_num < num_blocks; ++block_num) {
                            const auto &block_info = ndarray_field.values(block_num);
                            input += block_info.out_bytes();
                        }
                        break;
                    }
                    default:
                        util::raise_error_msg("Unsupported encoding {}", *field);
                    }
                });
    }
}
