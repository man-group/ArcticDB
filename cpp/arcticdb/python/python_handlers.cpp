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
        const uint8_t*&,
        uint8_t* dest,
        const VariantField&,
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
    }
}
