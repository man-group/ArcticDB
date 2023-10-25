/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <entity/types.hpp>
#include <util/type_handler.hpp>

namespace arcticdb {
    namespace py = pybind11;
    struct EmptyHandler {

        EmptyHandler() = default;

        /// @see arcticdb::ITypeHandler
        void handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field,
            const entity::TypeDescriptor& type_descriptor,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers);
    };
} //namespace arcticdb
