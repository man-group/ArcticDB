/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/buffer_holder.hpp>
#include <arcticdb/codec/variant_encoded_field_collection.hpp>

#include <folly/Poly.h>

#include <memory>
#include <mutex>

namespace arcticdb {


struct ITypeHandler {
    template<class Base>
    struct Interface : Base {

        /// Handle decoding of a non-trivial type to Python object
        /// @param[in] source Data to be decoded to Python objects
        /// @param[out] dest Memory where the resulting Python objects are stored
        /// @param[in] dest_bytes Size of dest in bytes
        void handle_type(
            const uint8_t*& source,
            uint8_t* dest,
            const VariantField& encoded_field_info,
            const entity::TypeDescriptor& type_descriptor,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers) {
            folly::poly_call<0>(*this, source, dest, encoded_field_info, type_descriptor, dest_bytes, buffers);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::handle_type>;
};

using TypeHandler = folly::Poly<ITypeHandler>;

/// Some types cannot be trivially converted from storage to Python types .This singleton holds a set of type erased
/// handlers (implementing the handle_type function) which handle the parsing from storage to python.
class TypeHandlerRegistry {
public:
    static std::shared_ptr<TypeHandlerRegistry> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<TypeHandlerRegistry> instance();

    std::shared_ptr<TypeHandler> get_handler(entity::DataType data_type) const;
    void register_handler(entity::DataType data_type, TypeHandler&& handler);
private:
    std::unordered_map<entity::DataType, std::shared_ptr<TypeHandler>> handlers_;
};

}
