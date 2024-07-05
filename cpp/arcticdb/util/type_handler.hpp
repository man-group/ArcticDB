/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/codec/segment_header.hpp>

#include <folly/Poly.h>

#include <memory>
#include <mutex>

namespace arcticdb {

struct DecodePathData;
struct ColumnMapping;
class StringPool;

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
            const EncodedFieldImpl& encoded_field_info,
            const ColumnMapping& mapping,
            size_t dest_bytes,
            const DecodePathData& shared_data,
            EncodingVersion encoding_version,
            const std::shared_ptr<StringPool>& string_pool
        ) {
            folly::poly_call<0>(
                *this,
                source,
                dest,
                encoded_field_info,
                mapping,
                dest_bytes,
                shared_data,
                encoding_version,
                string_pool
            );
        }

        int type_size() const {
            return folly::poly_call<1>(*this);
        }

        void default_initialize(void* dest, size_t byte_size) const {
            folly::poly_call<2>(*this, dest, byte_size);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::handle_type, &T::type_size, &T::default_initialize>;
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

    std::shared_ptr<TypeHandler> get_handler(const entity::TypeDescriptor& type_descriptor) const;
    void register_handler(const entity::TypeDescriptor& type_descriptor, TypeHandler&& handler);
private:
    struct Hasher {
        size_t operator()(const entity::TypeDescriptor val) const;
    };
    std::unordered_map<entity::TypeDescriptor, std::shared_ptr<TypeHandler>, Hasher> handlers_;
};

}
