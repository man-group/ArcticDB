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
#include <arcticdb/util/lazy.hpp>
#include <arcticdb/util/spinlock.hpp>

#include <folly/Poly.h>

#include <memory>
#include <mutex>
#include <any>

namespace arcticdb {

struct DecodePathData;
struct ColumnMapping;
class StringPool;
class Column;

struct ITypeHandler {
    template<class Base>
    struct Interface : Base {

        /// Handle decoding of a non-trivial type to Python object
        /// @param[in] source Data to be decoded to Python objects
        /// @param[out] dest Memory where the resulting Python objects are stored
        /// @param[in] dest_bytes Size of dest in bytes
        void handle_type(
            const uint8_t*& source,
            ChunkedBuffer& dest_buffer,
            const EncodedFieldImpl& encoded_field_info,
            const ColumnMapping& mapping,
            const DecodePathData& shared_data,
            std::any& handler_data,
            EncodingVersion encoding_version,
            const std::shared_ptr<StringPool>& string_pool
        ) {
            folly::poly_call<0>(
                *this,
                source,
                dest_buffer,
                encoded_field_info,
                mapping,
                shared_data,
                handler_data,
                encoding_version,
                string_pool
            );
        }

        [[nodiscard]] int type_size() const {
            return folly::poly_call<2>(*this);
        }

        void convert_type(
            const Column& source_column,
            ChunkedBuffer& dest_buffer,
            size_t num_rows,
            size_t offset_bytes,
            TypeDescriptor source_type_desc,
            TypeDescriptor dest_type_desc,
            const DecodePathData& shared_data,
            std::any& handler_data,
            const std::shared_ptr<StringPool>& string_pool) {
            folly::poly_call<1>(*this, source_column, dest_buffer, num_rows, offset_bytes, source_type_desc, dest_type_desc, shared_data, handler_data, string_pool);
        }

        void default_initialize(ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& shared_data, std::any& handler_data) const {
            folly::poly_call<3>(*this, buffer, offset, byte_size, shared_data, handler_data);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::handle_type, &T::convert_type, &T::type_size, &T::default_initialize>;
};

using TypeHandler = folly::Poly<ITypeHandler>;

class TypeHandlerDataFactory {
public:
    virtual std::any get_data() const = 0;
    virtual ~TypeHandlerDataFactory() = default;
};

/// Some types cannot be trivially converted from storage to Python types .This singleton holds a set of type erased
/// handlers (implementing the handle_type function) which handle the parsing from storage to python.
class TypeHandlerRegistry {
public:
    static std::shared_ptr<TypeHandlerRegistry> instance_;
    static std::once_flag init_flag_;

    static void init();
    static std::shared_ptr<TypeHandlerRegistry> instance();
    static void destroy_instance();

    std::shared_ptr<TypeHandler> get_handler(const entity::TypeDescriptor& type_descriptor) const;
    void register_handler(const entity::TypeDescriptor& type_descriptor, TypeHandler&& handler);

    void set_handler_data(std::unique_ptr<TypeHandlerDataFactory>&& data) {
        handler_data_factory_ = std::move(data);
    }

    std::any get_handler_data() {
        util::check(static_cast<bool>(handler_data_factory_), "No type handler set");
        return handler_data_factory_->get_data();
    }

private:
    std::unique_ptr<TypeHandlerDataFactory> handler_data_factory_;

    struct Hasher {
        size_t operator()(entity::TypeDescriptor val) const;
    };
    std::unordered_map<entity::TypeDescriptor, std::shared_ptr<TypeHandler>, Hasher> handlers_;
};


inline std::shared_ptr<TypeHandler> get_type_handler(TypeDescriptor source, TypeDescriptor target) {
    auto handler = TypeHandlerRegistry::instance()->get_handler(source);
    if(handler)
        return handler;

    return TypeHandlerRegistry::instance()->get_handler(target);
}

inline std::any get_type_handler_data() {
    return TypeHandlerRegistry::instance()->get_handler_data();
}


}
