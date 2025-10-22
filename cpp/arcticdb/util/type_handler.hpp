/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/output_format.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/pipeline/read_options.hpp>
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

        /// Handle decoding of a non-trivial type to internal object representation which can be handed over to Python
        /// zero-copy.
        /// @param[in] source Data to be decoded to Python objects
        /// @param[out] dest_column Column where the resulting Python objects are stored
        /// @param[in] mapping Describes where in the column to decode (essentially a range of bytes to fill inside the
        /// column)
        void handle_type(
                const uint8_t*& source, Column& dest_column, const EncodedFieldImpl& encoded_field_info,
                const ColumnMapping& mapping, const DecodePathData& shared_data, std::any& handler_data,
                EncodingVersion encoding_version, const std::shared_ptr<StringPool>& string_pool,
                const ReadOptions& read_options
        ) {
            folly::poly_call<0>(
                    *this,
                    source,
                    dest_column,
                    encoded_field_info,
                    mapping,
                    shared_data,
                    handler_data,
                    encoding_version,
                    string_pool,
                    read_options
            );
        }

        /// Convert the entire source_column onto a part of dest_column.
        /// @param[in] source_column Source column
        /// @param[out] dest_column Column where the resulting Python objects are stored
        /// @param[in] mapping Describes where in the dest_column to place the converted source_column
        void convert_type(
                const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
                const DecodePathData& shared_data, std::any& handler_data,
                const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
        ) const {
            folly::poly_call<1>(
                    *this, source_column, dest_column, mapping, shared_data, handler_data, string_pool, read_options
            );
        }

        std::pair<entity::TypeDescriptor, std::optional<size_t>> output_type_and_extra_bytes(
                const entity::TypeDescriptor& input_type, const std::string_view& column_name,
                const ReadOptions& read_options
        ) const {
            return folly::poly_call<2>(*this, input_type, column_name, read_options);
        }

        void default_initialize(
                ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& shared_data,
                std::any& handler_data
        ) const {
            folly::poly_call<3>(*this, buffer, offset, byte_size, shared_data, handler_data);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<
            &T::handle_type, &T::convert_type, &T::output_type_and_extra_bytes, &T::default_initialize>;
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

    std::shared_ptr<TypeHandler> get_handler(OutputFormat output_format, const entity::TypeDescriptor& type_descriptor)
            const;
    void register_handler(
            OutputFormat output_format, const entity::TypeDescriptor& type_descriptor, TypeHandler&& handler
    );

    void set_handler_data(OutputFormat output_format, std::unique_ptr<TypeHandlerDataFactory>&& data) {
        handler_data_factories_[static_cast<uint8_t>(output_format)] = std::move(data);
    }

    std::any get_handler_data(OutputFormat output_format) {
        util::check(
                static_cast<bool>(handler_data_factories_[static_cast<uint8_t>(output_format)]), "No type handler set"
        );
        return handler_data_factories_[static_cast<uint8_t>(output_format)]->get_data();
    }

  private:
    std::array<std::unique_ptr<TypeHandlerDataFactory>, static_cast<size_t>(OutputFormat::COUNT)>
            handler_data_factories_;

    struct Hasher {
        size_t operator()(entity::TypeDescriptor val) const;
    };

    using TypeHandlerMap = std::unordered_map<entity::TypeDescriptor, std::shared_ptr<TypeHandler>, Hasher>;

    TypeHandlerMap& handler_map(OutputFormat output_format) { return handlers_[static_cast<int>(output_format)]; }

    const TypeHandlerMap& handler_map(OutputFormat output_format) const {
        const auto pos = static_cast<int>(output_format);
        util::check(size_t(pos) < handlers_.size(), "No handler map found for output format {}", pos);
        return handlers_[static_cast<int>(output_format)];
    }

    std::array<TypeHandlerMap, static_cast<size_t>(OutputFormat::COUNT)> handlers_;
};

inline std::shared_ptr<TypeHandler> get_type_handler(OutputFormat output_format, entity::TypeDescriptor source) {
    return TypeHandlerRegistry::instance()->get_handler(output_format, source);
}

inline std::shared_ptr<TypeHandler> get_type_handler(
        OutputFormat output_format, entity::TypeDescriptor source, entity::TypeDescriptor target
) {
    auto handler = TypeHandlerRegistry::instance()->get_handler(output_format, source);
    if (handler)
        return handler;

    return TypeHandlerRegistry::instance()->get_handler(output_format, target);
}

} // namespace arcticdb
