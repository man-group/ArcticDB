// Copyright 2020 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include <bsoncxx/array/view_or_value.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

namespace types {
namespace bson_value {

///
/// A variant owning type that represents any BSON type. Owns its underlying
/// buffer. When a bson_value::value goes out of scope, its underlying
/// buffer is freed.
///
/// For accessors into this type and to extract the various BSON types out,
/// please use bson_value::view.
///
/// @relatesalso bson_value::view
///
class BSONCXX_API value {
   public:
///
/// Constructor for each BSON type.
///
/// These x-macros will expand to:
///    value(b_double v);
///    value(b_string v);
///    value(b_document v);
///    value(b_array v); ...
///
#define BSONCXX_ENUM(name, val) value(b_##name v);
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM

    ///
    /// Constructs a BSON UTF-8 string value.
    ///
    value(const char* v);

    ///
    /// Constructs a BSON UTF-8 string value.
    ///
    value(std::string v);

    ///
    /// Constructs a BSON UTF-8 string value.
    ///
    value(stdx::string_view v);

    ///
    /// Constructs a BSON 32-bit signed integer value.
    ///
    value(int32_t v);

    ///
    /// Constructs a BSON 64-bit signed integer value.
    ///
    value(int64_t v);

    ///
    /// Constructs a BSON double value.
    ///
    value(double v);

    ///
    /// Constructs a BSON boolean value.
    ///
    value(bool v);

    ///
    /// Constructs a BSON ObjectId value.
    ///
    value(oid v);

    ///
    /// Constructs a BSON Decimal128 value.
    ///
    value(decimal128 v);

    ///
    /// Constructs a BSON date value.
    ///
    value(std::chrono::milliseconds v);

    ///
    /// Constructs a BSON null value.
    ///
    value(std::nullptr_t);

    ///
    /// Constructs a BSON document value.
    ///
    value(bsoncxx::document::view v);

    ///
    /// Constructs a BSON array value.
    ///
    value(bsoncxx::array::view v);

    ///
    /// Constructs a BSON binary data value.
    ///
    /// @param v
    ///     a stream of bytes
    /// @param sub_type
    ///     an optional binary sub type. Defaults to type::k_binary
    ///
    value(std::vector<unsigned char> v, const binary_sub_type sub_type = {});

    ///
    /// Constructs a BSON binary data value.
    ///
    /// @param data
    ///     pointer to a stream of bytes
    /// @param size
    ///     the size of the stream of bytes
    /// @param sub_type
    ///     an optional binary sub type. Defaults to type::k_binary
    ///
    value(const uint8_t* data, size_t size, const binary_sub_type sub_type = {});

    ///
    /// Constructs a BSON DBPointer value.
    ///
    /// @param collection
    ///     the collection name
    /// @param value
    ///     the object id
    ///
    /// @deprecated
    ///   A BSON DBPointer (aka DBRef) is still supported but deprecated.
    ///
    value(stdx::string_view collection, oid value);

    ///
    /// Constructs a BSON JavaScript code with scope value.
    ///
    /// @param code
    ///     the JavaScript code
    /// @param scope
    ///     a bson document view holding the scope environment
    ///
    value(stdx::string_view code, bsoncxx::document::view_or_value scope);

    ///
    /// Constructs a BSON regex value with options.
    ///
    /// @param regex
    ///   The regex pattern
    /// @param options
    ///   The regex options
    ///
    value(stdx::string_view regex, stdx::string_view options);

    ///
    /// Constructs one of the following BSON values (each specified by the parenthesized type):
    /// - BSON code value (type::k_code)
    /// - BSON regex value (type::k_regex)
    /// - BSON symbol value (type::k_symbol)
    ///
    /// @param id
    ///     the type of BSON value to construct.
    /// @param v
    ///     the symbol, JavaScript code, or regex pattern for the BSON symbol, code, or regex value
    ///     respectively.
    ///
    /// @throws bsoncxx::exception if the type's value is not k_code, k_regex, or k_symbol.
    ///
    /// @deprecated
    ///   The BSON symbol type is deprecated and use by clients is discouraged.
    /// @deprecated
    ///   The BSON undefined type is deprecated and use by clients is discouraged.
    ///
    value(const type id, stdx::string_view v);

    ///
    /// Constructs one of the following BSON values (each specified by the parenthesized type):
    /// - BSON maxkey value (type::k_maxkey)
    /// - BSON minkey value (type::k_minkey)
    /// - BSON undefined value (type::k_undefined)
    ///
    /// @param id
    ///     the type of BSON value to construct.
    ///
    /// @throws bsoncxx::exception if the type's value is not k_maxkey, k_minkey, or k_undefined.
    ///
    value(const type id);

    ///
    /// Constructs one of the following BSON values (each specified by the parenthesized type):
    /// - BSON decimal128 value (type::k_decimal128)
    /// - BSON timestamp value (type::k_timestamp)
    ///
    /// @param id
    ///     the type of the BSON value to construct.
    /// @param a
    ///     If a BSON decimal128 value is to be constructed, this is the high value.
    ///     If a BSON timestamp value is to be constructed, this is the increment.
    /// @param b
    ///     If a BSON decimal128 value is to be constructed, this is the low value.
    ///     If a BSON timestamp value is to be constructed, this is the timestamp.
    ///
    /// @throws bsoncxx::exception if the specified type is missing its required arguments.
    ///
    /// @warning
    ///   The BSON timestamp type is used internally by the MongoDB server - use by clients
    ///   is discouraged.
    ///
    value(const type id, uint64_t a, uint64_t b);

    ~value();

    value(const value&);
    value& operator=(const value&);

    value(value&&) noexcept;
    value& operator=(value&&) noexcept;

    ///
    /// Create an owning copy of a bson_value::view.
    ///
    explicit value(const view&);

    ///
    /// Get a view over the bson_value owned by this object.
    ///
    bson_value::view view() const noexcept;

    ///
    /// Conversion operator that provides a bson_value::view given a bson_value::value.
    ///
    operator bson_value::view() const noexcept;

   private:
    friend class bsoncxx::document::element;

    value(const std::uint8_t* raw,
          std::uint32_t length,
          std::uint32_t offset,
          std::uint32_t keylen);

    // Makes a copy of 'internal_value' and owns the copy.
    value(void* internal_value);

    friend value make_owning_bson(void* internal_value);

    class BSONCXX_PRIVATE impl;
    std::unique_ptr<impl> _impl;
};

///
/// @{
///
/// Compares values for (in)-equality.
///
/// @relates bson_value::value
///
BSONCXX_INLINE bool operator==(const value& lhs, const value& rhs) {
    return (lhs.view() == rhs.view());
}

BSONCXX_INLINE bool operator!=(const value& lhs, const value& rhs) {
    return !(lhs == rhs);
}

///
/// @}
///

///
/// @{
///
/// Compares a value with a view for (in)-equality.
///
/// @relates bson_value::value
///
BSONCXX_INLINE bool operator==(const value& lhs, const view& rhs) {
    return (lhs.view() == rhs);
}

BSONCXX_INLINE bool operator==(const view& lhs, const value& rhs) {
    return (rhs == lhs);
}

BSONCXX_INLINE bool operator!=(const value& lhs, const view& rhs) {
    return !(lhs == rhs);
}

BSONCXX_INLINE bool operator!=(const view& lhs, const value& rhs) {
    return !(lhs == rhs);
}

///
/// @}
///

}  // namespace bson_value
}  // namespace types

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
