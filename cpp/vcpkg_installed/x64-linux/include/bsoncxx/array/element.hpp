// Copyright 2014 MongoDB Inc.
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

#include <cstddef>
#include <cstdint>

#include <bsoncxx/document/element.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

namespace types {
namespace bson_value {
class view;
}  // namespace bson_value
}  // namespace types

namespace array {

///
/// A variant view type that accesses values in serialized BSON arrays.
///
/// Element functions as a variant type, where the kind of the element can be
/// interrogated by calling type() and a specific value can be extracted through
/// get_X() accessors.
///
class BSONCXX_API element : private document::element {
   public:
    element();

    using document::element::operator bool;

    using document::element::type;

    using document::element::get_array;
    using document::element::get_binary;
    using document::element::get_bool;
    using document::element::get_code;
    using document::element::get_codewscope;
    using document::element::get_date;
    using document::element::get_dbpointer;
    using document::element::get_decimal128;
    using document::element::get_document;
    using document::element::get_double;
    using document::element::get_int32;
    using document::element::get_int64;
    using document::element::get_maxkey;
    using document::element::get_minkey;
    using document::element::get_null;
    using document::element::get_oid;
    using document::element::get_regex;
    using document::element::get_string;
    using document::element::get_symbol;
    using document::element::get_timestamp;
    using document::element::get_undefined;
    using document::element::get_utf8;

    using document::element::get_value;

    using document::element::operator[];

    using document::element::key;
    using document::element::keylen;
    using document::element::length;
    using document::element::offset;
    using document::element::raw;

   private:
    friend class view;

    BSONCXX_PRIVATE explicit element(const std::uint8_t* raw,
                                     std::uint32_t length,
                                     std::uint32_t offset,
                                     std::uint32_t keylen);
};

///
/// @{
///
/// Convenience methods to compare for equality against a bson_value.
///
/// Returns true if this element contains a bson_value that matches.
///
/// @relates element
///
BSONCXX_API bool BSONCXX_CALL operator==(const element& elem, const types::bson_value::view& v);
BSONCXX_API bool BSONCXX_CALL operator==(const types::bson_value::view& v, const element& elem);
///
/// @}
///

///
/// @{
///
/// Convenience methods to compare for equality against a bson_value.
///
/// Returns false if this element contains a bson_value that matches.
///
/// @relates element
///
BSONCXX_API bool BSONCXX_CALL operator!=(const element& elem, const types::bson_value::view& v);
BSONCXX_API bool BSONCXX_CALL operator!=(const types::bson_value::view& v, const element& elem);
///
/// @}
///

}  // namespace array

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
