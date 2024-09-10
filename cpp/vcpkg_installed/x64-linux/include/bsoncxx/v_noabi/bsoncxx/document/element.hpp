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

#include <bsoncxx/array/element-fwd.hpp>
#include <bsoncxx/document/element-fwd.hpp>
#include <bsoncxx/document/view-fwd.hpp>
#include <bsoncxx/types-fwd.hpp>
#include <bsoncxx/types/bson_value/value-fwd.hpp>
#include <bsoncxx/types/bson_value/view-fwd.hpp>

#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
namespace v_noabi {
namespace document {

///
/// A variant view type that accesses values in serialized BSON documents.
///
/// Element functions as a variant type, where the kind of the element can be
/// interrogated by calling type(), the key can be extracted by calling key() and
/// a specific value can be extracted through get_X() accessors.
///
/// @relatesalso array::element
///
class element {
   public:
    ///
    /// Construct an invalid element.
    ///
    /// This is useful when mapping the end iterator of a document or array
    /// view.
    ///
    element();

    ///
    /// Conversion operator to bool which is true for valid elements
    /// and false for invalid elements.
    ///
    explicit operator bool() const;

    ///
    /// Getter for the raw bson bytes the element points to.
    ///
    /// @return a pointer to the raw bson bytes.
    ///
    const std::uint8_t* raw() const;

    ///
    /// Getter for length of the raw bson bytes the element points to.
    ///
    /// @return a pointer to the length of the raw bson bytes.
    ///
    std::uint32_t length() const;

    ///
    /// Getter for the offset into the raw bson bytes the element points to.
    ///
    /// @return the offset into the raw bson bytes.
    ///
    std::uint32_t offset() const;

    ///
    /// Getter for the type of the element.
    ///
    /// @return the element's type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is invalid.
    ///
    bsoncxx::v_noabi::type type() const;

    ///
    /// Getter for the element's key.
    ///
    /// @return the element's key.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is invalid.
    ///
    stdx::string_view key() const;

    ///
    /// Getter for the element's key length.
    ///
    /// @return the element's key length.
    ///
    std::uint32_t keylen() const;

    ///
    /// Getter for elements of the b_double type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_double.
    ///
    /// @return the element's value.
    ///
    types::b_double get_double() const;

    ///
    /// Getter for elements of the b_string type.
    ///
    /// @deprecated use document::element::get_string() instead.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_string.
    ///
    /// @return the element's value.
    ///
    BSONCXX_DEPRECATED types::b_string get_utf8() const;

    ///
    /// Getter for elements of the b_string type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_string.
    ///
    /// @return the element's value.
    ///
    types::b_string get_string() const;

    ///
    /// Getter for elements of the b_document type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_document.
    ///
    /// @return the element's value.
    ///
    types::b_document get_document() const;

    ///
    /// Getter for elements of the b_array type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_array.
    ///
    /// @return the element's value.
    ///
    types::b_array get_array() const;

    ///
    /// Getter for elements of the b_binary type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_binary.
    ///
    /// @return the element's value.
    ///
    types::b_binary get_binary() const;

    ///
    /// Getter for elements of the b_undefined type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_undefined.
    ///
    /// @return the element's value.
    ///
    types::b_undefined get_undefined() const;

    ///
    /// Getter for elements of the b_oid type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_oid.
    ///
    /// @return the element's value.
    ///
    types::b_oid get_oid() const;

    ///
    /// Getter for elements of the b_bool type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_bool.
    ///
    /// @return the element's value.
    ///
    types::b_bool get_bool() const;

    ///
    /// Getter for elements of the b_date type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_date.
    ///
    /// @return the element's value.
    ///
    types::b_date get_date() const;

    ///
    /// Getter for elements of the b_null type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_null.
    ///
    /// @return the element's value.
    ///
    types::b_null get_null() const;

    ///
    /// Getter for elements of the b_regex type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_regex.
    ///
    /// @return the element's value.
    ///
    types::b_regex get_regex() const;

    ///
    /// Getter for elements of the b_dbpointer type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_dbpointer.
    ///
    /// @return the element's value.
    ///
    types::b_dbpointer get_dbpointer() const;

    ///
    /// Getter for elements of the b_code type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_code.
    ///
    /// @return the element's value.
    ///
    types::b_code get_code() const;

    ///
    /// Getter for elements of the b_symbol type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_symbol.
    ///
    /// @return the element's value.
    ///
    types::b_symbol get_symbol() const;

    ///
    /// Getter for elements of the b_codewscope type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_codewscope.
    ///
    /// @return the element's value.
    ///
    types::b_codewscope get_codewscope() const;

    ///
    /// Getter for elements of the b_int32 type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_int32.
    ///
    /// @return the element's value.
    ///
    types::b_int32 get_int32() const;

    ///
    /// Getter for elements of the b_timestamp type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_timestamp.
    ///
    /// @return the element's value.
    ///
    types::b_timestamp get_timestamp() const;

    ///
    /// Getter for elements of the b_int64 type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_int64.
    ///
    /// @return the element's value.
    ///
    types::b_int64 get_int64() const;

    ///
    /// Getter for elements of the b_decimal128 type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_decimal128.
    ///
    /// @return the element's value.
    ///
    types::b_decimal128 get_decimal128() const;

    ///
    /// Getter for elements of the b_minkey type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_minkey.
    ///
    /// @return the element's value.
    ///
    types::b_minkey get_minkey() const;

    ///
    /// Getter for elements of the b_maxkey type.
    ///
    /// @throws bsoncxx::v_noabi::exception if this element is not a b_maxkey.
    ///
    /// @return the element's value.
    ///
    types::b_maxkey get_maxkey() const;

    ///
    /// Getter for a types::bson_value::view variant wrapper of the value portion of the
    /// element.
    ///
    /// @return the element's value.
    ///
    types::bson_value::view get_value() const;

    ///
    /// Getter for a types::bson_value::value variant wrapper of the value portion of
    /// the element. The returned object will make a copy of the buffer from this object.
    ///
    /// @return an owning version of the element's value.
    ///
    types::bson_value::value get_owning_value() const;

    ///
    /// If this element is a document, finds the first element of the document
    /// with the provided key. If there is no such element, an invalid
    /// document::element will be returned.  The runtime of operator[] is
    /// linear in the length of the document.
    ///
    /// If this element is not a document, an invalid document::element will
    /// be returned.
    ///
    /// @param key
    ///   The key to search for.
    ///
    /// @return The matching element, if found, or an invalid element.
    ///
    element operator[](stdx::string_view key) const;

    ///
    /// If this element is an array, indexes into this BSON array. If the
    /// index is out-of-bounds, an invalid array::element will be returned. As
    /// BSON represents arrays as documents, the runtime of operator[] is
    /// linear in the length of the array.
    ///
    /// If this element is not an array, an invalid array::element will
    /// be returned.
    ///
    /// @param i
    ///   The index of the element.
    ///
    /// @return The element if it exists, or an invalid element.
    ///
    array::element operator[](std::uint32_t i) const;

   private:
    ///
    /// Construct an element as an offset into a buffer of bson bytes.
    ///
    /// @param raw
    ///   A pointer to the raw bson bytes.
    ///
    /// @param length
    ///   The size of the bson buffer.
    ///
    /// @param offset
    ///   The element's offset into the buffer.
    ///
    BSONCXX_PRIVATE explicit element(const std::uint8_t* raw,
                                     std::uint32_t length,
                                     std::uint32_t offset,
                                     std::uint32_t keylen);

    // Construct an invalid element with a key. Useful for exceptions.
    BSONCXX_PRIVATE explicit element(const stdx::string_view key);

    friend ::bsoncxx::v_noabi::array::element;
    friend ::bsoncxx::v_noabi::document::view;

    const std::uint8_t* _raw;
    std::uint32_t _length;
    std::uint32_t _offset;
    std::uint32_t _keylen;
    // _key will only exist when a caller attempts to find a key in the BSON but is unsuccessful.
    // The key is stored for a more helpful error message if the user tries to access the value of
    // a key that does not exist.
    stdx::optional<stdx::string_view> _key;
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

}  // namespace document
}  // namespace v_noabi
}  // namespace bsoncxx

namespace bsoncxx {
namespace document {

using ::bsoncxx::v_noabi::document::operator==;
using ::bsoncxx::v_noabi::document::operator!=;

}  // namespace document
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
