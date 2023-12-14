// Copyright 2015 MongoDB Inc.
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

#include <cstdint>
#include <system_error>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

///
/// Enum representing the various error types that can occur while operating on BSON values.
///
enum class error_code : std::int32_t {
    /// A new key was appended while building a subarray.
    k_cannot_append_key_in_sub_array = 1,

    /// A subarray was closed while building a subdocument.
    k_cannot_close_array_in_sub_document,

    /// A subdocument was closed while building a subarray.
    k_cannot_close_document_in_sub_array,

    /// An array operation was performed while building a document.
    k_cannot_perform_array_operation_on_document,

    /// A document operation was performed while building an array.
    k_cannot_perform_document_operation_on_array,
#define BSONCXX_ENUM(name, value) k_need_element_type_k_##name,
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
    /// No key was provided when one was needed.
    k_need_key,

    /// An array was closed while no array was open.
    k_no_array_to_close,

    /// A document was closed while no document was open.
    k_no_document_to_close,

    // Attempted to view or extract a document when a key was still awaiting a matching value.
    k_unmatched_key_in_builder,

    /// An empty element was accessed.
    k_unset_element,

    /// A JSON document failed to parse.
    k_json_parse_failure,

    /// An Object ID string failed to parse.
    k_invalid_oid,

    // This type is unused and deprecated.
    k_failed_converting_bson_to_json,

    /// A Decimal128 string failed to parse.
    k_invalid_decimal128,

    /// BSON data could not be processed, but no specific reason was available.
    k_internal_error,

    /// Failed to begin appending an array to a BSON document or array.
    k_cannot_begin_appending_array,

    /// Failed to begin appending a BSON document to a BSON document or array.
    k_cannot_begin_appending_document,

    /// Failed to complete appending an array to a BSON document or array.
    k_cannot_end_appending_array,

    /// Failed to complete appending a BSON document to a BSON document or array.
    k_cannot_end_appending_document,

    /// Invalid binary subtype.
    k_invalid_binary_subtype,

    /// Invalid type.
    k_invalid_bson_type_id,

/// A value failed to append.
#define BSONCXX_ENUM(name, value) k_cannot_append_##name,
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
    k_cannot_append_utf8 = k_cannot_append_string,
    k_need_element_type_k_utf8 = k_need_element_type_k_string,
    // Add new constant string message to error_code.cpp as well!
};

///
/// Get the error_category for exceptions originating from the bsoncxx library.
///
/// @return The bsoncxx error_category
///
BSONCXX_API const std::error_category& BSONCXX_CALL error_category();

///
/// Translate a bsoncxx::error_code into a std::error_code.
///
/// @param error An error from bsoncxx
/// @return An error_code
///
BSONCXX_INLINE std::error_code make_error_code(error_code error) {
    return {static_cast<int>(error), error_category()};
}

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>

namespace std {
// Specialize is_error_code_enum so we get simpler std::error_code construction
template <>
struct is_error_code_enum<bsoncxx::error_code> : public true_type {};
}  // namespace std
