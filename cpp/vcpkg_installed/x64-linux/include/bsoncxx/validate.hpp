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
#include <memory>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

class validator;

///
/// Validates a BSON document. This is a simplified overload that will
/// only do the bare minimum validation of document structure, and does
/// not provide any further information if the document is found to be invalid.
///
/// @param data
///   A buffer containing a BSON document to validate.
/// @param length
///   The size of the buffer.
///
/// @returns
///   An engaged optional containing a view if the document is valid, or
///   an unengaged optional if the document is invalid.
///
BSONCXX_API stdx::optional<document::view> BSONCXX_CALL validate(const std::uint8_t* data,
                                                                 std::size_t length);

///
/// Validates a BSON document. This overload provides additional control over the
/// precise validation that is performed, and will give the caller access to the
/// offset at which the document was found to be invalid.
///
/// @param data
///   A buffer containing a BSON document to validate.
/// @param length
///   The size of the buffer.
/// @param validator
///   A validator used to configure what checks are done. If validation fails, it
///   will contain the offset at which the document was found to be invalid.
/// @param invalid_offset
///   If validation fails, the offset at which the document was found to be invalid
///   will be stored here (if non-null).
///
/// @returns
///   An engaged optional containing a view if the document is valid, or
///   an unengaged optional if the document is invalid.
///
BSONCXX_API stdx::optional<document::view> BSONCXX_CALL
validate(const std::uint8_t* data,
         std::size_t length,
         const validator& validator,
         std::size_t* invalid_offset = nullptr);
///
/// A validator is used to enable or disable specific checks that can be
/// performed during BSON validation.
///
class BSONCXX_API validator {
   public:
    ///
    /// Constructs a validator.
    ///
    validator();

    ///
    /// Destructs a validator.
    ///
    ~validator();

    ///
    /// Verify that all keys and string values are valid UTF-8.
    ///
    /// @param check_utf8
    ///   If true, UTF-8 validation is performed.
    ///
    void check_utf8(bool check_utf8);

    ///
    /// Getter for the current check_utf8 value of the underlying validator.
    ///
    /// @return True if UTF-8 validation is performed.
    ///
    bool check_utf8() const;

    ///
    /// Verify that all keys and string values are valid UTF-8, but allow
    /// null bytes. This is generally bad practice, but some languages such
    /// as Python, can sennd UTF-8 encoded strings with null bytes.
    ///
    /// @param check_utf8_allow_null
    ///   If true, UTF-8 validation (with null bytes allowed) is performed.
    ///
    void check_utf8_allow_null(bool check_utf8_allow_null);

    ///
    /// Getter for the current check_utf8_allow_null value of the underlying
    /// validator.
    ///
    /// @return True if UTF-8 validation (with null bytes allowed) is
    ///   performed.
    ///
    bool check_utf8_allow_null() const;

    ///
    /// Verifies that document keys are not preceeded with '$'.
    ///
    /// @param check_dollar_keys
    ///   If true, keys starting with '$' will be treated as invalid.
    ///
    void check_dollar_keys(bool check_dollar_keys);

    ///
    /// Getter for the current check_dollar_keys value of the underlying
    /// validator.
    ///
    /// @return True if keys starting with '$' will be treated as invalid.
    ///
    bool check_dollar_keys() const;

    ///
    /// Verifies that document keys do not contain any '.' characters.
    ///
    /// @param check_dot_keys
    ///   If true, keys containing '.' will be treated as invalid.
    ///
    void check_dot_keys(bool check_dot_keys);

    ///
    /// Getter for the current check_dot_keys value of the underlying
    /// validator.
    ///
    /// @return True if keys containing '.' will be treated as invalid.
    ///
    bool check_dot_keys() const;

   private:
    struct BSONCXX_PRIVATE impl;
    std::unique_ptr<impl> _impl;
};

BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>
