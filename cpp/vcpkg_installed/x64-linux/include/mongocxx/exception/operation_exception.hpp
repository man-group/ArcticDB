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

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing an exception received from a MongoDB server.  It includes the server-provided
/// error code, if one was available.
///
/// @see mongocxx::exception
///
class MONGOCXX_API operation_exception : public exception {
   public:
    using exception::exception;

    ///
    /// Constructs a new operation exception.
    ///
    /// @param ec
    ///   The error code associated with this exception.
    /// @param raw_server_error
    ///   The optional raw bson error document to be associated with this exception.
    /// @param what_arg
    ///   An optional message to be returned by `what`.
    ///
    operation_exception(std::error_code ec,
                        bsoncxx::document::value&& raw_server_error,
                        std::string what_arg = "");

    ///
    /// @{
    ///
    /// The optional raw bson error document from the server.
    ///
    /// @returns The raw server error, if it is available.
    ///
    const stdx::optional<bsoncxx::document::value>& raw_server_error() const;
    stdx::optional<bsoncxx::document::value>& raw_server_error();
    ///
    /// @}
    ///

    ///
    /// Checks for the specified label in a operation exception.
    ///
    /// @param label
    ///   A string of the label to search for
    ///
    /// @returns True if the label is found in the operation exception, and false otherwise.
    ///
    bool has_error_label(stdx::string_view label) const;

   private:
    stdx::optional<bsoncxx::document::value> _raw_server_error;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
