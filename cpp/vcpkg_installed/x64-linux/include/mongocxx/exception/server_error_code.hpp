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

#include <system_error>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Enum representing the various errors types that can be returned from the server. As this list
/// changes over time, this is just a placeholder for an Int32 error code value from the server.
///
enum class server_error_code : std::int32_t {
    // Intentionally empty at this time!
};

///
/// Get the error_category for mongocxx library exceptions.
///
/// @return The mongocxx error_category
///
MONGOCXX_API const std::error_category& MONGOCXX_CALL server_error_category();

///
/// Translate a mongocxx::server_error_code into a std::error_code.
///
/// @param error A mongocxx::error_code
///
/// @return A std::error_code
///
MONGOCXX_INLINE std::error_code make_error_code(server_error_code error) {
    return {static_cast<int>(error), server_error_category()};
}

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>

namespace std {
// Specialize is_error_code_enum so we get simpler std::error_code construction
template <>
struct is_error_code_enum<mongocxx::server_error_code> : public true_type {};
}  // namespace std
