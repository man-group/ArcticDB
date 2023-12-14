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

#include <memory>

#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// The log level of a message passed to a mongocxx::logger.
///
enum class log_level {
    k_error,
    k_critical,
    k_warning,
    k_message,
    k_info,
    k_debug,
    k_trace,
};

///
/// Returns a stringification of the given log level.
///
/// @param level
///   The type to stringify.
///
/// @return a std::string representation of the type.
///
MONGOCXX_API stdx::string_view MONGOCXX_CALL to_string(log_level level);

///
/// The interface that all user-defined loggers must implement.
///
class MONGOCXX_API logger {
   public:
    virtual ~logger();

    ///
    /// Handles a log message. User defined logger implementations may do whatever they wish when
    /// this is called, such as log the output to a file or send it to a remote server for analysis.
    ///
    /// @param level
    ///   The log level of the current log message
    /// @param domain
    ///   The domain of the current log message, such as 'client'
    /// @param message
    ///   The text of the current log message.
    virtual void operator()(log_level level,
                            stdx::string_view domain,
                            stdx::string_view message) noexcept = 0;

   protected:
    ///
    /// Default constructor
    ///
    logger();
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
