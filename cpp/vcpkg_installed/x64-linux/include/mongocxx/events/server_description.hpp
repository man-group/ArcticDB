// Copyright 2018-present MongoDB Inc.
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

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/string_view.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

namespace events {

///
/// Class representing what the driver knows about a MongoDB server.
///
class MONGOCXX_API server_description {
   public:
    MONGOCXX_PRIVATE explicit server_description(const void* event);

    ///
    /// Destroys a server_description.
    ///
    ~server_description();

    ///
    /// An opaque id, unique to this server for this mongocxx::client or mongocxx::pool.
    ///
    /// @return The id.
    ///
    std::uint32_t id() const;

    ///
    /// The duration of the last hello call, indicating network latency.
    ///
    /// @return The duration in microseconds.
    ///
    std::int64_t round_trip_time() const;

    ///
    /// The server type: "Unknown", "Standalone", "Mongos", "PossiblePrimary", "RSPrimary",
    /// "RSSecondary", "RSArbiter", "RSOther", or "RSGhost".
    ///
    /// @return The type as a short-lived string view.
    ///
    bsoncxx::stdx::string_view type() const;

    ///
    /// @return The response as a short-lived document view.
    ///
    /// @deprecated use hello instead.
    ///
    MONGOCXX_DEPRECATED bsoncxx::document::view is_master() const;

    ///
    /// The server's last response to the "hello" command, or an empty document if the driver
    /// has not yet reached the server or there was an error.
    ///
    /// @return The response as a short-lived document view.
    ///
    bsoncxx::document::view hello() const;

    ///
    /// Returns the server host name.
    ///
    /// @return The host name.
    ///
    bsoncxx::stdx::string_view host() const;

    ///
    /// Returns the server port.
    ///
    /// @return The port.
    ///
    std::uint16_t port() const;

   private:
    const void* _sd;
};

}  // namespace events
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
