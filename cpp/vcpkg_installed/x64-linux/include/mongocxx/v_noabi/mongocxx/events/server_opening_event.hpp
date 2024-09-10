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

#include <cstdint>

#include <mongocxx/events/server_opening_event-fwd.hpp>

#include <bsoncxx/oid.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace events {

///
/// An event notification sent when the driver adds a MongoDB server to the topology description
/// and begins monitoring it.
///
/// @see "ServerOpeningEvent" in
/// https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst
///
class server_opening_event {
   public:
    MONGOCXX_PRIVATE explicit server_opening_event(const void* event);

    ///
    /// Destroys a server_opening_event.
    ///
    ~server_opening_event();

    ///
    /// Returns the server host name.
    ///
    /// @return The host name.
    ///
    bsoncxx::v_noabi::stdx::string_view host() const;

    ///
    /// Returns the server port.
    ///
    /// @return The port.
    ///
    std::uint16_t port() const;

    ///
    /// An opaque id, unique to this topology for this mongocxx::v_noabi::client or
    /// mongocxx::v_noabi::pool.
    ///
    /// @return The id.
    ///
    const bsoncxx::v_noabi::oid topology_id() const;

   private:
    const void* _event;
};

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
