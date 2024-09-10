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

#include <mongocxx/events/heartbeat_started_event-fwd.hpp>

#include <bsoncxx/stdx/string_view.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace events {

///
/// An event notification sent when the driver begins executing a "hello" command to check the
/// status of a server.
///
/// @see "ServerHeartbeatStartedEvent" in
/// https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst
///
class heartbeat_started_event {
   public:
    MONGOCXX_PRIVATE explicit heartbeat_started_event(const void* event);

    ///
    /// Destroys a heartbeat_started_event.
    ///
    ~heartbeat_started_event();

    ///
    /// Returns the host name.
    ///
    /// @return The host name.
    ///
    bsoncxx::v_noabi::stdx::string_view host() const;

    ///
    /// Returns the port.
    ///
    /// @return The port.
    ///
    std::uint16_t port() const;

    ///
    /// Returns a boolean indicating whether this heartbeat event is from an awaitable hello.
    ///
    /// @return A boolean.
    ///
    bool awaited() const;

   private:
    const void* _started_event;
};

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
