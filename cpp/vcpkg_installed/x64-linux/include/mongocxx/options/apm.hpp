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

#include <functional>

#include <mongocxx/events/command_failed_event.hpp>
#include <mongocxx/events/command_started_event.hpp>
#include <mongocxx/events/command_succeeded_event.hpp>
#include <mongocxx/events/heartbeat_failed_event.hpp>
#include <mongocxx/events/heartbeat_started_event.hpp>
#include <mongocxx/events/heartbeat_succeeded_event.hpp>
#include <mongocxx/events/server_changed_event.hpp>
#include <mongocxx/events/server_closed_event.hpp>
#include <mongocxx/events/server_opening_event.hpp>
#include <mongocxx/events/topology_changed_event.hpp>
#include <mongocxx/events/topology_closed_event.hpp>
#include <mongocxx/events/topology_opening_event.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing MongoDB application performance monitoring.
///
class MONGOCXX_API apm {
   public:
    ///
    /// Set the command started monitoring callback. The callback takes a reference to a
    /// command_started_event which will only contain valid data for the duration of the callback.
    ///
    /// @param command_started
    ///   The command started monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_command_started(
        std::function<void(const mongocxx::events::command_started_event&)> command_started);

    ///
    /// Retrieves the command started monitoring callback.
    ///
    /// @return The command started monitoring callback.
    ///
    const std::function<void(const mongocxx::events::command_started_event&)>& command_started()
        const;

    ///
    /// Set the command failed monitoring callback. The callback takes a reference to a
    /// command_failed_event which will only contain valid data for the duration of the callback.
    ///
    /// @param command_failed
    ///   The command failed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_command_failed(
        std::function<void(const mongocxx::events::command_failed_event&)> command_failed);

    ///
    /// Retrieves the command failed monitoring callback.
    ///
    /// @return The command failed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::command_failed_event&)>& command_failed()
        const;

    ///
    /// Set the command succeeded monitoring callback. The callback takes a reference to a
    /// command_succeeded_event which will only contain valid data for the duration of the callback.
    ///
    /// @param command_succeeded
    ///   The command succeeded monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_command_succeeded(
        std::function<void(const mongocxx::events::command_succeeded_event&)> command_succeeded);

    ///
    /// Retrieves the command succeeded monitoring callback.
    ///
    /// @return The command succeeded monitoring callback.
    ///
    const std::function<void(const mongocxx::events::command_succeeded_event&)>& command_succeeded()
        const;

    ///
    /// Set the server opening monitoring callback. The callback takes a reference to a
    /// server_opening_event which will only contain valid data for the duration of the callback.
    ///
    /// @param server_opening
    ///   The server opening monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_server_opening(
        std::function<void(const mongocxx::events::server_opening_event&)> server_opening);

    ///
    /// Retrieves the server opening monitoring callback.
    ///
    /// @return The server opening monitoring callback.
    ///
    const std::function<void(const mongocxx::events::server_opening_event&)>& server_opening()
        const;

    ///
    /// Set the server closed monitoring callback. The callback takes a reference to a
    /// server_closed_event which will only contain valid data for the duration of the callback.
    ///
    /// @param server_closed
    ///   The server closed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_server_closed(
        std::function<void(const mongocxx::events::server_closed_event&)> server_closed);

    ///
    /// Retrieves the server closed monitoring callback.
    ///
    /// @return The server closed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::server_closed_event&)>& server_closed() const;

    ///
    /// Set the server description changed monitoring callback. The callback takes a reference to a
    /// server_changed_event which will only contain valid data for the duration of the
    /// callback.
    ///
    /// @param server_changed
    ///   The server description changed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_server_changed(
        std::function<void(const mongocxx::events::server_changed_event&)> server_changed);

    ///
    /// Retrieves the server description changed monitoring callback.
    ///
    /// @return The server description changed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::server_changed_event&)>& server_changed()
        const;

    ///
    /// Set the topology_opening monitoring callback. The callback takes a reference to a
    /// topology_opening_event which will only contain valid data for the duration of the callback.
    ///
    /// @param topology_opening
    ///   The topology_opening monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_topology_opening(
        std::function<void(const mongocxx::events::topology_opening_event&)> topology_opening);

    ///
    /// Retrieves the topology_opening monitoring callback.
    ///
    /// @return The topology_opening monitoring callback.
    ///
    const std::function<void(const mongocxx::events::topology_opening_event&)>& topology_opening()
        const;

    ///
    /// Set the topology closed monitoring callback. The callback takes a reference to a
    /// topology_closed_event which will only contain valid data for the duration of the callback.
    ///
    /// @param topology_closed
    ///   The topology closed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_topology_closed(
        std::function<void(const mongocxx::events::topology_closed_event&)> topology_closed);

    ///
    /// Retrieves the topology closed monitoring callback.
    ///
    /// @return The topology closed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::topology_closed_event&)>& topology_closed()
        const;

    ///
    /// Set the topology description changed monitoring callback. The callback takes a reference to
    /// a
    /// topology_changed_event which will only contain valid data for the duration of
    /// the callback.
    ///
    /// @param topology_changed
    ///   The topology description changed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_topology_changed(
        std::function<void(const mongocxx::events::topology_changed_event&)> topology_changed);

    ///
    /// Retrieves the topology description changed monitoring callback.
    ///
    /// @return The topology description changed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::topology_changed_event&)>& topology_changed()
        const;

    ///
    /// Set the heartbeat started monitoring callback. The callback takes a reference to a
    /// heartbeat_started_event which will only contain valid data for the duration of the callback.
    ///
    /// @param heartbeat_started
    ///   The heartbeat started monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_heartbeat_started(
        std::function<void(const mongocxx::events::heartbeat_started_event&)> heartbeat_started);

    ///
    /// Retrieves the heartbeat started monitoring callback.
    ///
    /// @return The heartbeat started monitoring callback.
    ///
    const std::function<void(const mongocxx::events::heartbeat_started_event&)>& heartbeat_started()
        const;

    ///
    /// Set the heartbeat failed monitoring callback. The callback takes a reference to a
    /// heartbeat_failed_event which will only contain valid data for the duration of the callback.
    ///
    /// @param heartbeat_failed
    ///   The heartbeat failed monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_heartbeat_failed(
        std::function<void(const mongocxx::events::heartbeat_failed_event&)> heartbeat_failed);

    ///
    /// Retrieves the heartbeat failed monitoring callback.
    ///
    /// @return The heartbeat failed monitoring callback.
    ///
    const std::function<void(const mongocxx::events::heartbeat_failed_event&)>& heartbeat_failed()
        const;

    ///
    /// Set the heartbeat succeeded monitoring callback. The callback takes a reference to a
    /// heartbeat_succeeded_event which will only contain valid data for the duration of the
    /// callback.
    ///
    /// @param heartbeat_succeeded
    ///   The heartbeat succeeded monitoring callback.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    apm& on_heartbeat_succeeded(
        std::function<void(const mongocxx::events::heartbeat_succeeded_event&)>
            heartbeat_succeeded);

    ///
    /// Retrieves the heartbeat succeeded monitoring callback.
    ///
    /// @return The heartbeat succeeded monitoring callback.
    ///
    const std::function<void(const mongocxx::events::heartbeat_succeeded_event&)>&
    heartbeat_succeeded() const;

   private:
    std::function<void(const mongocxx::events::command_started_event&)> _command_started;
    std::function<void(const mongocxx::events::command_failed_event&)> _command_failed;
    std::function<void(const mongocxx::events::command_succeeded_event&)> _command_succeeded;
    std::function<void(const mongocxx::events::server_closed_event&)> _server_closed;
    std::function<void(const mongocxx::events::server_changed_event&)> _server_changed;
    std::function<void(const mongocxx::events::server_opening_event&)> _server_opening;
    std::function<void(const mongocxx::events::topology_closed_event&)> _topology_closed;
    std::function<void(const mongocxx::events::topology_changed_event&)> _topology_changed;
    std::function<void(const mongocxx::events::topology_opening_event&)> _topology_opening;
    std::function<void(const mongocxx::events::heartbeat_started_event&)> _heartbeat_started;
    std::function<void(const mongocxx::events::heartbeat_failed_event&)> _heartbeat_failed;
    std::function<void(const mongocxx::events::heartbeat_succeeded_event&)> _heartbeat_succeeded;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
