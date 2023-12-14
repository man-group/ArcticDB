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

#include <vector>

#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/events/server_description.hpp>
#include <mongocxx/read_preference.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

namespace events {

using mongocxx::read_preference;

///
/// Class representing what the driver knows about a topology of MongoDB servers: either a
/// standalone, a replica set, or a sharded cluster.
///
class MONGOCXX_API topology_description {
   public:
    ///
    /// An array of server_description instances.
    ///
    class MONGOCXX_API server_descriptions {
       private:
        using container = std::vector<server_description>;

       public:
        ///
        /// Move constructs a server_descriptions array.
        ///
        server_descriptions(server_descriptions&&) noexcept;

        ///
        /// Move assigns a server_descriptions array.
        ///
        server_descriptions& operator=(server_descriptions&&) noexcept;

        server_descriptions(const server_descriptions&) = delete;
        server_descriptions& operator=(const server_descriptions&) = delete;

        ///
        /// Destroys a server_descriptions array.
        ///
        ~server_descriptions();

        ///
        /// The array's iterator type.
        ///
        using iterator = container::iterator;

        ///
        /// The array's const iterator type.
        ///
        using const_iterator = container::const_iterator;

        ///
        /// @{
        ///
        /// Returns an iterator to the beginning.
        ///
        iterator begin() noexcept;
        const_iterator begin() const noexcept;

        ///
        /// @}
        ///

        ///
        /// @{
        ///
        /// Returns an iterator to the end.
        ///
        iterator end() noexcept;
        const_iterator end() const noexcept;

        ///
        /// @}
        ///

        ///
        /// The number of server_description instances in the array.
        ///
        std::size_t size() const noexcept;

       private:
        friend topology_description;
        MONGOCXX_PRIVATE explicit server_descriptions(void* sds, std::size_t size);
        MONGOCXX_PRIVATE void swap(server_descriptions& other) noexcept;
        container _container;
        void* _sds;
        std::size_t _size;
    };

    MONGOCXX_PRIVATE explicit topology_description(void* event);

    ///
    /// Destroys a topology_description.
    ///
    ~topology_description();

    ///
    /// The topology type: "Unknown", "Sharded", "ReplicaSetNoPrimary", "ReplicaSetWithPrimary", or
    /// "Single".
    ///
    /// @return The type as a short-lived string view.
    ///
    bsoncxx::stdx::string_view type() const;

    ///
    /// Determines if the topology has a readable server available. Servers are
    /// filtered by the given read preferences only if the driver is connected
    /// to a replica set, otherwise the read preferences are ignored. This
    /// function uses the driver's current knowledge of the state of the
    /// MongoDB server or servers it is connected to; it does no I/O.
    ///
    /// @return Whether there is a readable server available.
    ///
    bool has_readable_server(const mongocxx::read_preference& pref) const;

    ///
    /// Determines if the topology has a writable server available, such as a
    /// primary, mongos, or standalone. This function uses the driver's current
    /// knowledge of the state of the MongoDB server or servers it is connected
    /// to; it does no I/O.
    ///
    /// @return Whether there is a writable server available.
    ///
    bool has_writable_server() const;

    ///
    /// Fetches descriptions for all known servers in the topology.
    ///
    /// @return An array of server_description objects.
    ///
    server_descriptions servers() const;

   private:
    // Non-const since mongoc_topology_description_has_readable_server/writable_server take
    // non-const. They do server selection, which modifies the mongoc_topology_description_t.
    void* _td;
};

}  // namespace events
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
