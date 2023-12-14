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

#include <functional>
#include <memory>

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/options/pool.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;

///
/// A pool of @c client objects associated with a MongoDB deployment.
///
/// For interoperability with other MongoDB drivers, the minimum and maximum number of connections
/// in the pool is configured using the 'minPoolSize' and 'maxPoolSize' connection string options.
///
/// @see https://docs.mongodb.com/manual/reference/connection-string/#connection-string-options
///
/// @remark When connecting to a replica set, it is @b much more efficient to use a pool as opposed
/// to manually constructing @c client objects. The pool will use a single background thread per
/// server to monitor the topology of the replica set, all of which are shared between the client
/// objects created by the pool. A standalone client will instead "stop the world" every 60 seconds
/// to check the status of the cluster. Because of this, if multiple threads are available, a
/// connection pool should be used even if the application itself is single-threaded.
///
class MONGOCXX_API pool {
   public:
    ///
    /// Creates a pool associated with a connection string.
    ///
    /// @param mongodb_uri
    ///  A MongoDB URI representing the connection parameters
    /// @param options
    ///  Options to use when connecting to the MongoDB deployment.
    ///
    /// @throws mongocxx::exception if invalid options are provided (whether from the URI or
    ///  provided client options).
    explicit pool(const uri& mongodb_uri = mongocxx::uri(),
                  const options::pool& options = options::pool());

    ///
    /// Destroys a pool.
    ///
    ~pool();

    ///
    /// An entry is a handle on a @c client object acquired via the pool. Similar to
    /// std::unique_ptr.
    ///
    /// @note The lifetime of any entry object must be a subset of the pool object
    ///  from which it was acquired.
    ///
    class MONGOCXX_API entry {
       public:
        /// Access a member of the client instance.
        client* operator->() const& noexcept;
        client* operator->() && = delete;

        /// Retrieve a reference to the client.
        client& operator*() const& noexcept;
        client& operator*() && = delete;

        /// Assign nullptr to this entry to release its client to the pool.
        entry& operator=(std::nullptr_t) noexcept;

        /// Return true if this entry has a client acquired from the pool.
        explicit operator bool() const noexcept;

       private:
        friend class pool;

        using unique_client = std::unique_ptr<client, std::function<void MONGOCXX_CALL(client*)>>;

        MONGOCXX_PRIVATE explicit entry(unique_client);

        unique_client _client;
    };

    ///
    /// Acquires a client from the pool. The calling thread will block until a connection is
    /// available.
    ///
    entry acquire();

    ///
    /// Acquires a client from the pool. This method will return immediately, but may return a
    /// disengaged optional if a client is not available.
    ///
    stdx::optional<entry> try_acquire();

   private:
    friend class options::auto_encryption;

    MONGOCXX_PRIVATE void _release(client* client);

    class MONGOCXX_PRIVATE impl;
    const std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
