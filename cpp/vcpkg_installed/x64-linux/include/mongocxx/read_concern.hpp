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

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/options/transaction.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;
class collection;
class database;
class uri;

///
/// A class to represent the read concern. Read concern can be set at the client, database, or
/// collection level. The read concern can also be provided via connection string, and will be
/// parsed and set on the client constructed for the URI.
///
/// For the WiredTiger storage engine, MongoDB 3.2 introduced the readConcern option for replica
/// sets and replica set shards. The readConcern option allows clients to choose a level of
/// isolation for their reads. You can specify a readConcern of "majority" to read data that has
/// been written to a majority of nodes and thus cannot be rolled back. By default, MongoDB uses a
/// readConcern of "local" which does not guarantee that the read data would not be rolled back.
///
/// MongoDB 3.4 introduces a read concern level of "linearizable" to read data that has been written
/// to a majority of nodes (i.e. cannot be rolled back) @b and is not stale. Linearizable read
/// concern is available for all MongoDB supported storage engines and applies to read operations on
/// a single document. Note that writes must be made with majority write concern in order for reads
/// to be linearizable.
///
/// @see https://docs.mongodb.com/manual/reference/read-concern/
///
class MONGOCXX_API read_concern {
   public:
    ///
    /// A class to represent the read concern level.
    ///
    /// @see https://docs.mongodb.com/manual/reference/read-concern/#read-concern-levels
    ///
    enum class level {
        k_local,
        k_majority,
        k_linearizable,
        k_server_default,
        k_unknown,
        k_available,
        k_snapshot
    };

    ///
    /// Constructs a new read_concern with default acknowledge_level of k_server_default.
    ///
    /// The k_server_default acknowledge level has an empty acknowledge_string. Queries that
    /// run with this read_concern will use the server's default read_concern instead of
    /// specifying one.
    ///
    read_concern();

    ///
    /// Copy constructs a read_concern.
    ///
    read_concern(const read_concern&);

    ///
    /// Copy assigns a read_concern.
    ///
    read_concern& operator=(const read_concern&);

    ///
    /// Move constructs a read_concern.
    ///
    read_concern(read_concern&&) noexcept;

    ///
    /// Move assigns a read_concern.
    ///
    read_concern& operator=(read_concern&&) noexcept;

    ///
    /// Destroys a read_concern.
    ///
    ~read_concern();

    ///
    /// Sets the read concern level.
    ///
    /// @param rc_level
    ///   Either k_local, k_majority, k_linearizable, or k_server_default.
    ///
    /// @throws
    ///   mongocxx::exception if rc_level is not k_local, k_majority, k_linearizable, or
    ///   k_server_default.
    ///
    void acknowledge_level(level rc_level);

    ///
    /// Gets the current read concern level.
    ///
    /// If this was set with acknowledge_string to anything other than "local", "majority",
    /// "linearizable", or an empty string, this will return k_unknown.
    ///
    /// @return The read concern level.
    ///
    level acknowledge_level() const;

    ///
    /// Sets the read concern string. Any valid read concern string (e.g. "local",
    /// "majority", "linearizable", "") may be passed in.  For forward-compatibility
    /// with read concern levels introduced in the future, no validation is performed on
    /// this string.
    ///
    /// @param rc_string
    ///   The read concern string.
    ///
    void acknowledge_string(stdx::string_view rc_string);

    ///
    /// Gets the current read concern string.
    ///
    /// If the read concern level was set with acknowledge_level, this will return either "local",
    /// "majority", "linearizable", or an empty string for k_server_default.
    ///
    /// @return The read concern string.
    ///
    stdx::string_view acknowledge_string() const;

    ///
    /// Gets the document form of this read_concern.
    ///
    /// @return
    ///   Document representation of this read_concern.
    ///
    bsoncxx::document::value to_document() const;

   private:
    friend client;
    friend collection;
    friend database;
    /// \relates mongocxx::options::transaction
    friend mongocxx::options::transaction;
    friend uri;

    ///
    /// @{
    ///
    /// Compares two read_concern objects for (in)-equality.
    ///
    /// @relates: read_concern
    ///
    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const read_concern&, const read_concern&);
    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const read_concern&, const read_concern&);
    ///
    /// @}
    ///

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE read_concern(std::unique_ptr<impl>&& implementation);

    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
