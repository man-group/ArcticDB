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

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/options/transaction.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class bulk_write;
class client;
class collection;
class database;
class uri;

///
/// Class representing the server-side requirement for reporting the success of a write
/// operation. The strength of the write concern setting determines the level of guarantees
/// that you will receive from MongoDB regarding write durability.
///
/// Weaker requirements that provide fewer guarantees report on success quickly while stronger
/// requirements that provide greater guarantees will take longer (or potentially forever, if
/// the write_concern's requirements are not satisfied and no timeout is set).
///
/// MongoDB drivers allow for different levels of write concern to better address the specific
/// needs of applications. Clients may adjust write concern to ensure that the most important
/// operations persist successfully to an entire MongoDB deployment. However, for other less
/// critical operations, clients can adjust the write concern to ensure better performance
/// rather than persistence to the entire deployment.
///
/// @see https://docs.mongodb.com/manual/core/write-concern/
///
class MONGOCXX_API write_concern {
   public:
    ///
    /// A class to represent the special case values for write_concern::nodes.
    /// @see https://docs.mongodb.com/manual/reference/write-concern/#w-option
    ///
    enum class level { k_default, k_majority, k_tag, k_unacknowledged, k_acknowledged };

    ///
    /// Constructs a new write_concern.
    ///
    write_concern();

    ///
    /// Copy constructs a write_concern.
    ///
    write_concern(const write_concern&);

    ///
    /// Copy assigns a write_concern.
    ///
    write_concern& operator=(const write_concern&);

    ///
    /// Move constructs a write_concern.
    ///
    write_concern(write_concern&&) noexcept;

    ///
    /// Move assigns a write_concern.
    ///
    write_concern& operator=(write_concern&&) noexcept;

    ///
    /// Destroys a write_concern.
    ///
    ~write_concern();

    ///
    /// Sets the journal parameter for this write concern.
    ///
    /// @param journal
    ///   If @c true confirms that the mongod instance has written the data to the on-disk journal
    ///   before reporting a write operations was successful. This ensures that data is not lost if
    ///   the mongod instance shuts down unexpectedly.
    ///
    void journal(bool journal);

    ///
    /// Sets the number of nodes that are required to acknowledge the write before the operation is
    /// considered successful. Write operations will block until they have been replicated to the
    /// specified number of servers in a replica set.
    ///
    /// @param confirm_from
    ///   The number of replica set nodes that must acknowledge the write.
    ///
    /// @warning Setting the number of nodes to 0 disables write acknowledgment and all other
    /// write concern options.
    ///
    /// @warning Setting the number of nodes required to an amount greater than the number of
    /// available nodes will cause writes using this write concern to block forever if no timeout
    /// is set.
    ///
    void nodes(std::int32_t confirm_from);

    ///
    /// Sets the acknowledge level.
    /// @see https://docs.mongodb.com/manual/reference/write-concern/#w-option
    ///
    /// @param confirm_level
    ///   Either level::k_unacknowledged, level::k_acknowledged, level::k_default, or
    ///   level::k_majority.
    ///
    /// @note
    ///   the acknowledge level of level::k_tag is set automatically when a tag is set.
    ///
    /// @warning
    ///   Setting this to level::k_unacknowledged disables write acknowledgment and all other
    ///   write concern options.
    ///
    /// @warning
    ///   Unacknowledged writes using dollar-prefixed or dotted keys may be silently rejected by
    ///   pre-5.0 servers.
    ///
    /// @exception
    ///   Throws mongocxx::exception for setting a tag acknowledge level. Use tag() instead.
    ///
    void acknowledge_level(level confirm_level);

    ///
    /// Requires that a majority of the nodes in a replica set acknowledge a write operation before
    /// it is considered a success.
    ///
    /// @param timeout
    ///   The amount of time to wait before the write operation times out if it cannot reach
    ///   the majority of nodes in the replica set. If the value is zero, then no timeout is set.
    ///
    /// @throws mongocxx::logic_error for an invalid timeout value.
    ///
    void majority(std::chrono::milliseconds timeout);

    ///
    /// Sets the name representing the server-side getLastErrorMode entry containing the list of
    /// nodes that must acknowledge a write operation before it is considered a success.
    ///
    /// @note the acknowledge level of level::k_tag is set automatically when a tag is set.
    ///
    /// @param tag
    ///   The string representing on of the "getLastErrorModes" in the replica set configuration.
    ///
    void tag(stdx::string_view tag);

    ///
    /// Sets an upper bound on the time a write concern can take to be satisfied. If the write
    /// concern cannot be satisfied within the timeout, the operation is considered a failure.
    ///
    /// @param timeout
    ///   The timeout (in milliseconds) for this write concern. If the value is zero, then no
    ///   timeout is set.
    ///
    /// @throws mongocxx::logic_error for an invalid timeout value.
    ///
    void timeout(std::chrono::milliseconds timeout);

    ///
    /// Gets the current status of the journal parameter.
    ///
    /// @return @c true if journal is required, @c false if not.
    ///
    bool journal() const;

    ///
    /// Gets the current number of nodes that this write_concern requires operations to reach.
    /// This value will be unset if the acknowledge_level is set to majority, default, or tag.
    ///
    /// This is unset by default.
    ///
    /// @see https://docs.mongodb.com/manual/reference/write-concern/#w-option
    ///
    /// @return The number of required nodes.
    ///
    stdx::optional<std::int32_t> nodes() const;

    ///
    /// Gets the current acknowledgment level.
    ///
    /// @see https://docs.mongodb.com/manual/reference/write-concern/#w-option
    ///
    /// @return The acknowledgment level.
    ///
    level acknowledge_level() const;

    ///
    /// Gets the current getLastErrorMode that is required by this write_concern.
    ///
    /// @return The current getLastErrorMode.
    ///
    stdx::optional<std::string> tag() const;

    ///
    /// Gets whether the majority of nodes is currently required by this write_concern.
    ///
    /// @return The current majority setting.
    ///
    bool majority() const;

    ///
    /// Gets the current timeout for this write_concern.
    ///
    /// @return Current timeout in milliseconds.
    ///
    std::chrono::milliseconds timeout() const;

    ///
    /// Gets whether this write_concern requires an acknowledged write.
    ///
    /// @return Whether this write concern requires an acknowledged write.
    ///
    bool is_acknowledged() const;

    ///
    /// Gets the document form of this write_concern.
    ///
    /// @return
    ///   Document representation of this write_concern.
    ///
    bsoncxx::document::value to_document() const;

   private:
    friend bulk_write;
    friend client;
    friend collection;
    friend database;
    /// \relates mongocxx::options::transaction
    friend mongocxx::options::transaction;
    friend uri;

    ///
    /// @{
    ///
    /// Compares two write_concern objects for (in)-equality.
    ///
    /// @relates: write_concern
    ///
    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const write_concern&, const write_concern&);
    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const write_concern&, const write_concern&);
    ///
    /// @}
    ///

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE write_concern(std::unique_ptr<impl>&& implementation);

    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
