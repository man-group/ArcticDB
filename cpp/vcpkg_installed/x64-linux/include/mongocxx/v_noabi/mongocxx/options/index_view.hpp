// Copyright 2017 MongoDB Inc.
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
#include <string>

#include <mongocxx/options/index_view-fwd.hpp>

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing optional arguments to IndexView operations
///
class index_view {
   public:
    index_view();

    ///
    /// Sets the maximum amount of time for this operation to run (server-side) in milliseconds.
    ///
    /// @param max_time
    ///   The max amount of time (in milliseconds).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    index_view& max_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return
    ///   The current max allowed running time (in milliseconds).
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const bsoncxx::v_noabi::stdx::optional<std::chrono::milliseconds>& max_time() const;

    ///
    /// Sets the write concern for this operation.
    ///
    /// @param write_concern
    ///   Object representing the write concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    index_view& write_concern(mongocxx::v_noabi::write_concern write_concern);

    ///
    /// Gets the current write concern.
    ///
    /// @return
    ///   The current write concern.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const bsoncxx::v_noabi::stdx::optional<mongocxx::v_noabi::write_concern>& write_concern() const;

    ///
    /// Sets the commit quorum for this operation.
    ///
    /// This option may only be used with MongoDB version 4.4 or later.
    ///
    /// @param commit_quorum
    ///   Integer representing the minimum number of data-bearing voting replica set members (i.e.
    ///   commit quorum), including the primary, that must report a successful index build before
    ///   the primary marks the indexes as ready. A value of @c 0 disables quorum-voting behavior.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/createIndexes
    ///
    index_view& commit_quorum(std::int32_t commit_quorum);

    ///
    /// Sets the commit quorum for this operation.
    ///
    /// This option may only be used with MongoDB version 4.4 or later.
    ///
    /// @param commit_quorum
    ///   String representing the minimum number of data-bearing voting replica set members (i.e.
    ///   commit quorum), including the primary, that must report a successful index build before
    ///   the primary marks the indexes as ready.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/createIndexes
    ///
    index_view& commit_quorum(std::string commit_quorum);

    ///
    /// Gets the current commitQuorum setting.
    ///
    /// This option may only be used with MongoDB version 4.4 or later.
    ///
    /// @return
    ///   The current commitQuorum setting.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/createIndexes
    ///
    const stdx::optional<bsoncxx::v_noabi::document::value> commit_quorum() const;

   private:
    bsoncxx::v_noabi::stdx::optional<std::chrono::milliseconds> _max_time;
    bsoncxx::v_noabi::stdx::optional<mongocxx::v_noabi::write_concern> _write_concern;
    bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::document::value> _commit_quorum;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
