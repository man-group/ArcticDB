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

#include <mongocxx/options/find_one_and_delete-fwd.hpp>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/hint.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing the optional arguments to a MongoDB find_and_modify delete operation
///
class find_one_and_delete {
   public:
    /// Sets the collation for this operation.
    ///
    /// @param collation
    ///   The new collation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    find_one_and_delete& collation(bsoncxx::v_noabi::document::view_or_value collation);

    ///
    /// Retrieves the current collation for this operation.
    ///
    /// @return
    ///   The current collation.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& collation() const;

    ///
    /// Sets the maximum amount of time for this operation to run (server-side) in milliseconds.
    ///
    /// @param max_time
    ///   The max amount of running time (in milliseconds).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    find_one_and_delete& max_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return the current max time (in milliseconds).
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const stdx::optional<std::chrono::milliseconds>& max_time() const;

    ///
    /// Sets a projection that limits the fields to return.
    ///
    /// @param projection
    ///   The projection document.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    find_one_and_delete& projection(bsoncxx::v_noabi::document::view_or_value projection);

    ///
    /// Gets the current projection set on this operation.
    ///
    /// @return The current projection.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& projection() const;

    ///
    /// Sets the order to search for a matching document.
    ///
    /// @warning This can influence which document the operation modifies if the provided filter
    /// selects multiple documents.
    ///
    /// @param ordering
    ///   Document describing the order of the documents to be returned.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    find_one_and_delete& sort(bsoncxx::v_noabi::document::view_or_value ordering);

    ///
    /// Gets the current sort ordering.
    ///
    /// @return The current sort ordering.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& sort() const;

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
    find_one_and_delete& write_concern(mongocxx::v_noabi::write_concern write_concern);

    ///
    /// Gets the current write concern.
    ///
    /// @return
    ///   The current write concern.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    ///
    const stdx::optional<mongocxx::v_noabi::write_concern>& write_concern() const;

    ///
    /// Sets the index to use for this operation.
    ///
    /// @note if the server already has a cached shape for this query, it may
    /// ignore a hint.
    ///
    /// @param index_hint
    ///   Object representing the index to use.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    find_one_and_delete& hint(mongocxx::v_noabi::hint index_hint);

    ///
    /// Gets the current hint.
    ///
    /// @return The current hint, if one is set.
    ///
    const stdx::optional<mongocxx::v_noabi::hint>& hint() const;

    ///
    /// Set the value of the let option.
    ///
    /// @param let
    ///   The new let option.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    find_one_and_delete& let(bsoncxx::v_noabi::document::view_or_value let);

    ///
    /// Gets the current value of the let option.
    ///
    /// @return
    ///  The current let option.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value> let() const;

    ///
    /// Set the value of the comment option.
    ///
    /// @param comment
    ///   The new comment option.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    find_one_and_delete& comment(bsoncxx::v_noabi::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return
    ///  The current comment option.
    ///
    const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> comment() const;

   private:
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _collation;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _projection;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _ordering;
    stdx::optional<mongocxx::v_noabi::write_concern> _write_concern;
    stdx::optional<mongocxx::v_noabi::hint> _hint;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _let;
    stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> _comment;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
