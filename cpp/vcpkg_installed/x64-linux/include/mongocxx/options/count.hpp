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
#include <string>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/hint.hpp>
#include <mongocxx/read_preference.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

namespace options {

///
/// Class representing the optional arguments to mongocxx::collection::count_documents
///
class MONGOCXX_API count {
   public:
    ///
    /// Sets the collation for this operation.
    ///
    /// @param collation
    ///   The new collation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& collation(bsoncxx::document::view_or_value collation);

    ///
    /// Retrieves the current collation for this operation.
    ///
    /// @return
    ///   The current collation.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;

    ///
    /// Sets the index to use for this operation.
    ///
    /// @param index_hint
    ///   Object representing the index to use.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& hint(class hint index_hint);

    ///
    /// Gets the current hint.
    ///
    /// @return The current hint, if one is set.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class hint>& hint() const;

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
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& comment(bsoncxx::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return The current comment option.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value>& comment() const;

    ///
    /// Sets the maximum number of documents to count.
    ///
    /// @param limit
    ///  The max number of documents to count.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& limit(std::int64_t limit);

    ///
    /// Gets the current limit.
    ///
    /// @return The current limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<std::int64_t>& limit() const;

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
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& max_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return The current max time (in milliseconds).
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<std::chrono::milliseconds>& max_time() const;

    ///
    /// Sets the number of documents to skip before counting documents.
    ///
    /// @param skip
    ///   The number of documents to skip.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& skip(std::int64_t skip);

    ///
    /// Gets the current number of documents to skip.
    ///
    /// @return The number of documents to skip.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<std::int64_t>& skip() const;

    ///
    /// Sets the read_preference for this operation.
    ///
    /// @param rp
    ///   The new read_preference.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    count& read_preference(class read_preference rp);

    ///
    /// The current read_preference for this operation.
    ///
    /// @return the current read_preference
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class read_preference>& read_preference() const;

   private:
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<class hint> _hint;
    stdx::optional<bsoncxx::types::bson_value::view_or_value> _comment;
    stdx::optional<std::int64_t> _limit;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<std::int64_t> _skip;
    stdx::optional<class read_preference> _read_preference;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
