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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/hint.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB aggregation operation.
///
class MONGOCXX_API aggregate {
   public:
    ///
    /// Enables writing to temporary files. When set to @c true, aggregation stages can write data
    /// to the _tmp subdirectory in the dbPath directory. The server-side default is @c false.
    ///
    /// @param allow_disk_use
    ///   Whether or not to allow disk use.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& allow_disk_use(bool allow_disk_use);

    ///
    /// Retrieves the current allow_disk_use setting.
    ///
    /// @return Whether disk use is allowed.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bool>& allow_disk_use() const;

    ///
    /// Sets the number of documents to return per batch.
    ///
    /// @param batch_size
    ///   The size of the batches to request.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& batch_size(std::int32_t batch_size);

    ///
    /// The current batch size setting.
    ///
    /// @return The current batch size.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<std::int32_t>& batch_size() const;

    ///
    /// Sets the collation for this operation.
    ///
    /// @param collation
    ///   The new collation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& collation(bsoncxx::document::view_or_value collation);

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
    /// Sets the variable mapping for this operation.
    ///
    /// @param let
    ///   The new variable mapping.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& let(bsoncxx::document::view_or_value let);

    ///
    /// Retrieves the current variable mapping for this operation.
    ///
    /// @return
    ///   The current variable mapping.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& let() const;

    ///
    /// Sets the maximum amount of time for this operation to run server-side in milliseconds.
    ///
    /// @param max_time
    ///   The max amount of time (in milliseconds).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& max_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return
    ///   The current max time (in milliseconds).
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<std::chrono::milliseconds>& max_time() const;

    ///
    /// Sets the read_preference for this operation.
    ///
    /// @param rp the new read_preference
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    aggregate& read_preference(class read_preference rp);

    ///
    /// The current read_preference for this operation.
    ///
    /// @return the current read_preference
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class read_preference>& read_preference() const;

    ///
    /// Sets whether the $out stage should bypass document validation.
    ///
    /// @param bypass_document_validation whether or not to bypass validation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    aggregate& bypass_document_validation(bool bypass_document_validation);

    ///
    /// The current bypass_document_validation setting.
    ///
    /// @return the current bypass_document_validation setting
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bool>& bypass_document_validation() const;

    ///
    /// Sets the index to use for this operation.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
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
    aggregate& hint(class hint index_hint);

    ///
    /// Gets the current hint.
    ///
    /// @return The current hint, if one is set.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class hint>& hint() const;

    ///
    /// Sets the write concern to use for this operation. Only has an effect if $out is a part of
    /// the pipeline.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    /// @param write_concern
    ///   Object representing the write_concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    aggregate& write_concern(class write_concern write_concern);

    ///
    /// Gets the current write concern.
    ///
    /// @return
    ///   The current write concern, if it is set.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class write_concern>& write_concern() const;

    ///
    /// Sets the read concern to use for this operation.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    /// @param read_concern
    ///   Object representing the read_concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.
    ///
    aggregate& read_concern(class read_concern read_concern);

    ///
    /// Gets the current read concern.
    ///
    /// @return
    ///   The current read concern, if it is set.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<class read_concern>& read_concern() const;

    ///
    /// Sets the comment to use for this operation.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    /// @param comment
    ///   Object representing the comment.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.
    ///
    aggregate& comment(bsoncxx::types::bson_value::view_or_value comment);

    ///
    /// Gets the current comment.
    ///
    /// @return
    ///   The current comment, if it is set.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/aggregate/
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value>& comment() const;

   private:
    friend class ::mongocxx::database;
    friend class ::mongocxx::collection;

    void append(bsoncxx::builder::basic::document& builder) const;

    stdx::optional<bool> _allow_disk_use;
    stdx::optional<std::int32_t> _batch_size;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<bsoncxx::document::view_or_value> _let;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<class read_preference> _read_preference;
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<class hint> _hint;
    stdx::optional<class write_concern> _write_concern;
    stdx::optional<class read_concern> _read_concern;
    stdx::optional<bsoncxx::types::bson_value::view_or_value> _comment;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
