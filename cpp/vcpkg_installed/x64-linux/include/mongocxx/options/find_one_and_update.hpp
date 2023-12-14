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

#include <bsoncxx/array/view_or_value.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/hint.hpp>
#include <mongocxx/options/find_one_common_options.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB find_and_modify update operation.
///
class MONGOCXX_API find_one_and_update {
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
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& collation(bsoncxx::document::view_or_value collation);

    ///
    /// Retrieves the current collation for this operation.
    ///
    /// @return
    ///   The current collation.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;

    ///
    /// Whether or not to bypass document validation for this operation.
    ///
    /// @note
    ///   On servers >= 3.2, the server applies validation by default. On servers < 3.2, this option
    ///   is ignored.
    ///
    /// @param bypass_document_validation
    ///   Whether or not to bypass document validation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& bypass_document_validation(bool bypass_document_validation);

    ///
    /// The current setting for bypassing document validation.
    ///
    /// @return the current bypass document validation setting.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bool>& bypass_document_validation() const;

    ///
    /// Sets the index to use for this operation.
    ///
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
    find_one_and_update& hint(class hint index_hint);

    ///
    /// Gets the current hint.
    ///
    /// @return The current hint, if one is set.
    ///
    const stdx::optional<class hint>& hint() const;

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
    find_one_and_update& let(bsoncxx::document::view_or_value let);

    ///
    /// Gets the current value of the let option.
    ///
    /// @return
    ///  The current let option.
    ///
    const stdx::optional<bsoncxx::document::view_or_value> let() const;

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
    find_one_and_update& comment(bsoncxx::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return
    ///  The current comment option.
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value> comment() const;

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
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& max_time(std::chrono::milliseconds max_time);

    ///
    /// The current max_time setting.
    ///
    /// @return the current max allowed running time (in milliseconds).
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<std::chrono::milliseconds>& max_time() const;

    ///
    /// Sets a projection, which limits the fields to return.
    ///
    /// @param projection
    ///   The projection document.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& projection(bsoncxx::document::view_or_value projection);

    ///
    /// Gets the current projection for this operation.
    ///
    /// @return The current projection.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& projection() const;

    ///
    /// Set the desired version of the updated document to return, either the original
    /// document, or the updated. By default, the original document is returned.
    ///
    /// @param return_document
    ///   Version of document to return, either original or updated.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    /// @see mongocxx::options::return_document
    ///
    find_one_and_update& return_document(return_document return_document);

    ///
    /// Which version of the updated document to return.
    ///
    /// @return Version of document to return, either original or updated.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    /// @see mongocxx::options::return_document
    ///
    const stdx::optional<mongocxx::options::return_document>& return_document() const;

    ///
    /// Sets the order by which to search the collection for a matching document.
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
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& sort(bsoncxx::document::view_or_value ordering);

    ///
    /// Gets the current sort ordering.
    ///
    /// @return The current sort ordering.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& sort() const;

    ///
    /// Sets the upsert flag on the operation. When @c true, the operation creates a new document if
    /// no document matches the filter. When @c false, this operation will do nothing if there are
    /// no matching documents. The server-side default is false.
    ///
    /// @param upsert
    ///   Whether or not to perform an upsert.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& upsert(bool upsert);

    ///
    /// Gets the current upsert setting.
    ///
    /// @return The current upsert setting.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bool>& upsert() const;

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
    ///   https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& write_concern(mongocxx::write_concern write_concern);

    ///
    /// Gets the current write concern.
    ///
    /// @return
    ///   The current write concern.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<mongocxx::write_concern>& write_concern() const;

    ///
    /// Set array filters for this operation.
    ///
    /// @param array_filters
    ///   Array representing filters determining which array elements to modify.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    find_one_and_update& array_filters(bsoncxx::array::view_or_value array_filters);

    ///
    /// Get array filters for this operation.
    ///
    /// @return
    ///   The current array filters.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/findAndModify/
    ///
    const stdx::optional<bsoncxx::array::view_or_value>& array_filters() const;

   private:
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<class hint> _hint;
    stdx::optional<bsoncxx::document::view_or_value> _let;
    stdx::optional<bsoncxx::types::bson_value::view_or_value> _comment;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<bsoncxx::document::view_or_value> _projection;
    stdx::optional<mongocxx::options::return_document> _return_document;
    stdx::optional<bsoncxx::document::view_or_value> _ordering;
    stdx::optional<bool> _upsert;
    stdx::optional<mongocxx::write_concern> _write_concern;
    stdx::optional<bsoncxx::array::view_or_value> _array_filters;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
