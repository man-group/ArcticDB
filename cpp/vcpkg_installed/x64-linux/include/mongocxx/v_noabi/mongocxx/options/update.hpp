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

#include <mongocxx/options/update-fwd.hpp>

#include <bsoncxx/array/view_or_value.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/hint.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing the optional arguments to a MongoDB update operation.
///
class update {
   public:
    ///
    /// Sets the bypass_document_validation option.
    /// If true, allows the write to opt-out of document level validation.
    ///
    /// @note
    ///   On servers >= 3.2, the server applies validation by default. On servers < 3.2, this option
    ///   is ignored.
    ///
    /// @param bypass_document_validation
    ///   Whether or not to bypass document validation
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    update& bypass_document_validation(bool bypass_document_validation);

    ///
    /// Gets the current value of the bypass_document_validation option.
    ///
    /// @return The optional value of the bypass_document_validation option.
    ///
    const stdx::optional<bool>& bypass_document_validation() const;

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
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/collation/
    ///
    update& collation(bsoncxx::v_noabi::document::view_or_value collation);

    ///
    /// Retrieves the current collation for this operation.
    ///
    /// @return
    ///   The current collation.
    ///
    /// @see
    ///   https://www.mongodb.com/docs/manual/reference/collation/
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& collation() const;

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
    update& hint(mongocxx::v_noabi::hint index_hint);

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
    update& let(bsoncxx::v_noabi::document::view_or_value let);

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
    update& comment(bsoncxx::v_noabi::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return
    ///  The current comment option.
    ///
    const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> comment() const;

    ///
    /// Sets the upsert option.
    ///
    /// By default, if no document matches the filter, the update operation does nothing.
    /// However, by specifying upsert as @c true, this operation either updates matching documents
    /// or inserts a new document using the update specification if no matching document exists.
    ///
    /// @param upsert
    ///   If set to @c true, creates a new document when no document matches the query criteria.
    ///   The server-side default is @c false, which does not insert a new document if a match
    ///   is not found.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    update& upsert(bool upsert);

    ///
    /// Gets the current value of the upsert option.
    ///
    /// @return The optional value of the upsert option.
    ///
    const stdx::optional<bool>& upsert() const;

    ///
    /// Sets the write_concern for this operation.
    ///
    /// @param wc
    ///   The new write_concern
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/write-concern/
    ///
    update& write_concern(mongocxx::v_noabi::write_concern wc);

    ///
    /// The current write_concern for this operation.
    ///
    /// @return
    ///   The current write_concern
    ///
    /// @see https://www.mongodb.com/docs/manual/core/write-concern/
    ///
    const stdx::optional<mongocxx::v_noabi::write_concern>& write_concern() const;

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
    /// @see https://www.mongodb.com/docs/manual/reference/command/update/
    ///
    update& array_filters(bsoncxx::v_noabi::array::view_or_value array_filters);

    ///
    /// Get array filters for this operation.
    ///
    /// @return
    ///   The current array filters.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/update/
    ///
    const stdx::optional<bsoncxx::v_noabi::array::view_or_value>& array_filters() const;

   private:
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _collation;
    stdx::optional<bool> _upsert;
    stdx::optional<mongocxx::v_noabi::write_concern> _write_concern;
    stdx::optional<bsoncxx::v_noabi::array::view_or_value> _array_filters;
    stdx::optional<mongocxx::v_noabi::hint> _hint;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _let;
    stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> _comment;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
