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

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB bulk write
///
class MONGOCXX_API bulk_write {
   public:
    ///
    /// Constructs a new bulk_write object. By default, bulk writes are considered ordered
    /// as this is the only safe choice. If you want an unordered update, you must call
    /// ordered(false) to switch to unordered mode.
    ///
    bulk_write();

    ///
    /// Sets whether the writes must be executed in order by the server.
    ///
    /// The server-side default is @c true.
    ///
    /// @param ordered
    ///   If @c true all write operations will be executed serially in the order they were appended,
    ///   and the entire bulk operation will abort on the first error. If @c false operations will
    ///   be executed in arbitrary order (possibly in parallel on the server) and any errors will be
    ///   reported after attempting all operations.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    bulk_write& ordered(bool ordered);

    ///
    /// Gets the current value of the ordered option.
    ///
    /// @return The value of the ordered option.
    ///
    bool ordered() const;

    ///
    /// Sets the write_concern for this operation.
    ///
    /// @param wc
    ///   The new write_concern.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/write-concern/
    ///
    bulk_write& write_concern(class write_concern wc);

    ///
    /// The current write_concern for this operation.
    ///
    /// @return
    ///   The current write_concern.
    ///
    /// @see https://docs.mongodb.com/manual/core/write-concern/
    ///
    const stdx::optional<class write_concern>& write_concern() const;

    ///
    /// Set whether or not to bypass document validation for this operation.
    ///
    /// @param bypass_document_validation
    ///   Whether or not to bypass document validation.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    bulk_write& bypass_document_validation(bool bypass_document_validation);

    ///
    /// The current setting for bypassing document validation for this operation.
    ///
    /// @return
    ///  The current document validation bypass setting.
    ///
    const stdx::optional<bool> bypass_document_validation() const;

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
    bulk_write& let(bsoncxx::document::view_or_value let);

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
    bulk_write& comment(bsoncxx::types::bson_value::view_or_value comment);

    ///
    /// Gets the current value of the comment option.
    ///
    /// @return
    ///  The current comment option.
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value> comment() const;

   private:
    bool _ordered;
    stdx::optional<class write_concern> _write_concern;
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::document::view_or_value> _let;
    stdx::optional<bsoncxx::types::bson_value::view_or_value> _comment;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
