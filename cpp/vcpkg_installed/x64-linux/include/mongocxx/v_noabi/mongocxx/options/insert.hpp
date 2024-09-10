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

#include <mongocxx/options/insert-fwd.hpp>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing the optional arguments to a MongoDB insert operation
///
class insert {
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
    insert& bypass_document_validation(bool bypass_document_validation);

    ///
    /// Gets the current value of the bypass_document_validation option.
    ///
    /// @return The optional value of the bypass_document_validation option.
    ///
    const stdx::optional<bool>& bypass_document_validation() const;

    ///
    /// Sets the write_concern for this operation.
    ///
    /// @param wc
    ///   The new write_concern.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/write-concern/
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    insert& write_concern(mongocxx::v_noabi::write_concern wc);

    ///
    /// The current write_concern for this operation.
    ///
    /// @return The current write_concern.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/write-concern/
    ///
    const stdx::optional<mongocxx::v_noabi::write_concern>& write_concern() const;

    ///
    /// @note: This applies only to insert_many and is ignored for insert_one.
    ///
    /// If true, when an insert fails, return without performing the remaining
    /// writes. If false, when a write fails, continue with the remaining
    /// writes, if any. Inserts can be performed in any order if this is false.
    /// Defaults to true.
    ///
    /// @param ordered
    ///   Whether or not the insert_many will be ordered.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/insert/
    ///
    insert& ordered(bool ordered);

    ///
    /// The current ordered value for this operation.
    ///
    /// @return The current ordered value.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/insert/
    ///
    const stdx::optional<bool>& ordered() const;

    ///
    /// Sets the comment for this operation.
    ///
    /// @param comment
    ///   The new comment.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/insert/
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    insert& comment(bsoncxx::v_noabi::types::bson_value::view_or_value comment);

    ///
    /// The current comment for this operation.
    ///
    /// @return The current comment.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/command/insert/
    ///
    const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& comment() const;

   private:
    stdx::optional<mongocxx::v_noabi::write_concern> _write_concern;
    stdx::optional<bool> _ordered;
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> _comment;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
