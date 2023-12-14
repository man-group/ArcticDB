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
#include <mongocxx/hint.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace model {

///
/// Class representing a MongoDB delete operation that removes a single document.
///
class MONGOCXX_API delete_one {
   public:
    ///
    /// Constructs a delete operation that will delete the first document matching the filter.
    ///
    /// @param filter
    ///   Document representing the criteria for deletion.
    ///
    delete_one(bsoncxx::document::view_or_value filter);

    ///
    /// Gets the filter on this delete operation.
    ///
    /// @return The filter to be used for the delete operation.
    ///
    const bsoncxx::document::view_or_value& filter() const;

    ///
    /// Sets the collation for this delete operation.
    ///
    /// @param collation
    ///   The new collation.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/collation/
    ///
    delete_one& collation(bsoncxx::document::view_or_value collation);

    ///
    /// Gets the collation option for this delete operation.
    ///
    /// @return
    ///   The optional value of the collation option.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/collation/
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;

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
    delete_one& hint(class hint index_hint);

    ///
    /// Gets the current hint.
    ///
    /// @return The current hint, if one is set.
    ///
    const stdx::optional<class hint>& hint() const;

   private:
    bsoncxx::document::view_or_value _filter;

    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<class hint> _hint;
};

}  // namespace model
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
