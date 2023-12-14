// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing a hint to be passed to a database operation.
///
class MONGOCXX_API hint {
   public:
    ///
    /// Constructs a new hint.
    ///
    /// Note: this constructor is purposefully not explicit, to allow conversion
    /// from either document::view or document::value to view_or_value.
    ///
    /// @param index
    ///   Document view or value representing the index to be used.
    ///
    hint(bsoncxx::document::view_or_value index);

    ///
    /// Constructs a new hint.
    ///
    /// @param index
    ///   String representing the name of the index to be used.
    ///
    explicit hint(bsoncxx::string::view_or_value index);

    ///
    /// @{
    ///
    /// Compare this hint to a string for (in)-equality
    ///
    /// @relates hint
    ///
    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const hint& index_hint, std::string index);

    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const hint& index_hint,
                                                      bsoncxx::document::view index);
    ///
    /// @}
    ///

    ///
    /// Returns a types::bson_value::view representing this hint.
    ///
    /// @return Hint, as a types::bson_value::view. The caller must ensure that the returned object
    /// not outlive
    /// the hint object that it was created from.
    ///
    bsoncxx::types::bson_value::view to_value() const;

    ///
    /// Returns a types::bson_value::view representing this hint.
    ///
    /// @return Hint, as a types::bson_value::view. The caller must ensure that the returned object
    /// not outlive
    /// the hint object that it was created from.
    ///
    MONGOCXX_INLINE operator bsoncxx::types::bson_value::view() const;

   private:
    stdx::optional<bsoncxx::document::view_or_value> _index_doc;
    stdx::optional<bsoncxx::string::view_or_value> _index_string;
};

///
/// Convenience methods to compare for equality against an index name.
///
/// Return true if this hint contains an index name that matches.
///
/// @relates hint
///
MONGOCXX_API bool MONGOCXX_CALL operator==(std::string index, const hint& index_hint);

///
/// @{
///
/// Convenience methods to compare for inequality against an index name.
///
/// Return true if this hint contains an index name that matches.
///
/// @relates hint
///
MONGOCXX_API bool MONGOCXX_CALL operator!=(const hint& index_hint, std::string index);
MONGOCXX_API bool MONGOCXX_CALL operator!=(std::string index, const hint& index_index);
///
/// @}
///

///
/// Convenience methods to compare for equality against an index document.
///
/// Return true if this hint contains an index document that matches.
///
/// @relates hint
///
MONGOCXX_API bool MONGOCXX_CALL operator==(bsoncxx::document::view index, const hint& index_hint);

///
/// @{
///
/// Convenience methods to compare for equality against an index document.
///
/// Return true if this hint contains an index document that matches.
///
///
/// @relates hint
///
MONGOCXX_API bool MONGOCXX_CALL operator!=(const hint& index_hint, bsoncxx::document::view index);
MONGOCXX_API bool MONGOCXX_CALL operator!=(bsoncxx::document::view index, const hint& index_hint);
///
/// @}
///

MONGOCXX_INLINE hint::operator bsoncxx::types::bson_value::view() const {
    return to_value();
}

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
