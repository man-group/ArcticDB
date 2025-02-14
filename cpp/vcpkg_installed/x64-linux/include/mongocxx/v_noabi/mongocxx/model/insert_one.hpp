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

#include <mongocxx/model/insert_one-fwd.hpp>

#include <bsoncxx/document/view_or_value.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace model {

///
/// Class representing a MongoDB insert operation that creates a single document.
///
class insert_one {
   public:
    ///
    /// Constructs an insert operation that will create a single document.
    ///
    /// @param document
    ///   The document to insert.
    ///
    insert_one(bsoncxx::v_noabi::document::view_or_value document);

    ///
    /// Gets the document to be inserted.
    ///
    /// @return The document to be inserted.
    ///
    const bsoncxx::v_noabi::document::view_or_value& document() const;

   private:
    bsoncxx::v_noabi::document::view_or_value _document;
};

}  // namespace model
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
