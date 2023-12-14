// Copyright 2017 MongoDB Inc.
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
#include <mongocxx/options/index.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing an index on a MongoDB server.
///
class MONGOCXX_API index_model {
   public:
    ///
    /// Initializes a new index_model over a mongocxx::collection.
    ///
    index_model(const bsoncxx::document::view_or_value& keys,
                const bsoncxx::document::view_or_value& options = {});

    index_model() = delete;

    ///
    /// Move constructs an index_model.
    ///
    index_model(index_model&&) noexcept;

    ///
    /// Move assigns an index_model.
    ///
    index_model& operator=(index_model&&) noexcept;

    ///
    /// Copy constructs an index_model.
    ///
    index_model(const index_model&);

    index_model& operator=(const index_model&) = delete;

    ///
    /// Destroys an index_model.
    ///
    ~index_model();

    ///
    /// Retrieves keys of an index_model.
    ///
    bsoncxx::document::view keys() const;

    ///
    /// Retrieves options of an index_model.
    ///
    bsoncxx::document::view options() const;

   private:
    bsoncxx::document::value _keys;
    bsoncxx::document::value _options;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>