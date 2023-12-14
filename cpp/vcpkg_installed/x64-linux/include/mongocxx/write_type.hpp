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

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Enum representing the the types of write operations that can be performed.
///
enum class write_type {
    /// Inserting a single document into a collection.
    k_insert_one,

    /// Deleting a single document from a collection.
    k_delete_one,

    /// Delete one or more documents from a collection.
    k_delete_many,

    /// Update a single document in a collection.
    k_update_one,

    /// Update one or more documents in a collection.
    k_update_many,

    /// Replace a single document in a collection with a new one.
    k_replace_one,
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
