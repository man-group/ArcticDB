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

#include <cstdint>
#include <map>

#include <bsoncxx/array/value.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/result/bulk_write.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class collection;

namespace result {

///
/// Class representing the result of a MongoDB insert many operation
/// (executed as a bulk write).
///
class MONGOCXX_API insert_many {
   public:
    using id_map = std::map<std::size_t, bsoncxx::document::element>;

    insert_many(result::bulk_write result, bsoncxx::array::value inserted_ids);

    insert_many(const insert_many&);
    insert_many(insert_many&&) = default;

    insert_many& operator=(const insert_many&);
    insert_many& operator=(insert_many&&) = default;

    ///
    /// Returns the bulk write result for this insert many operation.
    ///
    /// @return The raw bulk write result.
    ///
    const result::bulk_write& result() const;

    ///
    /// Gets the number of documents that were inserted during this operation.
    ///
    /// @return The number of documents that were inserted.
    ///
    std::int32_t inserted_count() const;

    ///
    /// Gets the _ids of the inserted documents.
    ///
    /// @note The returned id_map must not be accessed after the result::insert_many object is
    /// destroyed.
    /// @return Map of the index of the operation to the _id of the inserted document.
    ///
    id_map inserted_ids() const;

   private:
    friend collection;

    // Construct _inserted_ids from _inserted_ids_owned
    MONGOCXX_PRIVATE void _buildInsertedIds();

    result::bulk_write _result;

    // Array containing documents with the values of the _id field for the inserted documents. This
    // array is in the following format: [{"_id": ...}, {"_id": ...}, ...].
    bsoncxx::array::value _inserted_ids_owned;

    // Points into _inserted_ids_owned.
    id_map _inserted_ids;

    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const insert_many&, const insert_many&);
    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const insert_many&, const insert_many&);
};

}  // namespace result
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
