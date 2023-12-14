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

#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/result/bulk_write.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace result {

/// Class representing the result of a MongoDB update operation.
class MONGOCXX_API update {
   public:
    // This constructor is public for testing purposes only
    explicit update(result::bulk_write result);

    ///
    /// Returns the bulk write result for this update operation.
    ///
    /// @return The raw bulk write result.
    ///
    const result::bulk_write& result() const;

    ///
    /// Gets the number of documents that were matched during this operation.
    ///
    /// @return The number of documents that were matched.
    ///
    std::int32_t matched_count() const;

    ///
    /// Gets the number of documents that were modified during this operation.
    ///
    /// @return The number of documents that were modified.
    std::int32_t modified_count() const;

    ///
    /// Gets the number of documents that were upserted during this operation.
    ///
    /// @return The number of documents that were upserted.
    std::int32_t upserted_count() const;

    ///
    /// If a document was upserted during this operation, gets the _id of the upserted document.
    ///
    /// @return The value of the _id field for upserted document.
    ///
    stdx::optional<bsoncxx::document::element> upserted_id() const;

   private:
    result::bulk_write _result;

    friend MONGOCXX_API bool MONGOCXX_CALL operator==(const update&, const update&);
    friend MONGOCXX_API bool MONGOCXX_CALL operator!=(const update&, const update&);
};

}  // namespace result
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
