// Copyright 2014-present MongoDB Inc.
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

#include <mongocxx/client_session.hpp>
#include <mongocxx/model/write.hpp>
#include <mongocxx/options/bulk_write.hpp>
#include <mongocxx/result/bulk_write.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class collection;

///
/// Class representing a batch of write operations that can be sent to the server as a group.
///
/// If you have a lot of write operations to execute, it can be more efficient to send them as
/// part of a bulk_write in order to avoid unnecessary network-level round trips between the driver
/// and the server.
///
/// Bulk writes affect a single collection only and are executed via the bulk_write::execute()
/// method. Options that you would typically specify for individual write operations (such as write
/// concern) are instead specified for the aggregate operation.
///
/// @see https://docs.mongodb.com/manual/core/crud/
/// @see https://docs.mongodb.com/manual/core/bulk-write-operations/
///
class MONGOCXX_API bulk_write {
   public:
    ///
    /// Move constructs a bulk write operation.
    ///
    bulk_write(bulk_write&&) noexcept;

    ///
    /// Move assigns a bulk write operation.
    ///
    bulk_write& operator=(bulk_write&&) noexcept;

    ///
    /// Destroys a bulk write operation.
    ///
    ~bulk_write();

    ///
    /// Appends a single write to the bulk write operation. The write operation's contents are
    /// copied into the bulk operation completely, so there is no dependency between the life of an
    /// appended write operation and the bulk operation itself.
    ///
    /// @param operation
    ///   The write operation to append (an instance of model::write)
    ///
    ///   A model::write can be implicitly constructed from any of the following MongoDB models:
    ///
    ///     - model::insert_one
    ///     - model::delete_one
    ///     - model::replace_one
    ///     - model::update_many
    ///     - model::update_one
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    /// @throws mongocxx::logic_error if the given operation is invalid.
    ///
    bulk_write& append(const model::write& operation);

    ///
    /// Executes a bulk write.
    ///
    /// @throws mongocxx::bulk_write_exception when there are errors processing the writes.
    ///
    /// @return The optional result of the bulk operation execution, a result::bulk_write.
    ///
    /// @see https://docs.mongodb.com/manual/core/bulk-write-operations/
    ///
    stdx::optional<result::bulk_write> execute() const;

   private:
    friend class collection;

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE bulk_write(const collection& coll,
                                const options::bulk_write& options,
                                const client_session* session = nullptr);

    bool _created_from_collection;
    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
