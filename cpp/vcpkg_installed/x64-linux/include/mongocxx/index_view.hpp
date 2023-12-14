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

#include <string>
#include <vector>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/index_model.hpp>
#include <mongocxx/options/index_view.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing a MongoDB index view.
///
class MONGOCXX_API index_view {
   public:
    index_view(index_view&&) noexcept;
    index_view& operator=(index_view&&) noexcept;

    ~index_view();

    ///
    /// @{
    ///
    /// Returns a cursor over all the indexes.
    ///
    cursor list();

    ///
    /// Returns a cursor over all the indexes.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the list operation.
    ///
    cursor list(const client_session& session);

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Creates an index. A convenience method that calls create_many.
    ///
    /// @param keys
    ///    A document containing the index keys and their corresponding index types.
    /// @param index_options
    ///    A document containing set of options that controls the creation of the index. See
    ///    https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    An optional containing the name of the created index. If and index with the same keys
    ///    already exists, an empty optional is returned.
    ///
    /// @exception
    ///    Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///    option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/
    ///
    stdx::optional<std::string> create_one(
        const bsoncxx::document::view_or_value& keys,
        const bsoncxx::document::view_or_value& index_options = {},
        const options::index_view& options = options::index_view{});

    ///
    /// Creates an index. A convenience method that calls create_many.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the operation.
    /// @param keys
    ///    A document containing the index keys and their corresponding index types.
    /// @param index_options
    ///    A document containing set of options that controls the creation of the index. See
    ///    https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    An optional containing the name of the created index. If and index with the same keys
    ///    already exists, an empty optional is returned.
    ///
    /// @exception
    ///    Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///    option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/
    ///
    stdx::optional<std::string> create_one(
        const client_session& session,
        const bsoncxx::document::view_or_value& keys,
        const bsoncxx::document::view_or_value& index_options = {},
        const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Creates an index. A convenience method that calls create_many.
    ///
    /// @param index
    ///    Index_model describing the index being created.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    An optional containing the name of the created index. If and index with the same keys
    ///    already exists, an empty optional is returned.
    ///
    /// @exception
    ///    Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///    option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    stdx::optional<std::string> create_one(
        const index_model& index, const options::index_view& options = options::index_view{});

    ///
    /// Creates an index. A convenience method that calls create_many.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the operation.
    /// @param index
    ///    Index_model describing the index being created.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    An optional containing the name of the created index. If and index with the same keys
    ///    already exists, an empty optional is returned.
    ///
    /// @exception
    ///    Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///    option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    stdx::optional<std::string> create_one(
        const client_session& session,
        const index_model& index,
        const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Adds a container of indexes to the collection.
    ///
    /// @param indexes
    ///   std::vector containing index models describing the indexes being created.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    The result document sent back by the server as if the createIndexes command was run from
    ///    the shell.
    ///
    /// @exception
    ///     Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///     option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    bsoncxx::document::value create_many(
        const std::vector<index_model>& indexes,
        const options::index_view& options = options::index_view{});

    ///
    /// Adds a container of indexes to the collection.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the operation.
    /// @param indexes
    ///   std::vector containing index models describing the indexes being created.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @return
    ///    The result document sent back by the server as if the createIndexes command was run from
    ///    the shell.
    ///
    /// @exception
    ///     Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///     option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    bsoncxx::document::value create_many(
        const client_session& session,
        const std::vector<index_model>& indexes,
        const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Drops a single index by name.
    ///
    /// @param name
    ///    The name of the index being dropped.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(stdx::string_view name,
                  const options::index_view& options = options::index_view{});

    ///
    /// Drops a single index by name.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the drop.
    /// @param name
    ///    The name of the index being dropped.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(const client_session& session,
                  stdx::string_view name,
                  const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Attempts to drop a single index from the collection given the keys and options.
    ///
    /// @param keys
    ///    A document containing the index keys and their corresponding index types. If no name
    ///    option is present in the options, a name based on the keys will be used.
    /// @param index_options (optional)
    ///    A document containing set of options used to create the index. Only the name field will
    ///    be used from here, and if it is not included, a name based on they keys will be used.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws bsoncxx::exception if "name" key is present in options but is not a string.
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(const bsoncxx::document::view_or_value& keys,
                  const bsoncxx::document::view_or_value& index_options = {},
                  const options::index_view& options = options::index_view{});

    ///
    /// Attempts to drop a single index from the collection given the keys and options.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the drop.
    /// @param keys
    ///    A document containing the index keys and their corresponding index types. If no name
    ///    option is present in the options, a name based on the keys will be used.
    /// @param index_options (optional)
    ///    A document containing set of options used to create the index. Only the name field will
    ///    be used from here, and if it is not included, a name based on they keys will be used.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws bsoncxx::exception if "name" key is present in options but is not a string.
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(const client_session& session,
                  const bsoncxx::document::view_or_value& keys,
                  const bsoncxx::document::view_or_value& index_options = {},
                  const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Attempts to drop a single index from the collection given an index model.
    ///
    /// @param index
    ///    An index model describing the index being dropped.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws bsoncxx::exception if "name" key is present in options but is not a string.
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(const index_model& index,
                  const options::index_view& options = options::index_view{});

    ///
    /// Attempts to drop a single index from the collection given an index model.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the drop.
    /// @param index
    ///    An index model describing the index being dropped.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws bsoncxx::exception if "name" key is present in options but is not a string.
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    /// @exception
    ///   Throws logic_error if "*" is passed in for the index name
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_one(const client_session& session,
                  const index_model& index,
                  const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Drops all indexes in the collection.
    ///
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_all(const options::index_view& options = options::index_view{});

    ///
    /// Drops all indexes in the collection.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the drop.
    /// @param options
    ///    Optional arguments for the overall operation, see mongocxx::options::index_view.
    ///
    /// @exception
    ///   Throws operation_exception for any errors encountered by the server or if max_time_ms
    ///   option is present and the operation exceeds the time limit.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS/
    ///
    void drop_all(const client_session& session,
                  const options::index_view& options = options::index_view{});

    ///
    /// @}
    ///

   private:
    friend class collection;
    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE index_view(void* coll, void* client);

    MONGOCXX_PRIVATE impl& _get_impl();

   private:
    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
