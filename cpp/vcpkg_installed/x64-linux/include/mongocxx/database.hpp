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

#include <memory>
#include <string>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/options/create_collection.hpp>
#include <mongocxx/options/gridfs/bucket.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;

///
/// Class representing a MongoDB database.
///
/// Acts as a gateway for accessing collections that are contained within a database. It inherits
/// all of its default settings from the client that creates it.
///
class MONGOCXX_API database {
   public:
    ///
    /// Default constructs a new database. The database is not valid for use and is equivalent
    /// to the state of a moved-from database. The only valid actions to take with a default
    /// constructed database are to assign to it, or destroy it.
    ///
    database() noexcept;

    ///
    /// Move constructs a database.
    ///
    database(database&&) noexcept;

    ///
    /// Move assigns a database.
    ///
    database& operator=(database&&) noexcept;

    ///
    /// Copy constructs a database.
    ///
    database(const database&);

    ///
    /// Copy assigns a database.
    ///
    database& operator=(const database&);

    ///
    /// Destroys a database.
    ///
    ~database();

    ///
    /// Returns true if the client is valid, meaning it was not default constructed
    /// or moved from.
    ///
    explicit operator bool() const noexcept;

    ///
    /// @{
    ///
    /// Runs an aggregation framework pipeline against this database for
    /// pipeline stages that do not require an underlying collection,
    /// such as $currentOp and $listLocalSessions.
    ///
    /// @param pipeline
    ///   The pipeline of aggregation operations to perform.
    /// @param options
    ///   Optional arguments, see mongocxx::options::aggregate.
    ///
    /// @return A mongocxx::cursor with the results.  If the query fails,
    /// the cursor throws mongocxx::query_exception when the returned cursor
    /// is iterated.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/#dbcmd.aggregate
    ///
    /// @note
    ///   In order to pass a read concern to this, you must use the
    ///   database level set read concern - database::read_concern(rc).
    ///   (Write concern supported only for MongoDB 3.4+).
    ///
    cursor aggregate(const pipeline& pipeline,
                     const options::aggregate& options = options::aggregate());

    ///
    /// Runs an aggregation framework pipeline against this database for
    /// pipeline stages that do not require an underlying collection,
    /// such as $currentOp and $listLocalSessions.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the aggregation.
    /// @param pipeline
    ///   The pipeline of aggregation operations to perform.
    /// @param options
    ///   Optional arguments, see mongocxx::options::aggregate.
    ///
    /// @return A mongocxx::cursor with the results.  If the query fails,
    /// the cursor throws mongocxx::query_exception when the returned cursor
    /// is iterated.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate/#dbcmd.aggregate
    ///
    /// @note
    ///   In order to pass a read concern to this, you must use the
    ///   database level set read concern - database::read_concern(rc).
    ///   (Write concern supported only for MongoDB 3.4+).
    ///
    cursor aggregate(const client_session& session,
                     const pipeline& pipeline,
                     const options::aggregate& options = options::aggregate());
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Runs a command against this database.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/db.runCommand/
    ///
    /// @param command document representing the command to be run.
    /// @return the result of executing the command.
    ///
    /// @throws mongocxx::operation_exception if the operation fails.
    ///
    bsoncxx::document::value run_command(bsoncxx::document::view_or_value command);

    ///
    /// Runs a command against this database.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/db.runCommand/
    ///
    /// @param session The mongocxx::client_session with which to run the command.
    /// @param command document representing the command to be run.
    /// @return the result of executing the command.
    ///
    /// @throws mongocxx::operation_exception if the operation fails.
    ///
    bsoncxx::document::value run_command(const client_session& session,
                                         bsoncxx::document::view_or_value command);

    ///
    /// Executes a command on a specific server using this database.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/db.runCommand/
    ///
    /// @param command document representing the command to be run.
    /// @param server_id specifying which server to use.
    /// @return the result of executing the command.
    ///
    /// @throws mongocxx::operation_exception if the operation fails.
    ///
    bsoncxx::document::value run_command(bsoncxx::document::view_or_value command,
                                         uint32_t server_id);
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Explicitly creates a collection in this database with the specified options.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @note This function can also be used to create a Time Series Collection. See:
    /// https://docs.mongodb.com/manual/core/timeseries-collections/
    ///
    /// @param name
    ///   the new collection's name.
    /// @param collection_options
    ///   the options for the new collection.
    /// @param write_concern
    ///   the write concern to use for this operation. Will default to database
    ///   set write concern if none passed here.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    class collection create_collection(stdx::string_view name,
                                       bsoncxx::document::view_or_value collection_options = {},
                                       const stdx::optional<write_concern>& write_concern = {});

    ///
    /// Explicitly creates a collection in this database with the specified options.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @note This function can also be used to create a Time Series Collection. See:
    /// https://docs.mongodb.com/manual/core/timeseries-collections/
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the create operation.
    /// @param name
    ///   the new collection's name.
    /// @param collection_options
    ///   the options for the new collection.
    /// @param write_concern
    ///   the write concern to use for this operation. Will default to database
    ///   set write concern if none passed here.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    class collection create_collection(const client_session& session,
                                       stdx::string_view name,
                                       bsoncxx::document::view_or_value collection_options = {},
                                       const stdx::optional<write_concern>& write_concern = {});

    ///
    /// Explicitly creates a collection in this database with the specified options.
    ///
    /// @deprecated
    ///   This overload is deprecated. Call database::create_collection with a
    ///   bsoncxx::document::view_or_value collection_options instead.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @param name
    ///   the new collection's name.
    /// @param collection_options
    ///   the options for the new collection.
    /// @param write_concern
    ///   the write concern to use for this operation. Will default to database
    ///   set write concern if none passed here.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    MONGOCXX_DEPRECATED class collection create_collection(
        bsoncxx::string::view_or_value name,
        const options::create_collection_deprecated& collection_options,
        const stdx::optional<write_concern>& write_concern = {}) {
        return create_collection_deprecated(name, collection_options, write_concern);
    }

    class collection create_collection_deprecated(
        bsoncxx::string::view_or_value name,
        const options::create_collection_deprecated& collection_options,
        const stdx::optional<write_concern>& write_concern = {});

    ///
    /// Explicitly creates a collection in this database with the specified options.
    ///
    /// @deprecated
    ///   This overload is deprecated. Call database::create_collection with a
    ///   bsoncxx::document::view_or_value collection_options instead.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the create operation.
    /// @param name
    ///   the new collection's name.
    /// @param collection_options
    ///   the options for the new collection.
    /// @param write_concern
    ///   the write concern to use for this operation. Will default to database
    ///   set write concern if none passed here.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    MONGOCXX_DEPRECATED class collection create_collection(
        const client_session& session,
        bsoncxx::string::view_or_value name,
        const options::create_collection_deprecated& collection_options,
        const stdx::optional<write_concern>& write_concern = {}) {
        return create_collection_deprecated(session, name, collection_options, write_concern);
    }

    ///
    /// Explicitly creates a collection in this database with the specified options.
    ///
    /// @deprecated
    ///   This overload is deprecated. Call database::create_collection with a
    ///   bsoncxx::document::view_or_value collection_options instead.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/create/
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the create operation.
    /// @param name
    ///   the new collection's name.
    /// @param collection_options
    ///   the options for the new collection.
    /// @param write_concern
    ///   the write concern to use for this operation. Will default to database
    ///   set write concern if none passed here.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    class collection create_collection_deprecated(
        const client_session& session,
        bsoncxx::string::view_or_value name,
        const options::create_collection_deprecated& collection_options,
        const stdx::optional<write_concern>& write_concern = {});

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Drops the database and all its collections.
    ///
    /// @param write_concern (optional)
    ///   The write concern to be used for this operation. If not passed here, the write concern
    ///   set on the database will be used.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/dropDatabase/
    ///
    void drop(const bsoncxx::stdx::optional<mongocxx::write_concern>& write_concern = {});

    ///
    /// Drops the database and all its collections.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the aggregation.
    /// @param write_concern (optional)
    ///   The write concern to be used for this operation. If not passed here, the write concern
    ///   set on the database will be used.
    ///
    /// @exception
    ///   mongocxx::operation_exception if the operation fails.
    ///
    /// @see
    ///   https://docs.mongodb.com/manual/reference/command/dropDatabase/
    ///
    void drop(const client_session& session,
              const bsoncxx::stdx::optional<mongocxx::write_concern>& write_concern = {});
    ///
    /// @}
    ///

    ///
    /// Checks whether this database contains a collection having the given name.
    ///
    /// @param name the name of the collection.
    ///
    /// @return bool whether the collection exists in this database.
    ///
    /// @throws mongocxx::operation_exception if the underlying 'listCollections'
    /// command fails.
    ///
    bool has_collection(bsoncxx::string::view_or_value name) const;

    ///
    /// @{
    ///
    /// Enumerates the collections in this database.
    ///
    /// @param filter
    ///   An optional query expression to filter the returned collections.
    ///
    /// @return mongocxx::cursor containing the collection information.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/listCollections/
    ///
    cursor list_collections(bsoncxx::document::view_or_value filter = {});

    ///
    /// Enumerates the collections in this database.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the aggregation.
    /// @param filter
    ///   An optional query expression to filter the returned collections.
    ///
    /// @return mongocxx::cursor containing the collection information.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/listCollections/
    ///
    cursor list_collections(const client_session& session,
                            bsoncxx::document::view_or_value filter = {});

    ///
    /// Enumerates the collection names in this database.
    ///
    /// @param filter
    ///   An optional query expression to filter the returned collection names.
    ///
    /// @return std::vector<std::string> containing the collection names.
    ///
    /// @throws mongocxx::operation_exception if the underlying 'listCollections'
    /// command fails.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/listCollections/
    ///
    std::vector<std::string> list_collection_names(bsoncxx::document::view_or_value filter = {});

    ///
    /// Enumerates the collection names in this database.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the aggregation.
    /// @param filter
    ///   An optional query expression to filter the returned collection names.
    ///
    /// @return std::vector<std::string> containing the collection names.
    ///
    /// @throws mongocxx::operation_exception if the underlying 'listCollections'
    /// command fails.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/listCollections/
    ///
    std::vector<std::string> list_collection_names(const client_session& session,
                                                   bsoncxx::document::view_or_value filter = {});

    ///
    /// @}
    ///

    ///
    /// Get the name of this database.
    ///
    /// @return the name of this database.
    ///
    stdx::string_view name() const;

    ///
    /// Sets the read_concern for this database.
    ///
    /// @note Modifications at this level do not affect existing collection instances that have come
    /// from this database, but do affect new ones. New collections will receive a copy of the
    /// new read_concern for this database upon instantiation.
    ///
    /// @param rc
    ///   The new @c read_concern
    ///
    /// @see https://docs.mongodb.com/manual/reference/read-concern/
    ///
    void read_concern(class read_concern rc);

    ///
    /// The current read concern for this database.
    ///
    /// If the read_concern is not explicitly set on this database object, it inherits the
    /// read_concern from its parent client object.
    ///
    /// @return the current read_concern
    ///
    class read_concern read_concern() const;

    ///
    /// Sets the read_preference for this database.
    ///
    /// @note Modifications at this level do not affect existing collection instances that have come
    /// from this database, but do affect new ones. New collections will receive a copy of the
    /// new read_preference for this database upon instantiation.
    ///
    /// @see https://docs.mongodb.com/manual/core/read-preference/
    ///
    /// @param rp the new read_preference.
    ///
    void read_preference(class read_preference rp);

    ///
    /// The current read preference for this database.
    ///
    /// @see https://docs.mongodb.com/manual/core/read-preference/
    ///
    /// @return the current read_preference
    ///
    class read_preference read_preference() const;

    ///
    /// Sets the write_concern for this database.
    ///
    /// @note Modifications at this level do not affect existing collection instances that have come
    /// from this database, but do affect new ones as new collections will receive a copy of the
    /// write_concern of this database upon instantiation.
    ///
    void write_concern(class write_concern wc);

    ///
    /// The current write_concern for this database.
    ///
    /// @return the current write_concern
    ///
    class write_concern write_concern() const;

    ///
    /// Access a collection (logical grouping of documents) within this database.
    ///
    /// @param name the name of the collection to get.
    ///
    /// @return the collection.
    ///
    class collection collection(bsoncxx::string::view_or_value name) const;

    ///
    /// Allows the db["collection_name"] syntax to be used to access a collection within this
    /// database.
    ///
    /// @param name the name of the collection to get.
    ///
    /// @return the collection.
    ///
    MONGOCXX_INLINE class collection operator[](bsoncxx::string::view_or_value name) const;

    ///
    /// Access a GridFS bucket within this database.
    ///
    /// @param options
    ///   The options for the bucket.
    ///
    /// @return
    ///   The GridFS bucket.
    ///
    /// @note
    ///   See the class comment for `gridfs::bucket` for more information about GridFS.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    class gridfs::bucket gridfs_bucket(
        const options::gridfs::bucket& options = options::gridfs::bucket()) const;

    ///
    /// @{
    ///
    /// Gets a change stream on this database with an empty pipeline.
    /// Change streams are only supported with a "majority" read concern or no read concern.
    ///
    /// @param options
    ///   The options to use when creating the change stream.
    ///
    /// @return
    ///  A change stream on this database.
    ///
    /// @see https://docs.mongodb.com/manual/changeStreams/
    ///
    change_stream watch(const options::change_stream& options = {});

    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the watch operation.
    /// @param options
    ///   The options to use when creating the change stream.
    ///
    /// @return
    ///  A change stream on this database.
    ///
    /// @see https://docs.mongodb.com/manual/changeStreams/
    ///
    change_stream watch(const client_session& session, const options::change_stream& options = {});

    ///
    /// Gets a change stream on this database.
    /// Change streams are only supported with a "majority" read concern or no read concern.
    ///
    /// @param pipe
    ///   The aggregation pipeline to be used on the change notifications.
    ///   Only a subset of pipeline operations are supported for change streams. For more
    ///   information see the change streams documentation.
    /// @param options
    ///   The options to use when creating the change stream.
    ///
    /// @return
    ///  A change stream on this database.
    ///
    /// @see https://docs.mongodb.com/manual/changeStreams/
    ///
    change_stream watch(const pipeline& pipe, const options::change_stream& options = {});

    ///
    /// Gets a change stream on this database.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the watch operation.
    /// @param pipe
    ///   The aggregation pipeline to be used on the change notifications.
    /// @param options
    ///   The options to use when creating the change stream.
    ///
    /// @return
    ///  A change stream on this database.
    ///
    /// @see https://docs.mongodb.com/manual/changeStreams/
    ///
    change_stream watch(const client_session& session,
                        const pipeline& pipe,
                        const options::change_stream& options = {});

    ///
    /// @}
    ///

   private:
    friend class client;
    friend class collection;

    MONGOCXX_PRIVATE database(const class client& client, bsoncxx::string::view_or_value name);

    MONGOCXX_PRIVATE cursor _aggregate(const client_session* session,
                                       const pipeline& pipeline,
                                       const options::aggregate& options);

    MONGOCXX_PRIVATE bsoncxx::document::value _run_command(
        const client_session* session, bsoncxx::document::view_or_value command);

    MONGOCXX_PRIVATE class collection _create_collection(
        const client_session* session,
        stdx::string_view name,
        bsoncxx::document::view_or_value collection_options,
        const stdx::optional<class write_concern>& write_concern);

    MONGOCXX_PRIVATE class collection _create_collection_deprecated(
        const client_session* session,
        bsoncxx::string::view_or_value name,
        const options::create_collection_deprecated& collection_options,
        const stdx::optional<class write_concern>& write_concern);

    MONGOCXX_PRIVATE cursor _list_collections(const client_session* session,
                                              bsoncxx::document::view_or_value filter);

    MONGOCXX_PRIVATE std::vector<std::string> _list_collection_names(
        const client_session* session, bsoncxx::document::view_or_value filter);

    MONGOCXX_PRIVATE void _drop(
        const client_session* session,
        const bsoncxx::stdx::optional<mongocxx::write_concern>& write_concern);

    MONGOCXX_PRIVATE change_stream _watch(const client_session* session,
                                          const pipeline& pipe,
                                          const options::change_stream& options);

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;

    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE collection database::operator[](bsoncxx::string::view_or_value name) const {
    return collection(name);
}

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
