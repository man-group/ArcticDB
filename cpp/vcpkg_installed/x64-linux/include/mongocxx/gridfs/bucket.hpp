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

#include <istream>
#include <memory>
#include <ostream>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/gridfs/downloader.hpp>
#include <mongocxx/gridfs/uploader.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/gridfs/bucket.hpp>
#include <mongocxx/options/gridfs/upload.hpp>
#include <mongocxx/result/gridfs/upload.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class database;

namespace gridfs {

///
/// Class representing a GridFS bucket.
///
/// A GridFS bucket is used to store files that may be too large to store in a single document due
/// to the 16 MB limit. The bucket comprises of two collections, `<bucketname>.files` and
/// `<bucketname>.chunks.` A file written to a GridFS bucket will be serialized into zero or more
/// chunk documents stored in the `<bucketname>.chunks` collection, and one document will be stored
/// in the `<bucketname>.files` collection containing the information about the file. Users should
/// not modify these collections directly.
///
/// Example of how obtain the default GridFS bucket for a given database:
/// @code
///   mongocxx::client mongo_client{mongocxx::uri{}};
///   auto gridfs_bucket = mongo_client["database"].gridfs_bucket();
/// @endcode
///
/// See also the method documentation for `mongocxx::database::gridfs_bucket()`.
///
/// @see http://www.mongodb.org/display/DOCS/GridFS
///
class MONGOCXX_API bucket {
   public:
    ///
    /// Default constructs a bucket object. The bucket is equivalent to the state of a moved from
    /// bucket. The only valid actions to take with a default constructed bucket are to assign to
    /// it, or destroy it.
    ///
    bucket() noexcept;

    ///
    /// Move constructs a bucket.
    ///
    bucket(bucket&&) noexcept;

    ///
    /// Move assigns a bucket.
    ///
    bucket& operator=(bucket&&) noexcept;

    ///
    /// Copy constructs a bucket.
    ///
    bucket(const bucket&);

    ///
    /// Copy assigns a bucket.
    ///
    bucket& operator=(const bucket&);

    ///
    /// Destroys a bucket.
    ///
    ~bucket();

    ///
    /// Returns true if the bucket is valid, meaning it was not default constructed or moved from.
    ///
    explicit operator bool() const noexcept;

    ///
    /// @{
    ///
    /// Opens a gridfs::uploader to create a new GridFS file. The id of the file will be
    /// automatically generated as an ObjectId.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   A stream for writing to the GridFS file.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    uploader open_upload_stream(stdx::string_view filename,
                                const options::gridfs::upload& options = {});

    ///
    /// Opens a gridfs::uploader to create a new GridFS file. The id of the file will be
    /// automatically generated as an ObjectId.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the upload. The client session must
    ///   remain valid for the lifetime of the uploader.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   A stream for writing to the GridFS file.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    uploader open_upload_stream(const client_session& session,
                                stdx::string_view filename,
                                const options::gridfs::upload& options = {});
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Opens a gridfs::uploader to create a new GridFS file.
    ///
    /// @param id
    ///   The unique id of the file being uploaded.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   The gridfs::uploader to which the GridFS file should be written.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    uploader open_upload_stream_with_id(bsoncxx::types::bson_value::view id,
                                        stdx::string_view filename,
                                        const options::gridfs::upload& options = {});

    ///
    /// Opens a gridfs::uploader to create a new GridFS file.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the upload. The client session must
    ///   remain valid for the lifetime of the uploader.
    ///
    /// @param id
    ///   The unique id of the file being uploaded.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   The gridfs::uploader to which the GridFS file should be written.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    uploader open_upload_stream_with_id(const client_session& session,
                                        bsoncxx::types::bson_value::view id,
                                        stdx::string_view filename,
                                        const options::gridfs::upload& options = {});
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Creates a new GridFS file by uploading bytes from an input stream. The id of the file will
    /// be automatically generated as an ObjectId.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param source
    ///    The non-null stream from which the GridFS file should be read. The exception mask on
    ///    `source` will be cleared of `eofbit` and set for `failbit` and `badbit`.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   The id of the uploaded file.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data or file metadata to the database.
    ///
    /// @throws std::ios_base::failure
    ///   if reading from `source` fails. Any exception thrown during the execution of
    ///   `source::read()` will be re-thrown.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    result::gridfs::upload upload_from_stream(stdx::string_view filename,
                                              std::istream* source,
                                              const options::gridfs::upload& options = {});

    ///
    /// Creates a new GridFS file by uploading bytes from an input stream. The id of the file will
    /// be automatically generated as an ObjectId.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the upload.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param source
    ///    The non-null stream from which the GridFS file should be read. The exception mask on
    ///    `source` will be cleared of `eofbit` and set for `failbit` and `badbit`.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @return
    ///   The id of the uploaded file.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data or file metadata to the database.
    ///
    /// @throws std::ios_base::failure
    ///   if reading from `source` fails. Any exception thrown during the execution of
    ///   `source::read()` will be re-thrown.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    result::gridfs::upload upload_from_stream(const client_session& session,
                                              stdx::string_view filename,
                                              std::istream* source,
                                              const options::gridfs::upload& options = {});
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Creates a new GridFS file with a user-supplied unique id by uploading bytes from an input
    /// stream.
    ///
    /// @param id
    ///   A unique id for the file being uploaded.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param source
    ///    The non-null stream from which the GridFS file should be read. The exception mask on
    ///    `source` will be cleared of `eofbit` and set for `failbit` and `badbit`.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data or file metadata to the database.
    ///
    /// @throws std::ios_base::failure
    ///   if reading from `source` fails. Any exception thrown during the execution of
    ///   `source::read()` will be re-thrown.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    void upload_from_stream_with_id(bsoncxx::types::bson_value::view id,
                                    stdx::string_view filename,
                                    std::istream* source,
                                    const options::gridfs::upload& options = {});

    ///
    /// Creates a new GridFS file with a user-supplied unique id by uploading bytes from an input
    /// stream.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the upload.
    ///
    /// @param id
    ///   A unique id for the file being uploaded.
    ///
    /// @param filename
    ///   The name of the file to be uploaded. A bucket can contain multiple files with the same
    ///   name.
    ///
    /// @param source
    ///    The non-null stream from which the GridFS file should be read. The exception mask on
    ///    `source` will be cleared of `eofbit` and set for `failbit` and `badbit`.
    ///
    /// @param options
    ///   Optional arguments; see options::gridfs::upload.
    ///
    /// @note
    ///   If this GridFS bucket does not already exist in the database, it will be implicitly
    ///   created and initialized with GridFS indexes.
    ///
    /// @throws mongocxx::logic_error if `options` are invalid.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data or file metadata to the database.
    ///
    /// @throws std::ios_base::failure
    ///   if reading from `source` fails. Any exception thrown during the execution of
    ///   `source::read()` will be re-thrown.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    /// @throws mongocxx::operation_exception if an error occurs when building GridFS indexes.
    ///
    void upload_from_stream_with_id(const client_session& session,
                                    bsoncxx::types::bson_value::view id,
                                    stdx::string_view filename,
                                    std::istream* source,
                                    const options::gridfs::upload& options = {});
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Opens a gridfs::downloader to read a GridFS file.
    ///
    /// @param id
    ///   The id of the file to read.
    ///
    /// @return
    ///   The gridfs::downloader from which the GridFS file should be read.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the requested file does not exist, or if the requested file has been corrupted.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    downloader open_download_stream(bsoncxx::types::bson_value::view id);

    ///
    /// Opens a gridfs::downloader to read a GridFS file.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the download. The client session must
    ///   remain valid for the lifetime of the downloader.
    ///
    /// @param id
    ///   The id of the file to read.
    ///
    /// @return
    ///   The gridfs::downloader from which the GridFS file should be read.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the requested file does not exist, or if the requested file has been corrupted.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files collection for this bucket.
    ///
    downloader open_download_stream(const client_session& session,
                                    bsoncxx::types::bson_value::view id);
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Downloads the contents of a stored GridFS file from the bucket and writes it to a stream.
    ///
    /// @param id
    ///   The id of the file to read.
    ///
    /// @param destination
    ///   The non-null stream to which the GridFS file should be written.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the requested file does not exist, or if the requested file has been corrupted.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files or chunks collections for this bucket.
    ///
    /// @throws std::ios_base::failure
    ///   if writing to `destination` fails.  In addition, if `destination::exceptions()` is set for
    ///   `badbit`, any exception thrown during execution of `destination::write()` will be
    ///   re-thrown.
    ///
    void download_to_stream(bsoncxx::types::bson_value::view id, std::ostream* destination);
    void download_to_stream(bsoncxx::types::bson_value::view id,
                            std::ostream* destination,
                            std::size_t start,
                            std::size_t end);

    ///
    /// Downloads the contents of a stored GridFS file from the bucket and writes it to a stream.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the download.
    ///
    /// @param id
    ///   The id of the file to read.
    ///
    /// @param destination
    ///   The non-null stream to which the GridFS file should be written.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the requested file does not exist, or if the requested file has been corrupted.
    ///
    /// @throws mongocxx::query_exception
    ///   if an error occurs when reading from the files or chunks collections for this bucket.
    ///
    /// @throws std::ios_base::failure
    ///   if writing to `destination` fails.  In addition, if `destination::exceptions()` is set for
    ///   `badbit`, any exception thrown during execution of `destination::write()` will be
    ///   re-thrown.
    ///
    void download_to_stream(const client_session& session,
                            bsoncxx::types::bson_value::view id,
                            std::ostream* destination);
    void download_to_stream(const client_session& session,
                            bsoncxx::types::bson_value::view id,
                            std::ostream* destination,
                            std::size_t start,
                            std::size_t end);
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Deletes a GridFS file from the bucket.
    ///
    /// @param id
    ///   The id of the file to be deleted.
    ///
    /// @throws mongocxx::gridfs_exception if the requested file does not exist.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when removing file data or chunk data from the database.
    ///
    void delete_file(bsoncxx::types::bson_value::view id);

    ///
    /// Deletes a GridFS file from the bucket.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the delete.
    ///
    /// @param id
    ///   The id of the file to be deleted.
    ///
    /// @throws mongocxx::gridfs_exception if the requested file does not exist.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when removing file data or chunk data from the database.
    ///
    void delete_file(const client_session& session, bsoncxx::types::bson_value::view id);
    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Finds the documents in the files collection of the bucket which match the provided filter.
    ///
    /// @param filter
    ///   Document view representing a document that should match the query.
    ///
    /// @param options
    ///   Optional arguments; see options::find.
    ///
    /// @return
    ///   A mongocxx::cursor with the results. If the query fails, the cursor throws
    ///   mongocxx::query_exception when the returned cursor is iterated.
    ///
    /// @throws mongocxx::logic_error if the options are invalid, or if the unsupported option
    /// modifiers "$query" or "$explain" are used.
    ///
    /// @see mongocxx::collection::find.
    ///
    cursor find(bsoncxx::document::view_or_value filter, const options::find& options = {});

    ///
    /// Finds the documents in the files collection of the bucket which match the provided filter.
    ///
    /// @param session
    ///   The mongocxx::client_session with which to perform the query. The client session must
    ///   remain valid for the lifetime of the cursor.
    ///
    /// @param filter
    ///   Document view representing a document that should match the query.
    ///
    /// @param options
    ///   Optional arguments; see options::find.
    ///
    /// @return
    ///   A mongocxx::cursor with the results. If the query fails, the cursor throws
    ///   mongocxx::query_exception when the returned cursor is iterated.
    ///
    /// @throws mongocxx::logic_error if the options are invalid, or if the unsupported option
    /// modifiers "$query" or "$explain" are used.
    ///
    /// @see mongocxx::collection::find.
    ///
    cursor find(const client_session& session,
                bsoncxx::document::view_or_value filter,
                const options::find& options = {});
    ///
    /// @}
    ///

    ///
    /// Gets the name of the GridFS bucket.
    ///
    /// @return
    ///   The name of the GridFS bucket.
    ///
    stdx::string_view bucket_name() const;

   private:
    friend class mongocxx::database;

    // Constructs a new GridFS bucket.  Throws if options are invalid.
    MONGOCXX_PRIVATE bucket(const database& db, const options::gridfs::bucket& options);

    MONGOCXX_PRIVATE void create_indexes_if_nonexistent(const client_session* session);

    MONGOCXX_PRIVATE uploader _open_upload_stream_with_id(const client_session* session,
                                                          bsoncxx::types::bson_value::view id,
                                                          stdx::string_view filename,
                                                          const options::gridfs::upload& options);

    MONGOCXX_PRIVATE void _upload_from_stream_with_id(const client_session* session,
                                                      bsoncxx::types::bson_value::view id,
                                                      stdx::string_view filename,
                                                      std::istream* source,
                                                      const options::gridfs::upload& options);

    MONGOCXX_PRIVATE downloader _open_download_stream(const client_session* session,
                                                      bsoncxx::types::bson_value::view id,
                                                      stdx::optional<std::size_t> start,
                                                      stdx::optional<std::size_t> end);

    MONGOCXX_PRIVATE void _download_to_stream(const client_session* session,
                                              bsoncxx::types::bson_value::view id,
                                              std::ostream* destination,
                                              stdx::optional<std::size_t> start,
                                              stdx::optional<std::size_t> end);

    MONGOCXX_PRIVATE void _delete_file(const client_session* session,
                                       bsoncxx::types::bson_value::view id);

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;

    std::unique_ptr<impl> _impl;
};

}  // namespace gridfs
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
