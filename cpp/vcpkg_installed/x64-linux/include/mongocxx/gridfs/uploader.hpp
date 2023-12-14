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

#include <cstddef>
#include <cstdint>
#include <memory>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <bsoncxx/view_or_value.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/result/gridfs/upload.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace gridfs {

///
/// Class used to upload a GridFS file.
///
class MONGOCXX_API uploader {
   public:
    ///
    /// Default constructs an uploader object. The uploader is equivalent to the state of a moved
    /// from uploader. The only valid actions to take with a default constructed uploader are to
    /// assign to it, or destroy it.
    ///
    uploader() noexcept;

    ///
    /// Move constructs an uploader.
    ///
    uploader(uploader&&) noexcept;

    ///
    /// Move assigns an uploader.
    ///
    uploader& operator=(uploader&&) noexcept;

    uploader(const uploader&) = delete;

    uploader& operator=(const uploader&) = delete;

    ///
    /// Destroys an uploader.
    ///
    ~uploader();

    ///
    /// Returns true if the uploader is valid, meaning it was not default constructed or moved from.
    ///
    explicit operator bool() const noexcept;

    ///
    /// Writes a specified number of bytes to a GridFS file.
    ///
    /// @param bytes
    ///   A pointer to the bytes to write.
    ///
    /// @param length
    ///   The number of bytes to write.
    ///
    /// @throws mongocxx::logic_error if the upload stream was already closed.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data to the database.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    void write(const std::uint8_t* bytes, std::size_t length);

    ///
    /// Closes the uploader stream.
    ///
    /// @throws mongocxx::logic_error if the upload stream was already closed.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when writing chunk data or file metadata to the database.
    ///
    /// @throws mongocxx::gridfs_exception
    ///   if the uploader requires more than 2^31-1 chunks to store the file at the requested chunk
    ///   size.
    ///
    result::gridfs::upload close();

    ///
    /// Aborts uploading the file.
    ///
    /// @throws mongocxx::logic_error if the upload stream was already closed.
    ///
    /// @throws mongocxx::bulk_write_exception
    ///   if an error occurs when removing chunk data from the database.
    ///
    void abort();

    ///
    /// Gets the chunk size of the file being uploaded.
    ///
    /// @return
    ///   The chunk size in bytes.
    ///
    std::int32_t chunk_size() const;

   private:
    friend class bucket;

    //
    // Constructs a new uploader stream.
    //
    // @param session
    //   The client session to use for upload operations.
    //
    // @param id
    //   The id of the GridFS file being uploaded.
    //
    // @param files
    //   The files collection of the bucket receiving the file.
    //
    // @param chunks
    //   The chunks collection of the bucket receiving the file.
    //
    // @param chunk_size
    //   The size in bytes of the chunks being uploaded.
    //
    // @param metadata
    //   Optional metadata field of the files collection document.
    //
    MONGOCXX_PRIVATE uploader(const client_session* session,
                              bsoncxx::types::bson_value::view id,
                              stdx::string_view filename,
                              collection files,
                              collection chunks,
                              std::int32_t chunk_size,
                              stdx::optional<bsoncxx::document::view_or_value> metadata = {});

    MONGOCXX_PRIVATE void finish_chunk();
    MONGOCXX_PRIVATE void flush_chunks();

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE impl& _get_impl();
    MONGOCXX_PRIVATE const impl& _get_impl() const;

    std::unique_ptr<impl> _impl;
};

}  // namespace gridfs
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx
