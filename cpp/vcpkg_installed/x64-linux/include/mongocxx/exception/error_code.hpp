// Copyright 2015-present MongoDB Inc.
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

#include <system_error>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Enum representing the various error types that can occur during driver usage.
///
enum class error_code : std::int32_t {
    /// More than one mongocxx::instance has been created.
    k_cannot_recreate_instance = 1,

    /// A default-constructed or moved-from mongocxx::client object has been used.
    k_invalid_client_object,

    /// A default-constructed or moved-from mongocxx::collection object has been used.
    k_invalid_collection_object,

    /// A default-constructed or moved-from mongocxx::database object has been used.
    k_invalid_database_object,

    /// An invalid or out-of-bounds parameter was provided.
    k_invalid_parameter,

    /// An SSL operation was used without SSL support being built.
    k_ssl_not_supported,

    /// An unknown read concern level was set.
    k_unknown_read_concern,

    /// An unknown write concern level was set.
    k_unknown_write_concern,

    /// The server returned a malformed response.
    k_server_response_malformed,

    /// An invalid MongoDB URI was provided.
    k_invalid_uri,

    /// A default-constructed or moved-from mongocxx::gridfs::bucket object has been used.
    k_invalid_gridfs_bucket_object,

    /// A default-constructed or moved-from mongocxx::gridfs::uploader object has been used.
    k_invalid_gridfs_uploader_object,

    /// A default-constructed or moved-from mongocxx::gridfs::downloader object has been used.
    k_invalid_gridfs_downloader_object,

    /// A mongocxx::gridfs::uploader object was not open for writing, or a
    /// mongocxx::gridfs::downloader object was not open for reading.
    k_gridfs_stream_not_open,

    /// A mongocxx::gridfs::uploader object has exceeded the maximum number of allowable GridFS
    /// chunks when attempting to upload the requested file.
    k_gridfs_upload_requires_too_many_chunks,

    /// The requested GridFS file was not found.
    k_gridfs_file_not_found,

    /// A GridFS file being operated on was discovered to be corrupted.
    k_gridfs_file_corrupted,

    /// The mongocxx::instance has been destroyed.
    k_instance_destroyed,

    /// mongocxx::client.create_session failed to create a mongocxx::client_session.
    k_cannot_create_session,

    /// A failure attempting to pass a mongocxx::client_session to a method.
    k_invalid_session,

    /// A moved-from mongocxx::options::transaction object has been used.
    k_invalid_transaction_options_object,

    // A resource (server API handle, etc.) could not be created:
    k_create_resource_fail,

    // Add new constant string message to error_code.cpp as well!
};

///
/// Get the error_category for mongocxx library exceptions.
///
/// @return The mongocxx error_category
///
MONGOCXX_API const std::error_category& MONGOCXX_CALL error_category();

///
/// Translate a mongocxx::error_code into a std::error_code.
///
/// @param error A mongocxx::error_code
///
/// @return A std::error_code
///
MONGOCXX_INLINE std::error_code make_error_code(error_code error) {
    return {static_cast<int>(error), error_category()};
}

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>

namespace std {
// Specialize is_error_code_enum so we get simpler std::error_code construction
template <>
struct is_error_code_enum<mongocxx::error_code> : public true_type {};
}  // namespace std
