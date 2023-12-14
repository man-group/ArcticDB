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

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {
namespace gridfs {

///
/// Class representing the optional arguments to a MongoDB GridFS upload operation.
///
class MONGOCXX_API upload {
   public:
    ///
    /// Sets the chunk size of the GridFS file being uploaded. Defaults to the chunk size specified
    /// in options::gridfs::bucket.
    ///
    /// @param chunk_size_bytes
    ///   The size of the chunks in bytes.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    upload& chunk_size_bytes(std::int32_t chunk_size_bytes);

    ///
    /// Gets the chunk size of the GridFS file being uploaded.
    ///
    /// @return
    ///   The chunk size of the GridFS file being uploaded in bytes.
    ///
    const stdx::optional<std::int32_t>& chunk_size_bytes() const;

    ///
    /// Sets the metadata field of the GridFS file being uploaded. A GridFS file can store arbitrary
    /// metadata in the form of a BSON document.
    ///
    /// @param metadata
    ///   The metadata document for the GridFS file.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    upload& metadata(bsoncxx::document::view_or_value metadata);

    ///
    /// Gets the metadata of the GridFS file being uploaded.
    ///
    /// @return
    ///   The metadata document of the GridFS file.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& metadata() const;

   private:
    stdx::optional<std::int32_t> _chunk_size_bytes;
    stdx::optional<bsoncxx::document::view_or_value> _metadata;
};

}  // namespace gridfs
}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
