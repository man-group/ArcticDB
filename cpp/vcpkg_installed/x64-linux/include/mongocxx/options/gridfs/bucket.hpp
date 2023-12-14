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

#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {
namespace gridfs {

///
/// Class representing the optional arguments to a MongoDB GridFS bucket creation operation.
///
class MONGOCXX_API bucket {
   public:
    ///
    /// Sets the name of the bucket. Defaults to 'fs'.
    ///
    /// @param bucket_name
    ///   The name of the bucket.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    bucket& bucket_name(std::string bucket_name);

    ///
    /// Gets the name of the bucket.
    ///
    /// @return
    ///   The name of the bucket.
    ///
    const stdx::optional<std::string>& bucket_name() const;

    ///
    /// Sets the size of the chunks in the bucket. This will be used as the chunk size for files
    /// uploaded through the bucket without a custom size specified. Defaults to 255KB (255 * 1024).
    ///
    /// @param chunk_size_bytes
    ///   The size of the chunks in bytes.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called. This facilitates
    ///   method chaining.
    ///
    bucket& chunk_size_bytes(std::int32_t chunk_size_bytes);

    ///
    /// Gets the size of the chunks in the bucket.
    ///
    /// @return
    ///   The size of the chunks in the bucket in bytes.
    ///
    const stdx::optional<std::int32_t>& chunk_size_bytes() const;

    ///
    /// Sets the read concern to be used when reading from the bucket. Defaults to the read
    /// concern of the database containing the bucket's collections.
    ///
    /// @param read_concern
    ///   The read concern of the bucket.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    bucket& read_concern(class read_concern read_concern);

    ///
    /// Gets the read concern of the bucket.
    ///
    /// @return
    ///   The read concern of the bucket.
    ///
    const stdx::optional<class read_concern>& read_concern() const;

    ///
    /// Sets the read preference to be used when reading from the GridFS bucket. Defaults to the
    /// read preference of the database containing the bucket's collections.
    ///
    /// @note
    ///   Because many GridFS operations require multiple independent reads from separate
    ///   collections, use with secondaries is strongly discouraged because reads could go to
    ///   different secondaries, resulting in inconsistent data if all file and chunk documents have
    ///   not replicated to all secondaries.
    ///
    /// @param read_preference
    ///   The read preference of the GridFS bucket.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    bucket& read_preference(class read_preference read_preference);

    ///
    /// Gets the read preference of the bucket.
    ///
    /// @return
    ///   The read preference of the bucket.
    ///
    const stdx::optional<class read_preference>& read_preference() const;

    ///
    /// Sets the write concern to be used when writing to the GridFS bucket. Defaults to the write
    /// concern of the database containing the bucket's collections.
    ///
    /// @param write_concern
    ///   The write concern of the GridFS bucket.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    bucket& write_concern(class write_concern write_concern);

    ///
    /// Gets the write concern of the bucket.
    ///
    /// @return
    ///   The write concern of the bucket.
    ///
    const stdx::optional<class write_concern>& write_concern() const;

   private:
    stdx::optional<std::string> _bucket_name;
    stdx::optional<std::int32_t> _chunk_size_bytes;
    stdx::optional<class read_concern> _read_concern;
    stdx::optional<class read_preference> _read_preference;
    stdx::optional<class write_concern> _write_concern;
};

}  // namespace gridfs
}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
