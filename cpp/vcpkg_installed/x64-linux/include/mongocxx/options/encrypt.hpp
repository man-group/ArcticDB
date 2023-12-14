// Copyright 2020 MongoDB Inc.
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

#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view_or_value.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client_encryption;

namespace options {

///
/// Class representing options for explicit client-side encryption.
///
class MONGOCXX_API encrypt {
   public:
    ///
    /// Sets the key to use for this encryption operation. A key id can be used instead
    /// of a key alt name.
    ///
    /// If a non-owning bson_value::view is passed in as the key_id, the object that owns
    /// key_id's memory must outlive this object.
    ///
    /// @param key_id
    ///   The id of the key to use for encryption, as a bson_value containing a
    ///   UUID (BSON binary subtype 4).
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/security-client-side-encryption/
    ///
    encrypt& key_id(bsoncxx::types::bson_value::view_or_value key_id);

    ///
    /// Sets a name by which to lookup a key from the key vault collection to use
    /// for this encryption operation. A key alt name can be used instead of a key id.
    ///
    /// @param name
    ///   The name of the key to use for encryption.
    ///
    /// @return
    ///   A reference to this obejct to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/method/getClientEncryption/
    ///
    encrypt& key_alt_name(std::string name);

    ///
    /// Gets the current key alt name.
    ///
    /// @return
    ///   An optional key name.
    ///
    const stdx::optional<std::string>& key_alt_name() const;

    ///
    /// Determines which AEAD_AES_256_CBC algorithm to use with HMAC_SHA_512 when
    /// encrypting data.
    ///
    enum class encryption_algorithm : std::uint8_t {
        ///
        /// Use deterministic encryption.
        ///
        k_deterministic,

        ///
        /// Use randomized encryption.
        ///
        k_random
    };

    ///
    /// Sets the algorithm to use for encryption.
    ///
    /// @param algorithm
    ///   An algorithm, either deterministic or random, to use for encryption.
    ///
    /// @see
    /// https://docs.mongodb.com/manual/core/security-client-side-encryption/#encryption-algorithms
    ///
    encrypt& algorithm(encryption_algorithm algorithm);

    ///
    /// Gets the current algorithm.
    ///
    /// @return
    ///   An optional algorithm.
    ///
    const stdx::optional<encryption_algorithm>& algorithm() const;

    ///
    /// Gets the key_id.
    ///
    /// @return
    ///   An optional owning bson_value containing the key_id.
    ///
    const stdx::optional<bsoncxx::types::bson_value::view_or_value>& key_id() const;

   private:
    friend class mongocxx::client_encryption;
    MONGOCXX_PRIVATE void* convert() const;

    stdx::optional<bsoncxx::types::bson_value::view_or_value> _key_id;
    stdx::optional<std::string> _key_alt_name;
    stdx::optional<encryption_algorithm> _algorithm;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
