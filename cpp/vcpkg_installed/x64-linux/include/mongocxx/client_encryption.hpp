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

#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/options/client_encryption.hpp>
#include <mongocxx/options/data_key.hpp>
#include <mongocxx/options/encrypt.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class supporting operations for MongoDB Client-Side Field Level Encryption.
///
class MONGOCXX_API client_encryption {
   public:
    ///
    /// Creates a client_encryption object.
    ///
    /// @param opts
    ///   An object representing encryption options.
    ///
    /// @see https://docs.mongodb.com/ecosystem/use-cases/client-side-field-level-encryption-guide
    ///
    client_encryption(options::client_encryption opts);

    ///
    /// Destroys a client_encryption.
    ///
    ~client_encryption() noexcept;

    ///
    /// Move-constructs a client_encryption object.
    ///
    client_encryption(client_encryption&&);

    ///
    /// Move-assigns a client_encryption object.
    ///
    client_encryption& operator=(client_encryption&&);

    client_encryption(const client_encryption&) = delete;
    client_encryption& operator=(const client_encryption&) = delete;

    ///
    /// Creates a new key document and inserts into the key vault collection.
    ///
    /// @param kms_provider
    ///   A string identifying the KMS service to use to encrypt the datakey.
    ///   Must be one of "aws", "azure", "gcp", "kmip", or "local".
    /// @param opts
    ///   Optional arguments, see options::data_key.
    ///
    /// @return The id of the created document as a bson_value::value containing
    ///   a UUID (BSON binary subtype 4).
    ///
    /// @throws mongocxx::exception if there is an error creating the key.
    ///
    /// @see
    /// https://docs.mongodb.com/ecosystem/use-cases/client-side-field-level-encryption-guide/#b-create-a-data-encryption-key
    ///
    bsoncxx::types::bson_value::value create_data_key(std::string kms_provider,
                                                      const options::data_key& opts = {});

    ///
    /// Encrypts a BSON value with a given key and algorithm.
    ///
    /// @param value
    ///   The BSON value to encrypt.
    /// @param opts
    ///   Options must be given in order to specify an encryption algorithm
    ///   and a key_id or key_alt_name. See options::encrypt.
    ///
    /// @return The encrypted value (BSON binary subtype 6).
    ///
    /// @throws mongocxx::exception if there is an error encrypting the value.
    ///
    /// @see
    /// https://docs.mongodb.com/manual/reference/method/ClientEncryption.encrypt/#ClientEncryption.encrypt
    ///
    bsoncxx::types::bson_value::value encrypt(bsoncxx::types::bson_value::view value,
                                              const options::encrypt& opts);

    ///
    /// Decrypts an encrypted value (BSON binary of subtype 6).
    ///
    /// @param value
    ///   The encrypted value.
    ///
    /// @return The original BSON value.
    ///
    /// @throws mongocxx::exception if there is an error decrypting the value.
    ///
    /// @see
    /// https://docs.mongodb.com/manual/reference/method/ClientEncryption.decrypt/#ClientEncryption.decrypt
    ///
    bsoncxx::types::bson_value::value decrypt(bsoncxx::types::bson_value::view value);

   private:
    class MONGOCXX_PRIVATE impl;

    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
