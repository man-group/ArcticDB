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

#include <string>

#include <mongocxx/client_encryption-fwd.hpp>
#include <mongocxx/collection-fwd.hpp>
#include <mongocxx/database-fwd.hpp>

#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/options/client_encryption.hpp>
#include <mongocxx/options/data_key.hpp>
#include <mongocxx/options/encrypt.hpp>
#include <mongocxx/options/rewrap_many_datakey.hpp>
#include <mongocxx/result/delete.hpp>
#include <mongocxx/result/rewrap_many_datakey.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {

///
/// Class supporting operations for MongoDB Client-Side Field Level Encryption.
///
class client_encryption {
   public:
    ///
    /// Creates a client_encryption object.
    ///
    /// @param opts
    ///   An object representing encryption options.
    ///
    /// @see
    /// https://www.mongodb.com/docs/ecosystem/use-cases/client-side-field-level-encryption-guide
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
    /// @throws mongocxx::v_noabi::exception if there is an error creating the key.
    ///
    /// @see
    /// https://www.mongodb.com/docs/ecosystem/use-cases/client-side-field-level-encryption-guide/#b-create-a-data-encryption-key
    ///
    bsoncxx::v_noabi::types::bson_value::value create_data_key(std::string kms_provider,
                                                               const options::data_key& opts = {});

    /**
     * @brief Create a collection with client-side-encryption enabled, automatically filling any
     * datakeys for encrypted fields.
     *
     * @param db The database in which the collection will be created
     * @param coll_name The name of the new collection
     * @param options The options for creating the collection. @see database::create_collection
     * @param out_options Output parameter to receive the generated collection options.
     * @param kms_provider The KMS provider to use when creating data encryption keys for the
     * collection's encrypted fields
     * @param masterkey If non-null, specify the masterkey to be used when creating data keys in the
     * collection.
     * @return collection A handle to the newly created collection
     */
    collection create_encrypted_collection(
        const database& db,
        const std::string& coll_name,
        const bsoncxx::v_noabi::document::view& options,
        bsoncxx::v_noabi::document::value& out_options,
        const std::string& kms_provider,
        const stdx::optional<bsoncxx::v_noabi::document::view>& masterkey = stdx::nullopt);

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
    /// @throws mongocxx::v_noabi::exception if there is an error encrypting the value.
    ///
    /// @see
    /// https://www.mongodb.com/docs/manual/reference/method/ClientEncryption.encrypt/#ClientEncryption.encrypt
    ///
    bsoncxx::v_noabi::types::bson_value::value encrypt(
        bsoncxx::v_noabi::types::bson_value::view value, const options::encrypt& opts);

    ///
    /// Encrypts a Match Expression or Aggregate Expression to query a range index.
    ///
    /// @note Only supported when queryType is "rangePreview" and algorithm is "RangePreview".
    ///
    /// @param expr A BSON document corresponding to either a Match Expression or an Aggregate
    /// Expression.
    /// @param opts Options must be given in order to specify queryType and algorithm.
    ///
    /// @returns The encrypted expression.
    ///
    /// @warning The Range algorithm is experimental only. It is not intended for public use. It is
    /// subject to breaking changes.
    bsoncxx::v_noabi::document::value encrypt_expression(
        bsoncxx::v_noabi::document::view_or_value expr, const options::encrypt& opts);

    ///
    /// Decrypts an encrypted value (BSON binary of subtype 6).
    ///
    /// @param value
    ///   The encrypted value.
    ///
    /// @return The original BSON value.
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error decrypting the value.
    ///
    /// @see
    /// https://www.mongodb.com/docs/manual/reference/method/ClientEncryption.decrypt/#ClientEncryption.decrypt
    ///
    bsoncxx::v_noabi::types::bson_value::value decrypt(
        bsoncxx::v_noabi::types::bson_value::view value);

    ///
    /// Decrypts multiple data keys and (re-)encrypts them with a new masterKey,
    /// or with their current masterKey if a new one is not given. The updated
    /// fields of each rewrapped data key is updated in the key vault collection
    /// as part of a single bulk write operation. If no data key matches the
    /// given filter, no bulk write operation is executed.
    ///
    /// @param filter
    ///   Document to filter which keys get re-wrapped.
    ///
    /// @param opts
    ///   Options to specify which provider to encrypt the data keys and an optional
    ///   master key document.
    ///
    /// @return a RewrapManyDataKeyResult.
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error rewrapping the key.
    ///
    /// @see
    /// https://www.mongodb.com/docs/manual/reference/method/KeyVault.rewrapManyDataKey/
    ///
    result::rewrap_many_datakey rewrap_many_datakey(
        bsoncxx::v_noabi::document::view_or_value filter, const options::rewrap_many_datakey& opts);

    ///
    /// Removes the key document with the given UUID (BSON binary subtype 0x04)
    /// from the key vault collection.
    ///
    /// @param id Binary id of which key to delete
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error deleting the key.
    ///
    /// @return the result of the internal deleteOne() operation on the key vault collection.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.deleteKey/
    ///
    result::delete_result delete_key(bsoncxx::v_noabi::types::bson_value::view_or_value id);

    ///
    /// Finds a single key document with the given UUID (BSON binary subtype 0x04).
    ///
    /// @param id Binary id of which key to delete
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error getting the key.
    ///
    /// @return The result of the internal find() operation on the key vault collection.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.getKey/
    ///
    stdx::optional<bsoncxx::v_noabi::document::value> get_key(
        bsoncxx::v_noabi::types::bson_value::view_or_value id);

    ///
    /// Finds all documents in the key vault collection.
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error getting the keys.
    ///
    /// @return the result of the internal find() operation on the key vault collection.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.getKeys/
    ///
    mongocxx::v_noabi::cursor get_keys();

    ///
    /// Adds a keyAltName to the keyAltNames array of the key document in the
    /// key vault collection with the given UUID (BSON binary subtype 0x04).
    ///
    /// @param id Binary id of the key to add the key alternate name to
    ///
    /// @param key_alt_name String alternative name for the key
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error adding the key alt name.
    ///
    /// @return the previous version of the key document.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.addKeyAlternateName/
    ///
    stdx::optional<bsoncxx::v_noabi::document::value> add_key_alt_name(
        bsoncxx::v_noabi::types::bson_value::view_or_value id,
        bsoncxx::v_noabi::string::view_or_value key_alt_name);

    ///
    /// Removes a keyAltName from the keyAltNames array of the key document in
    /// the key vault collection with the given UUID (BSON binary subtype 0x04).
    ///
    /// @param id Binary id of the key to remove the key alternate name from
    ///
    /// @param key_alt_name String alternative name for the key
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error removing the key alt name.
    ///
    /// @return The previous version of the key document.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.removeKeyAlternateName/
    ///
    stdx::optional<bsoncxx::v_noabi::document::value> remove_key_alt_name(
        bsoncxx::v_noabi::types::bson_value::view_or_value id,
        bsoncxx::v_noabi::string::view_or_value key_alt_name);

    ///
    /// Get the key document from the key vault collection with the provided name.
    ///
    /// @param key_alt_name String alternative name for the key
    ///
    /// @throws mongocxx::v_noabi::exception if there is an error getting the key by alt name.
    ///
    /// @return A key document in the key vault collection with the given keyAltName.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/KeyVault.getKeyByAltName/
    ///
    stdx::optional<bsoncxx::v_noabi::document::value> get_key_by_alt_name(
        bsoncxx::v_noabi::string::view_or_value key_alt_name);

   private:
    class MONGOCXX_PRIVATE impl;

    std::unique_ptr<impl> _impl;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
