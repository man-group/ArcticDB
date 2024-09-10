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
#include <vector>

#include <mongocxx/client_encryption-fwd.hpp>
#include <mongocxx/options/data_key-fwd.hpp>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing options for data key generation for encryption.
///
class data_key {
   public:
    ///
    /// Sets a KMS-specific key used to encrypt the new data key.
    ///
    /// If the KMS provider is "aws" the masterKey is required and has the following fields:
    ///
    /// {
    ///    region: String,
    ///    key: String, // The Amazon Resource Name (ARN) to the AWS customer master key (CMK).
    ///    endpoint: Optional<String> // An alternate host identifier to send KMS requests to. May
    ///    include port number. Defaults to "kms.<region>.amazonaws.com"
    /// }
    ///
    /// If the KMS provider is "azure" the masterKey is required and has the following fields:
    ///
    /// {
    ///    keyVaultEndpoint: String, // Host with optional port. Example: "example.vault.azure.net".
    ///    keyName: String,
    ///    keyVersion: Optional<String> // A specific version of the named key, defaults to using
    ///    the key's primary version.
    /// }
    ///
    /// If the KMS provider is "gcp" the masterKey is required and has the following fields:
    ///
    /// {
    ///    projectId: String,
    ///    location: String,
    ///    keyRing: String,
    ///    keyName: String,
    ///    keyVersion: Optional<String>, // A specific version of the named key, defaults to using
    ///    the key's primary version.
    ///    endpoint: Optional<String> // Host with optional port. Defaults to
    ///    "cloudkms.googleapis.com".
    /// }
    ///
    /// If the KMS provider is "kmip" the masterKey is required and has the following fields:
    ///
    /// {
    //     keyId: Optional<String>, // keyId is the KMIP Unique Identifier to a 96 byte KMIP Secret
    //                              // Data managed object.If keyId is omitted, the driver creates a
    //                              // random 96 byte KMIP Secret Data managed object.
    //     endpoint: Optional<String> // Host with optional port.
    /// }
    ///
    /// If the KMS provider is "local" the masterKey is not applicable.
    ///
    /// @param master_key
    ///   The document representing the master key.
    ///
    /// @return
    ///   A reference to this object.
    ///
    /// @see
    /// https://www.mongodb.com/docs/manual/core/security-client-side-encryption-key-management/
    ///
    data_key& master_key(bsoncxx::v_noabi::document::view_or_value master_key);

    ///
    /// Gets the master key.
    ///
    /// @return
    ///   An optional document containing the master key.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& master_key() const;

    ///
    /// Sets an optional list of string alternate names used to reference the key.
    /// If a key is created with alternate names, then encryption may refer to the
    /// key by the unique alternate name instead of by _id.
    ///
    /// @param key_alt_names
    ///   The alternate names for the key.
    ///
    /// @return
    ///   A reference to this object.
    ///
    /// @see https://www.mongodb.com/docs/manual/reference/method/getClientEncryption/
    ///
    data_key& key_alt_names(std::vector<std::string> key_alt_names);

    ///
    /// Gets the alternate names for the data key.
    ///
    /// @return
    ///   The alternate names for the data key.
    ///
    const std::vector<std::string>& key_alt_names() const;

    ///
    /// Sets the binary data for the key material
    ///
    /// An optional BinData of 96 bytes to use as custom key material for the
    /// data key being created. If keyMaterial is given, the custom key material
    /// is used for encrypting and decrypting data.
    ///
    /// Otherwise, the key material for the new data key is generated from a
    /// cryptographically secure random device.
    ///
    /// @param key_material
    ///   The binary data for the keyMaterial
    ///
    /// @return
    ///   A reference to this object.
    ///
    /// @see https://www.mongodb.com/docs/v6.0/reference/method/KeyVault.createKey/
    ///
    using key_material_type = std::vector<uint8_t>;
    data_key& key_material(key_material_type key_material);

    ///
    /// Gets the keyMaterial as binary data
    ///
    /// @return
    ///   The binary data for the key material
    ///
    /// @see https://www.mongodb.com/docs/v6.0/reference/method/KeyVault.createKey/
    ///
    const stdx::optional<key_material_type>& key_material();

   private:
    friend ::mongocxx::v_noabi::client_encryption;

    MONGOCXX_PRIVATE void* convert() const;

    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _master_key;
    std::vector<std::string> _key_alt_names;
    stdx::optional<key_material_type> _key_material;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

// CXX-2770: missing include of postlude header.
#if defined(MONGOCXX_TEST_MACRO_GUARDS_FIX_MISSING_POSTLUDE)
#include <mongocxx/config/postlude.hpp>
#endif
