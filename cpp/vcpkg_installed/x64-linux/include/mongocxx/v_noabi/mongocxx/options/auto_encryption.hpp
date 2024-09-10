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

#include <mongocxx/client-fwd.hpp>
#include <mongocxx/options/auto_encryption-fwd.hpp>
#include <mongocxx/pool-fwd.hpp>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

///
/// Class representing options for automatic client-side encryption.
///
class auto_encryption {
   public:
    ///
    /// Default constructs a new auto_encryption object.
    ///
    auto_encryption() noexcept;

    ///
    /// When the key vault collection is on a separate MongoDB cluster,
    /// sets the optional client to use to route data key queries to
    /// that cluster.
    ///
    /// The given key vault client MUST outlive any client that has
    /// been enabled to use it through these options.
    ///
    /// @param client
    ///   A client to use for routing queries to the key vault collection.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& key_vault_client(mongocxx::v_noabi::client* client);

    ///
    /// Gets the key vault client.
    ///
    /// @return
    ///   An optional pointer to the key vault client.
    ///
    const stdx::optional<mongocxx::v_noabi::client*>& key_vault_client() const;

    ///
    /// When the key vault collection is on a separate MongoDB cluster,
    /// sets the optional client pool to use to route data key queries to
    /// that cluster.
    ///
    /// This option may not be used if a key_vault_client is set.
    ///
    /// The given key vault pool MUST outlive any pool that has
    /// been enabled to use it through these options.
    ///
    /// May only be set when enabling automatic encryption on a pool.
    ///
    /// @param pool
    ///   A pool to use for routing queries to the key vault collection.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& key_vault_pool(mongocxx::v_noabi::pool* pool);

    ///
    /// Gets the key vault pool.
    ///
    /// @return
    ///   An optional pointer to the key vault pool.
    ///
    const stdx::optional<mongocxx::v_noabi::pool*>& key_vault_pool() const;

    ///
    /// Sets the namespace to use to access the key vault collection, which
    /// contains all data keys used for encryption and decryption. This
    /// option must be set:
    ///
    ///   auto_encryption.key_vault_namespace({ "db", "coll" });
    ///
    /// @param ns
    ///   A std::pair of strings representing the db and collection to use
    ///   to access the key vault.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    using ns_pair = std::pair<std::string, std::string>;
    auto_encryption& key_vault_namespace(ns_pair ns);

    ///
    /// Gets the key vault namespace.
    ///
    /// @return
    ///   An optional pair of strings representing the namespace of the
    ///   key vault collection.
    ///
    const stdx::optional<ns_pair>& key_vault_namespace() const;

    ///
    /// Sets the KMS providers to use for client side encryption.
    ///
    /// Multiple KMS providers may be specified. The following KMS providers are
    /// supported: "aws", "azure", "gcp", "kmip", and "local". The kmsProviders map values differ
    /// by provider:
    ///
    /// @code{.unparsed}
    ///    aws: {
    ///      accessKeyId: String,
    ///      secretAccessKey: String
    ///    }
    ///
    ///    azure: {
    ///       tenantId: String,
    ///       clientId: String,
    ///       clientSecret: String,
    ///       identityPlatformEndpoint: Optional<String> // Defaults to login.microsoftonline.com
    ///    }
    ///
    ///    gcp: {
    ///       email: String,
    ///       privateKey: byte[] or String, // May be passed as a base64 encoded string.
    ///       endpoint: Optional<String> // Defaults to oauth2.googleapis.com
    ///    }
    ///
    ///    kmip: {
    ///       endpoint: String
    ///    }
    ///
    ///    local: {
    ///      key: byte[96] // The master key used to encrypt/decrypt data keys.
    ///    }
    /// @endcode
    ///
    /// @param kms_providers
    ///   A document containing the KMS providers.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& kms_providers(bsoncxx::v_noabi::document::view_or_value kms_providers);

    ///
    /// Gets the KMS providers.
    ///
    /// @return
    ///   An optional document containing the KMS providers.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& kms_providers() const;

    ///
    /// Sets the TLS options to use for client side encryption with a given KMS provider.
    ///
    /// Multiple KMS providers may be specified. Supported KMS providers are "aws", "azure", "gcp",
    /// and "kmip". The map value has the same form for all supported providers:
    ///
    /// @code{.unparsed}
    ///    <KMS provider name>: {
    ///        tlsCaFile: Optional<String>
    ///        tlsCertificateKeyFile: Optional<String>
    ///        tlsCertificateKeyFilePassword: Optional<String>
    ///    }
    /// @endcode
    ///
    /// @param tls_opts
    ///   A document containing the TLS options.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& tls_opts(bsoncxx::v_noabi::document::view_or_value tls_opts);

    ///
    /// Gets the TLS options.
    ///
    /// @return
    ///   An optional document containing the TLS options.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& tls_opts() const;

    ///
    /// Sets a local JSON schema.
    ///
    /// Supplying a schemaMap provides more security than relying on
    /// JSON schemas obtained from the server. It protects against a
    /// malicious server advertising a false JSON Schema, which could
    /// trick the client into sending unencrypted data that should be
    /// encrypted.
    ///
    /// Schemas supplied in the schemaMap only apply to configuring
    /// automatic encryption for client side encryption. Other validation
    /// rules in the JSON schema will not be enforced by the driver and
    /// will result in an error.
    ///
    /// @param schema_map
    ///   The JSON schema to use.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& schema_map(bsoncxx::v_noabi::document::view_or_value schema_map);

    ///
    /// Gets the schema map.
    ///
    /// @return
    ///   An optional document containing the schema map.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& schema_map() const;

    ///
    /// Sets the local encrypted fields map.
    ///
    /// Supplying an encryptedFieldsMap provides more security than relying on
    /// an encryptedFields obtained from the server. It protects against a
    /// malicious server advertising a false encryptedFields.
    ///
    /// @param encrypted_fields_map
    ///   The mapping of which fields to encrypt.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& encrypted_fields_map(
        bsoncxx::v_noabi::document::view_or_value encrypted_fields_map);

    ///
    /// Get encrypted fields map
    ///
    /// @return
    ///   An optional document containing the encrypted fields map
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& encrypted_fields_map() const;

    ///
    /// Automatic encryption is disabled when the 'bypassAutoEncryption'
    /// option is true. Default is 'false,' so auto encryption is enabled.
    ///
    /// @param should_bypass
    ///   Whether or not to bypass automatic encryption.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& bypass_auto_encryption(bool should_bypass);

    ///
    /// Gets a boolean specifying whether or not auto encryption is bypassed.
    ///
    /// @return
    ///   A boolean specifying whether auto encryption is bypassed.
    ///
    bool bypass_auto_encryption() const;

    ///
    /// Query analysis is disabled when the 'bypassQueryAnalysis'
    /// option is true. Default is 'false' (i.e. query analysis is enabled).
    ///
    /// @param should_bypass
    ///   Whether or not to bypass query analysis.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& bypass_query_analysis(bool should_bypass);

    ///
    /// Gets a boolean specifying whether or not query analysis is bypassed.
    ///
    /// @return
    ///   A boolean specifying whether query analysis is bypassed.
    ///
    bool bypass_query_analysis() const;

    ///
    /// Set extra options related to the mongocryptd process. This options
    /// document may include the following fields:
    ///
    /// - mongocryptdURI: string, defaults to "mongodb://localhost:27020".
    ///
    /// - mongocryptdBypassSpawn: bool, defaults to false.
    ///
    /// - mongocryptdSpawnPath: string, defaults to "" and spawns mongocryptd
    ///   from the system path.
    ///
    /// - mongocryptdSpawnArgs: array[strings], options passed to mongocryptd
    ///   when spawing. Defaults to ["--idleShutdownTimeoutSecs=60"].
    ///
    /// - cryptSharedLibPath - Set a filepath string referring to a crypt_shared library file. Unset
    ///   by default. If not set (the default), libmongocrypt will attempt to load crypt_shared
    ///   using the host system’s default dynamic-library-search system.
    ///
    ///   If set, the given path should identify the crypt_shared dynamic library file itself, not
    ///   the directory that contains it.
    ///
    ///   If the given path is a relative path and the first path component is $ORIGIN, the $ORIGIN
    ///   component will be replaced with the absolute path to the directory containing the
    ///   libmongocrypt library in use by the application.
    ///
    ///   Note No other RPATH/RUNPATH-style substitutions are available.
    ///   If the given path is a relative path, the path will be resolved relative to the working
    ///   directory of the operating system process.
    ///
    ///   If this option is set and libmongocrypt fails to load crypt_shared from the given
    ///   filepath, libmongocrypt will fail to initialize and will not attempt to search for
    ///   crypt_shared in any other locations.
    ///
    /// - cryptSharedLibRequired - If set to true, and libmongocrypt fails to load a crypt_shared
    ///   library, initialization of auto-encryption will fail immediately and will not attempt to
    ///   spawn mongocryptd.
    ///
    ///   If set to false (the default), cryptSharedLibPath is not set, and libmongocrypt fails to
    ///   load crypt_shared, then libmongocrypt will proceed without crypt_shared and fall back to
    ///   using mongocryptd.
    ///
    /// @param extra
    ///   The extra options to set.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://www.mongodb.com/docs/manual/core/security-client-side-encryption/
    ///
    auto_encryption& extra_options(bsoncxx::v_noabi::document::view_or_value extra);

    ///
    /// Gets extra options related to the mongocryptd process.
    ///
    /// @return
    ///   An optional document containing the extra options.
    ///
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& extra_options() const;

   private:
    friend ::mongocxx::v_noabi::client;
    friend ::mongocxx::v_noabi::pool;

    MONGOCXX_PRIVATE void* convert() const;

    bool _bypass;
    bool _bypass_query_analysis;
    stdx::optional<mongocxx::v_noabi::client*> _key_vault_client;
    stdx::optional<mongocxx::v_noabi::pool*> _key_vault_pool;
    stdx::optional<ns_pair> _key_vault_namespace;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _kms_providers;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _tls_opts;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _schema_map;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _encrypted_fields_map;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _extra_options;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

// CXX-2770: missing include of postlude header.
#if defined(MONGOCXX_TEST_MACRO_GUARDS_FIX_MISSING_POSTLUDE)
#include <mongocxx/config/postlude.hpp>
#endif
