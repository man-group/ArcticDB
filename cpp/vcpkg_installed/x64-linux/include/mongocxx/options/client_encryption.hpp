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

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;
class client_encryption;

namespace options {

///
/// Class representing options for the object managing explicit client-side encryption.
///
class MONGOCXX_API client_encryption {
   public:
    ///
    /// When the key vault collection is on a separate MongoDB cluster,
    /// sets the optional client to use to route data key queries to
    /// that cluster.
    ///
    /// @param client
    ///   A client to use for routing queries to the key vault collection.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/security-client-side-encryption/
    ///
    client_encryption& key_vault_client(mongocxx::client* client);

    ///
    /// Gets the key vault client.
    ///
    /// @return
    ///   An optional pointer to the key vault client.
    ///
    const stdx::optional<mongocxx::client*>& key_vault_client() const;

    ///
    /// Sets the namespace to use to access the key vault collection, which
    /// contains all data keys used for encryption and decryption. This
    /// option must be set:
    ///
    ///   client_encryption.key_vault_namespace({ "db", "coll" });
    ///
    /// @param ns
    ///   A std::pair of strings representing the db and collection to use
    ///   to access the key vault.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/security-client-side-encryption/
    ///
    using ns_pair = std::pair<std::string, std::string>;
    client_encryption& key_vault_namespace(ns_pair ns);

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
    /// Multiple KMS providers may be specified. Supported KMS providers are
    /// "aws", "azure", "gcp", "kmip", and "local". The kmsProviders map values differ
    /// by provider:
    ///
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
    ///
    /// @param kms_providers
    ///   A document containing the KMS providers.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/security-client-side-encryption/
    ///
    client_encryption& kms_providers(bsoncxx::document::view_or_value kms_providers);

    ///
    /// Gets the KMS providers.
    ///
    /// @return
    ///   An optional document containing the KMS providers.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& kms_providers() const;

    ///
    /// Sets the TLS options to use for client side encryption with a given KMS provider.
    ///
    /// Multiple KMS providers may be specified. Supported KMS providers are "aws", "azure", "gcp",
    /// and "kmip". The map value has the same form for all supported providers:
    ///
    ///    <KMS provider name>: {
    ///        tlsCaFile: Optional<String>
    ///        tlsCertificateKeyFile: Optional<String>
    ///        tlsCertificateKeyFilePassword: Optional<String>
    ///    }
    ///
    /// @param tls_opts
    ///   A document containing the TLS options.
    ///
    /// @return
    ///   A reference to this object to facilitate method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/core/security-client-side-encryption/
    ///
    client_encryption& tls_opts(bsoncxx::document::view_or_value tls_opts);

    ///
    /// Gets the TLS options.
    ///
    /// @return
    ///   An optional document containing the TLS options.
    ///
    const stdx::optional<bsoncxx::document::view_or_value>& tls_opts() const;

   private:
    friend class mongocxx::client_encryption;

    MONGOCXX_PRIVATE void* convert() const;

    stdx::optional<mongocxx::client*> _key_vault_client;
    stdx::optional<ns_pair> _key_vault_namespace;
    stdx::optional<bsoncxx::document::view_or_value> _kms_providers;
    stdx::optional<bsoncxx::document::view_or_value> _tls_opts;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
