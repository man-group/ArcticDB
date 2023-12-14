// Copyright 2014 MongoDB Inc.
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

#include <memory>
#include <string>
#include <vector>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

///
/// Class representing a MongoDB connection string URI.
///
/// @todo CXX-2038 Use optionals for all option getters. Otherwise users cannot distinguish between
/// a case of an empty option explicitly present in the URI and the option being omitted. Also
/// change getters that return a std::string to return a string_view since a copy is not necessary.
///
/// @see https://docs.mongodb.com/manual/reference/connection-string/
/// @see http://mongoc.org/libmongoc/current/mongoc_uri_t.html for more information on supported URI
/// options.
///
class MONGOCXX_API uri {
   public:
    /// A host.
    struct host {
        std::string name;
        std::uint16_t port;
        std::int32_t family;
    };

    static const std::string k_default_uri;

    ///
    /// Constructs a uri from an optional MongoDB URI string. If no URI string is specified,
    /// uses the default URI string, 'mongodb://localhost:27017'.
    ///
    /// @see The documentation for the version of libmongoc used for the list of supported
    ///   URI options. For the latest libmongoc release:
    ///   http://mongoc.org/libmongoc/current/mongoc_uri_t.html
    ///
    /// @param uri_string
    ///   String representing a MongoDB connection string URI, defaults to k_default_uri.
    ///
    uri(bsoncxx::string::view_or_value uri_string = k_default_uri);

    ///
    /// Move constructs a uri.
    ///
    uri(uri&&) noexcept;

    ///
    /// Move assigns a uri.
    ///
    uri& operator=(uri&&) noexcept;

    ///
    /// Destroys a uri.
    ///
    ~uri();

    ///
    /// Returns the authentication mechanism from the uri.
    ///
    /// @return A string representing the authentication mechanism.
    ///
    std::string auth_mechanism() const;

    ///
    /// Returns the authentication source from the uri.
    ///
    /// @return A string representing the authentication source.
    ///
    std::string auth_source() const;

    ///
    /// Returns the hosts from the uri.
    ///
    /// @return A vector of hosts.
    ///
    std::vector<host> hosts() const;

    ///
    /// Returns the database from the uri.
    ///
    /// @return A string with the name of the database.
    ///
    std::string database() const;

    ///
    /// Returns other uri options.
    ///
    /// Note, options are returned in the case they were presented.
    /// The URI mongodb://localhost/?appName=abc will return { "appName": "abc" }
    /// The URI mongodb://localhost/?appname=abc will return { "appname": "abc" }
    ///
    /// @return A document view containing other options.
    ///
    bsoncxx::document::view options() const;

    ///
    /// Returns the password from the uri.
    ///
    /// @return A string containing the supplied password.
    ///
    std::string password() const;

    ///
    /// Returns the read concern from the uri.
    ///
    /// @return A read_concern that represents what was specified in the uri.
    ///
    class read_concern read_concern() const;

    ///
    /// Returns the read preference from the uri.
    ///
    /// @return A read_preference that represents what was specified in the uri.
    ///
    class read_preference read_preference() const;

    ///
    /// Returns the replica set specified in the uri.
    ///
    /// @return A string representing the supplied replica set name.
    ///
    std::string replica_set() const;

    ///
    /// Returns the ssl parameter from the uri.
    ///
    /// @return Boolean that is @c true if ssl is enabled and @c false if not.
    ///
    /// @deprecated The tls() method should be used instead of this method.
    ///
    MONGOCXX_DEPRECATED bool ssl() const;

    ///
    /// Returns the tls parameter from the uri.
    ///
    /// @return Boolean that is @c true if tls is enabled and @c false if not.
    ///
    bool tls() const;

    ///
    /// Returns the uri in a string format.
    ///
    /// @return A string with the uri.
    ///
    std::string to_string() const;

    ///
    /// Returns the supplied username from the uri.
    ///
    /// @return A string with the username specified in the uri.
    ///
    std::string username() const;

    ///
    /// Returns the write concern specified in the uri.
    ///
    /// @return A write_concern that represents what was specified in the uri.
    ///
    class write_concern write_concern() const;

    ///
    /// Returns the value of the option "appname" if present in the uri.
    ///
    /// @return An optional stdx::string_view
    ///
    stdx::optional<stdx::string_view> appname() const;

    ///
    /// Returns the value of the option "authMechanismProperties" if present in the uri.
    ///
    /// @return An optional bsoncxx::document::view
    ///
    stdx::optional<bsoncxx::document::view> auth_mechanism_properties() const;

    ///
    /// Returns the list of compressors present in the uri or an empty list if "compressors" was not
    /// present or contained no valid compressors.
    ///
    /// @return A std::vector of stdx::string_view.
    ///
    std::vector<stdx::string_view> compressors() const;

    ///
    /// Returns the value of the option "connectTimeoutMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> connect_timeout_ms() const;

    ///
    /// Returns the value of the option "directConnection" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> direct_connection() const;

    ///
    /// Returns the value of the option "heartbeatFrequencyMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> heartbeat_frequency_ms() const;

    ///
    /// Returns the value of the option "localThresholdMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> local_threshold_ms() const;

    ///
    /// Returns the value of the option "maxPoolSize" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> max_pool_size() const;

    ///
    /// Returns the value of the option "retryReads" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> retry_reads() const;

    ///
    /// Returns the value of the option "retryWrites" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> retry_writes() const;

    ///
    /// Returns the value of the option "serverSelectionTimeoutMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> server_selection_timeout_ms() const;

    ///
    /// Returns the value of the option "serverSelectionTryOnce" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> server_selection_try_once() const;

    ///
    /// Returns the value of the option "socketTimeoutMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> socket_timeout_ms() const;

    ///
    /// Returns the value of the option "tlsAllowInvalidCertificates" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> tls_allow_invalid_certificates() const;

    ///
    /// Returns the value of the option "tlsAllowInvalidHostnames" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> tls_allow_invalid_hostnames() const;

    ///
    /// Returns the value of the option "tlsCAFile" if present in the uri.
    ///
    /// @return An optional stdx::string_view
    ///
    stdx::optional<stdx::string_view> tls_ca_file() const;

    ///
    /// Returns the value of the option "tlsCertificateKeyFile" if present in the uri.
    ///
    /// @return An optional stdx::string_view
    ///
    stdx::optional<stdx::string_view> tls_certificate_key_file() const;

    ///
    /// Returns the value of the option "tlsCertificateKeyFilePassword" if present in the uri.
    ///
    /// @return An optional stdx::string_view
    ///
    stdx::optional<stdx::string_view> tls_certificate_key_file_password() const;

    ///
    /// Returns the value of the option "tlsDisableCertificateRevocationCheck" if present in the
    /// uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> tls_disable_certificate_revocation_check() const;

    ///
    /// Returns the value of the option "tlsDisableOCSPEndpointCheck" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> tls_disable_ocsp_endpoint_check() const;

    ///
    /// Returns the value of the option "tlsInsecure" if present in the uri.
    ///
    /// @return An optional bool
    ///
    stdx::optional<bool> tls_insecure() const;

    ///
    /// Returns the value of the option "waitQueueTimeoutMS" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> wait_queue_timeout_ms() const;

    ///
    /// Returns the value of the option "zlibCompressionLevel" if present in the uri.
    ///
    /// @return An optional std::int32_t
    ///
    stdx::optional<std::int32_t> zlib_compression_level() const;

   private:
    friend class client;
    friend class pool;

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE uri(std::unique_ptr<impl>&& implementation);

    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
