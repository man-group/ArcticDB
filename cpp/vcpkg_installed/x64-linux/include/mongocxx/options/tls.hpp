// Copyright 2019 MongoDB Inc.
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
#include <bsoncxx/string/view_or_value.hpp>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN
namespace options {

///
/// Class representing the optional arguments to a MongoDB driver client (TLS)
///
class MONGOCXX_API tls {
   public:
    ///
    /// The path to the .pem file containing a public key certificate and its associated private
    /// key.
    ///
    /// @param pem_file
    ///   The path to the .pem file.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& pem_file(bsoncxx::string::view_or_value pem_file);

    ///
    /// Retrieves the current path to the .pem file.
    ///
    /// @return The path to the .pem file.
    ///
    const stdx::optional<bsoncxx::string::view_or_value>& pem_file() const;

    ///
    /// The pass phrase used to decrypt an encrypted PEM file.
    ///
    /// @param pem_password
    ///   The pass phrase.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& pem_password(bsoncxx::string::view_or_value pem_password);

    ///
    /// Retrieves the current decryption pass phrase.
    ///
    /// @return The pass phrase.
    ///
    const stdx::optional<bsoncxx::string::view_or_value>& pem_password() const;

    ///
    /// The path to the .pem file that contains the root certificate chain from the Certificate
    /// Authority.
    ///
    /// @param ca_file
    ///   The path to the CA file.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& ca_file(bsoncxx::string::view_or_value ca_file);

    ///
    /// Retrieves the current path to the CA file.
    ///
    /// @return The path to the CA file.
    ///
    const stdx::optional<bsoncxx::string::view_or_value>& ca_file() const;

    ///
    /// The path to the Certificate Authority directory.
    ///
    /// @param ca_dir
    ///   The path to the CA directory.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& ca_dir(bsoncxx::string::view_or_value ca_dir);

    ///
    /// Retrieves the current path to the CA directory.
    ///
    /// @return The path to the CA directory.
    ///
    const stdx::optional<bsoncxx::string::view_or_value>& ca_dir() const;

    ///
    /// The path to the .pem file that contains revoked certificates.
    ///
    /// @param crl_file
    ///   The path to the PEM file.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& crl_file(bsoncxx::string::view_or_value crl_file);

    ///
    /// Retrieves the current path to the .pem file that contains revoked certificates.
    ///
    /// @return The path to the revoked certificates file.
    ///
    const stdx::optional<bsoncxx::string::view_or_value>& crl_file() const;

    ///
    /// If true, the driver will not verify the server's CA file.
    ///
    /// @param allow_invalid_certificates
    ///   Whether or not to check the server's CA file.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    tls& allow_invalid_certificates(bool allow_invalid_certificates);

    ///
    /// Retrieves whether or not the driver will check the server's CA file.
    ///
    /// @return Whether or not the driver will check the server's CA file.
    ///
    const stdx::optional<bool>& allow_invalid_certificates() const;

   private:
    stdx::optional<bsoncxx::string::view_or_value> _pem_file;
    stdx::optional<bsoncxx::string::view_or_value> _pem_password;
    stdx::optional<bsoncxx::string::view_or_value> _ca_file;
    stdx::optional<bsoncxx::string::view_or_value> _ca_dir;
    stdx::optional<bsoncxx::string::view_or_value> _crl_file;
    stdx::optional<bool> _allow_invalid_certificates;
};

}  // namespace options
MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
