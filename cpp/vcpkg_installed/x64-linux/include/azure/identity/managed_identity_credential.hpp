// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/**
 * @file
 * @brief Managed Identity Credential and options.
 */

#pragma once

#include <azure/core/credentials/credentials.hpp>
#include <azure/core/credentials/token_credential_options.hpp>

#include <memory>
#include <string>

namespace Azure { namespace Identity {
  namespace _detail {
    class ManagedIdentitySource;
  }

  /**
   * @brief Attempts authentication using a managed identity that has been assigned to the
   * deployment environment. This authentication type works in Azure VMs, App Service and Azure
   * Functions applications, as well as the Azure Cloud Shell. More information about configuring
   * managed identities can be found here:
   * https://docs.microsoft.com/azure/active-directory/managed-identities-azure-resources/overview
   */
  class ManagedIdentityCredential final : public Core::Credentials::TokenCredential {
  private:
    std::unique_ptr<_detail::ManagedIdentitySource> m_managedIdentitySource;

  public:
    /**
     * @brief Destructs `%TokenCredential`.
     *
     */
    ~ManagedIdentityCredential() override;

    /**
     * @brief Constructs a Managed Identity Credential.
     *
     * @param clientId Client ID.
     * @param options Options for token retrieval.
     */
    explicit ManagedIdentityCredential(
        std::string const& clientId = std::string(),
        Azure::Core::Credentials::TokenCredentialOptions const& options
        = Azure::Core::Credentials::TokenCredentialOptions());

    /**
     * @brief Constructs a Managed Identity Credential.
     *
     * @param options Options for token retrieval.
     */
    explicit ManagedIdentityCredential(
        Azure::Core::Credentials::TokenCredentialOptions const& options);

    /**
     * @brief Gets an authentication token.
     *
     * @param tokenRequestContext A context to get the token in.
     * @param context A context to control the request lifetime.
     *
     * @return Authentication token.
     *
     * @throw Azure::Core::Credentials::AuthenticationException Authentication error occurred.
     */
    Core::Credentials::AccessToken GetToken(
        Core::Credentials::TokenRequestContext const& tokenRequestContext,
        Core::Context const& context) const override;
  };

}} // namespace Azure::Identity
