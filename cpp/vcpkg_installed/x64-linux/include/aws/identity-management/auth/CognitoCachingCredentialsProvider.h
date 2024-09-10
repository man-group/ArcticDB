/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/cognito-identity/CognitoIdentityClient.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/identity-management/IdentityManagment_EXPORTS.h>
#include <aws/core/utils/DateTime.h>
#include <mutex>

namespace Aws
{
    namespace Auth
    {
        class PersistentCognitoIdentityProvider;

        /**
         * Credentials provider that can be used to authenticate a user with any other AWS service.
         * This credentials provider can be passed to any other Aws Client.
         */
        class AWS_IDENTITY_MANAGEMENT_API CognitoCachingCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
        {
        public:
            AWSCredentials GetAWSCredentials() override;

        protected:
            /**
             * PersistentCognitoIdentityProvider is responsible for storing and retrieving cognito identity information
             * from memory or file system.
             */
            CognitoCachingCredentialsProvider(const std::shared_ptr<PersistentCognitoIdentityProvider>& m_identityRepository,
                                              const std::shared_ptr<CognitoIdentity::CognitoIdentityClient>& cognitoIdentityClient = nullptr);

            virtual CognitoIdentity::Model::GetCredentialsForIdentityOutcome GetCredentialsFromCognito() const = 0;

            bool IsTimeExpired(double expiry);

            std::shared_ptr<CognitoIdentity::CognitoIdentityClient> m_cognitoIdentityClient;
            std::shared_ptr<PersistentCognitoIdentityProvider> m_identityRepository;

        private:
            void OnLoginsUpdated(const PersistentCognitoIdentityProvider&);

            AWSCredentials m_cachedCredentials;
            std::atomic<double> m_expiry;
            std::mutex m_credsMutex;
        };

        /**
         * Uses cognito anonymous process to get credentials for a given identity pool. Use this if you don't
         * have open ids to identify the user.
         */
        class AWS_IDENTITY_MANAGEMENT_API CognitoCachingAnonymousCredentialsProvider : public CognitoCachingCredentialsProvider
        {
        public:
            /**
             * PersistentCognitoIdentityProvider is responsible for storing and retrieving cognito identity information
             * from memory or file system.
             */
            CognitoCachingAnonymousCredentialsProvider(const std::shared_ptr<PersistentCognitoIdentityProvider>& identityRepository,
                        const std::shared_ptr<CognitoIdentity::CognitoIdentityClient>& cognitoIdentityClient = nullptr);

            CognitoCachingAnonymousCredentialsProvider(const Aws::String& accountId, const Aws::String& identityPoolId,
                        const std::shared_ptr<CognitoIdentity::CognitoIdentityClient>& cognitoIdentityClient = nullptr);

            CognitoCachingAnonymousCredentialsProvider(const Aws::String& identityPoolId,
                        const std::shared_ptr<CognitoIdentity::CognitoIdentityClient>& cognitoIdentityClient = nullptr);

        protected:
            CognitoIdentity::Model::GetCredentialsForIdentityOutcome GetCredentialsFromCognito() const override;
        };

        /**
         * Uses cognito authenticated process to get credentials for a given identity pool using a list of open ids
         * and auth providers to authenticate the user. Use this if you have an open id to identify the user.
         */
        class AWS_IDENTITY_MANAGEMENT_API CognitoCachingAuthenticatedCredentialsProvider : public CognitoCachingCredentialsProvider
        {
        public:
            /**
             * PersistentCognitoIdentityProvider is responsible for storing and retrieving cognito-identity information
             * from memory or file system.
             */
            CognitoCachingAuthenticatedCredentialsProvider(const std::shared_ptr<PersistentCognitoIdentityProvider>& identityRepository,
                                                           const std::shared_ptr<CognitoIdentity::CognitoIdentityClient>& cognitoIdentityClient = nullptr);

        protected:
            CognitoIdentity::Model::GetCredentialsForIdentityOutcome GetCredentialsFromCognito() const override;
        };
    }
}