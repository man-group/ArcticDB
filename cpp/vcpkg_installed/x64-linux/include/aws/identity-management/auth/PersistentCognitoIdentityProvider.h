/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#pragma once

#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/identity-management/IdentityManagment_EXPORTS.h>
#include <mutex>

namespace Aws
{
    namespace Utils
    {
        namespace Json
        {
            class JsonView;
            class JsonValue;
        }
    }
    namespace Auth
    {
        struct LoginAccessTokens
        {
            LoginAccessTokens() : longTermTokenExpiry(0) {}
            Aws::String accessToken;
            Aws::String longTermToken;
            long long longTermTokenExpiry;
        };

        class AWS_IDENTITY_MANAGEMENT_API PersistentCognitoIdentityProvider
        {
        public:
            virtual ~PersistentCognitoIdentityProvider() = default;
            virtual bool HasIdentityId() const = 0;
            virtual bool HasLogins() const = 0;
            virtual Aws::String GetIdentityId() const = 0;
            virtual Aws::Map<Aws::String, LoginAccessTokens> GetLogins() = 0;
            virtual Aws::String GetAccountId() const = 0;
            virtual Aws::String GetIdentityPoolId() const = 0;
            virtual void PersistIdentityId(const Aws::String&) = 0;
            virtual void PersistLogins(const Aws::Map<Aws::String, LoginAccessTokens>&) = 0;            
            inline void ClearLogins() { PersistLogins(Aws::Map<Aws::String, LoginAccessTokens>()); }
            inline void ClearIdentity() { PersistIdentityId(""); }
            inline void Logout() { ClearIdentity(); ClearLogins(); }

            inline void SetLoginsUpdatedCallback(const std::function<void(const PersistentCognitoIdentityProvider&)>& callback)
            {
                m_loginsUpdatedCallback = callback;
            }

            inline void SetIdentityIdUpdatedCallback(const std::function<void(const PersistentCognitoIdentityProvider&)>& callback)
            {
                m_identityIdUpdatedCallback = callback;
            }

        protected:
            std::function<void(const PersistentCognitoIdentityProvider&)> m_loginsUpdatedCallback;
            std::function<void(const PersistentCognitoIdentityProvider&)> m_identityIdUpdatedCallback;
        };

        class AWS_IDENTITY_MANAGEMENT_API PersistentCognitoIdentityProvider_JsonFileImpl : public PersistentCognitoIdentityProvider
        {
        public:
            PersistentCognitoIdentityProvider_JsonFileImpl(const Aws::String& identityPoolId, const Aws::String& accountId, bool disableCaching = false);
            PersistentCognitoIdentityProvider_JsonFileImpl(const Aws::String& identityPoolId, bool disableCaching = false);
            PersistentCognitoIdentityProvider_JsonFileImpl(const Aws::String& identityPoolId, const Aws::String& accountId, const char* identitiesFilePath, bool disableCaching = false);

            bool HasIdentityId() const override;
            bool HasLogins() const override;
            Aws::String GetIdentityId() const override;
            Aws::Map<Aws::String, LoginAccessTokens> GetLogins() override;
            Aws::String GetAccountId() const override { return m_accountId; }
            Aws::String GetIdentityPoolId() const override { return m_identityPoolId; }
            void PersistIdentityId(const Aws::String&) override;
            void PersistLogins(const Aws::Map<Aws::String, LoginAccessTokens>&) override;

        private:
            Utils::Json::JsonValue LoadJsonDocFromFile() const;
            void PersistChangesToFile(const Utils::Json::JsonValue&) const;
            void LoadAndParseDoc();

            static void BuildLoginsMap(Aws::Map<Aws::String, Aws::Utils::Json::JsonView>, Aws::Map<Aws::String, LoginAccessTokens>& logins);

            std::mutex m_docMutex;
            Aws::String m_identityPoolId;
            Aws::String m_accountId;
            Aws::String m_identityId;
            Aws::Map<Aws::String, LoginAccessTokens> m_logins;
            Aws::String m_identityFilePath;
            bool m_disableCaching;
        };

        typedef PersistentCognitoIdentityProvider_JsonFileImpl DefaultPersistentCognitoIdentityProvider;
    }
}
