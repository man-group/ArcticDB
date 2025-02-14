/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/identity-management/IdentityManagment_EXPORTS.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <memory>
#include <mutex>
#include <atomic>

namespace Aws
{
    namespace STS
    {
        class STSClient;
    }

    namespace Auth
    {
        /**
         * The default credential lifetime is 15 minutes
         */
        static const int DEFAULT_CREDS_LOAD_FREQ_SECONDS = 900;

        /**
         * Credentials provider for STS Assume Role
         */
        class AWS_IDENTITY_MANAGEMENT_API STSAssumeRoleCredentialsProvider : public AWSCredentialsProvider
        {
        public:
            /**
             *
             * Initializes credentials provider with
             * roleArn - required, this is the arn for the role you want to assume.
             * sessionName - if not specified, a unique session name will be generated for you.
             * externalId - if not specified, it will not be sent to STS.
             * loadFrequency, defaults to 15 minutes.
             * stsClient, sts client implementation to use.
             *
             * For more information, see:
             *    http://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
             */
            STSAssumeRoleCredentialsProvider(const Aws::String& roleArn, const Aws::String& sessionName = Aws::String(), 
                const Aws::String& externalId = Aws::String(), int loadFrequency = DEFAULT_CREDS_LOAD_FREQ_SECONDS, 
                const std::shared_ptr<Aws::STS::STSClient>& stsClient = nullptr);

            AWSCredentials GetAWSCredentials() override;

        private:
            void LoadCredentialsFromSTS();

            std::shared_ptr<Aws::STS::STSClient> m_stsClient;
            AWSCredentials m_cachedCredentials;
            Aws::String m_roleArn;
            Aws::String m_sessionName;
            Aws::String m_externalId;
            std::atomic<int64_t> m_expiry;
            std::mutex m_credsMutex;
            std::atomic<int> m_loadFrequency;
        };
    }
}
