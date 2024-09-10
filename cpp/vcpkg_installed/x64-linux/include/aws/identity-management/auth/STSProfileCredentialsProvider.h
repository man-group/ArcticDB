
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/identity-management/IdentityManagment_EXPORTS.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <functional>
#include <chrono>

namespace Aws
{
    namespace STS
    {
        class STSClient;
    }

    namespace Auth
    {
        /**
         * Credentials provider for STS Assume Role using the information in the shared config file.
         * The shared configuration file is typically created using the AWS CLI and is located in: ~/.aws/config
         * The location of the file can also be controlled via environment variables.
         * For more information see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
         */
        class AWS_IDENTITY_MANAGEMENT_API STSProfileCredentialsProvider : public AWSCredentialsProvider
        {
        public:
            /**
             * Use the default profile name.
             * The default profile name can be set using environment variables; otherwise it is the literal "default".
             */
            STSProfileCredentialsProvider();

            /**
             * Use the provided profile name from the shared configuration file.
             *
             * @param profileName The name of the profile in the shared configuration file.
             * @param duration The duration, in minutes, of the role session, after which the credentials are expired.
             * The value can range from 15 minutes up to the maximum session duration setting for the role. By default,
             * the duration is set to 1 hour.
             * Note: This credential provider refreshes the credentials 5 minutes before their expiration time. That
             * ensures the credentials do not expire between the time they're checked and the time they're returned to
             * the user.
             * If the duration for the credentials is 5 minutes or less, the provider will refresh the credentials only
             * when they expire.
             *
             */
            STSProfileCredentialsProvider(const Aws::String& profileName, std::chrono::minutes duration = std::chrono::minutes(60));

            STSProfileCredentialsProvider(const Aws::String& profileName, std::chrono::minutes duration, const std::function<Aws::STS::STSClient*(const AWSCredentials&)> &stsClientFactory);

            /**
             * Fetches the credentials set from STS following the rules defined in the shared configuration file.
             */
            AWSCredentials GetAWSCredentials() override;

        protected:
            void RefreshIfExpired();
            void Reload() override;
            /**
             * Assumes a role given its ARN. Communication with STS is done through the provided credentials.
             * Returns the assumed role credentials or empty credentials on error.
             */
            AWSCredentials GetCredentialsFromSTS(const AWSCredentials& credentials, const Aws::String& roleARN);
        private:
            AWSCredentials GetCredentialsFromSTSInternal(const Aws::String& roleArn, Aws::STS::STSClient* client);

            Aws::String m_profileName;
            AWSCredentials m_credentials;
            const std::chrono::minutes m_duration;
            const std::chrono::milliseconds m_reloadFrequency;
            std::function<Aws::STS::STSClient*(const AWSCredentials&)> m_stsClientFactory;
        };
    }
}
