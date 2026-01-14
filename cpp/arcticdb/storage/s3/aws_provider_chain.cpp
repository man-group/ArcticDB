/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/identity-management/auth/STSProfileCredentialsProvider.h>
#include <aws/core/auth/SSOCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <arcticdb/storage/s3/aws_provider_chain.hpp>

static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
static const char DefaultCredentialsProviderChainTag[] = "DefaultAWSCredentialsProviderChain";

// Definition of own chain to work around https://github.com/aws/aws-sdk-cpp/issues/150
// NOTE: These classes are not currently in use and may only be required if we need the STSProfileCred provider.
namespace arcticdb::storage::s3 {

using namespace Aws::Auth;

MyAWSCredentialsProviderChain::MyAWSCredentialsProviderChain() : Aws::Auth::AWSCredentialsProviderChain() {
    AddProvider(Aws::MakeShared<EnvironmentAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProfileConfigFileAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProcessCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<STSAssumeRoleWebIdentityCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<SSOCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<STSProfileCredentialsProvider>(DefaultCredentialsProviderChainTag));

    // ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set
    const auto relativeUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
    AWS_LOGSTREAM_DEBUG(
            DefaultCredentialsProviderChainTag,
            "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI << " is " << relativeUri
    );

    const auto absoluteUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
    AWS_LOGSTREAM_DEBUG(
            DefaultCredentialsProviderChainTag,
            "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI << " is " << absoluteUri
    );

    const auto ec2MetadataDisabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
    AWS_LOGSTREAM_DEBUG(
            DefaultCredentialsProviderChainTag,
            "The environment variable value " << AWS_EC2_METADATA_DISABLED << " is " << ec2MetadataDisabled
    );

    if (!relativeUri.empty()) {
        AddProvider(
                Aws::MakeShared<TaskRoleCredentialsProvider>(DefaultCredentialsProviderChainTag, relativeUri.c_str())
        );
        AWS_LOGSTREAM_INFO(
                DefaultCredentialsProviderChainTag,
                "Added ECS metadata service credentials provider with relative path: [" << relativeUri
                                                                                        << "] to the provider chain."
        );
    } else if (!absoluteUri.empty()) {
        const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
        AddProvider(Aws::MakeShared<TaskRoleCredentialsProvider>(
                DefaultCredentialsProviderChainTag, absoluteUri.c_str(), token.c_str()
        ));

        // DO NOT log the value of the authorization token for security purposes.
        AWS_LOGSTREAM_INFO(
                DefaultCredentialsProviderChainTag,
                "Added ECS credentials provider with URI: [" << absoluteUri << "] to the provider chain with a"
                                                             << (token.empty() ? "n empty " : " non-empty ")
                                                             << "authorization token."
        );
    } else if (Aws::Utils::StringUtils::ToLower(ec2MetadataDisabled.c_str()) != "true") {
        AddProvider(Aws::MakeShared<InstanceProfileCredentialsProvider>(DefaultCredentialsProviderChainTag));
        AWS_LOGSTREAM_INFO(
                DefaultCredentialsProviderChainTag,
                "Added EC2 metadata service credentials provider to the provider chain."
        );
    }
}

} // namespace arcticdb::storage::s3
