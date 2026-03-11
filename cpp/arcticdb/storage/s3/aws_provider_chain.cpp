/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/identity-management/auth/STSProfileCredentialsProvider.h>
#include <aws/core/auth/SSOCredentialsProvider.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleWithWebIdentityRequest.h>
#include <arcticdb/storage/s3/aws_provider_chain.hpp>

#include <fstream>

static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
static const char AWS_WEB_IDENTITY_TOKEN_FILE[] = "AWS_WEB_IDENTITY_TOKEN_FILE";
static const char AWS_ROLE_ARN[] = "AWS_ROLE_ARN";
static const char DefaultCredentialsProviderChainTag[] = "DefaultAWSCredentialsProviderChain";

// Custom credentials provider chain used for the DEFAULT_CREDENTIALS_PROVIDER_CHAIN auth method (_RBAC_ path).
// This avoids regressions in the SDK's default chain (see aws-sdk-cpp PR #3505, issues #3531, #3558, #3562)
// where the CRT-based STSAssumeRoleWebIdentityCredentialsProvider has caching and threading bugs.
namespace arcticdb::storage::s3 {

using namespace Aws::Auth;

static const char SafeSTSWebIdentityTag[] = "SafeSTSWebIdentityCredentialsProvider";

SafeSTSWebIdentityCredentialsProvider::SafeSTSWebIdentityCredentialsProvider() : m_initialized(false) {
    m_tokenFile = Aws::Environment::GetEnv(AWS_WEB_IDENTITY_TOKEN_FILE);
    m_roleArn = Aws::Environment::GetEnv(AWS_ROLE_ARN);
    m_sessionName = Aws::Environment::GetEnv("AWS_ROLE_SESSION_NAME");
    m_region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");

    // Fall back to config profile if env vars are incomplete (matches legacy STSAssumeRoleWebIdentityCredentialsProvider)
    if (m_roleArn.empty() || m_tokenFile.empty() || m_region.empty()) {
        auto profile = Aws::Config::GetCachedConfigProfile(Aws::Auth::GetConfigProfileName());
        if (m_region.empty()) {
            m_region = profile.GetRegion();
        }
        if (m_roleArn.empty() || m_tokenFile.empty()) {
            m_roleArn = profile.GetRoleArn();
            m_tokenFile = profile.GetValue("web_identity_token_file");
            if (m_sessionName.empty()) {
                m_sessionName = profile.GetValue("role_session_name");
            }
        }
    }

    if (m_sessionName.empty()) {
        m_sessionName = Aws::Utils::UUID::PseudoRandomUUID();
    }

    if (m_region.empty()) {
        m_region = Aws::Region::US_EAST_1;
    }

    if (!m_tokenFile.empty() && !m_roleArn.empty()) {
        m_initialized = true;
        AWS_LOGSTREAM_INFO(
                SafeSTSWebIdentityTag,
                "Initialized with role ARN: " << m_roleArn << ", token file: " << m_tokenFile
                                              << ", session: " << m_sessionName
        );
    } else {
        AWS_LOGSTREAM_DEBUG(
                SafeSTSWebIdentityTag,
                "Not initialized: " << AWS_WEB_IDENTITY_TOKEN_FILE << " or " << AWS_ROLE_ARN << " not set."
        );
    }
}

Aws::Auth::AWSCredentials SafeSTSWebIdentityCredentialsProvider::GetAWSCredentials() {
    if (!m_initialized) {
        return {};
    }
    std::lock_guard<std::mutex> lock(m_credsMutex);
    RefreshIfExpired();
    return m_credentials;
}

static const int STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD = 5 * 1000;

bool SafeSTSWebIdentityCredentialsProvider::ExpiresSoon() const {
    return (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count() <
           STS_CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD;
}

void SafeSTSWebIdentityCredentialsProvider::RefreshIfExpired() {
    if (!m_credentials.IsEmpty() && !ExpiresSoon()) {
        return;
    }

    // Read the web identity token from file (re-read each time as IRSA rotates tokens).
    std::ifstream tokenStream(m_tokenFile.c_str());
    if (!tokenStream.is_open()) {
        AWS_LOGSTREAM_ERROR(
                SafeSTSWebIdentityTag, "Can't open token file: " << m_tokenFile
        );
        return; // Keep old cached credentials
    }
    Aws::String token((std::istreambuf_iterator<char>(tokenStream)), std::istreambuf_iterator<char>());
    tokenStream.close();

    if (token.empty()) {
        AWS_LOGSTREAM_WARN(SafeSTSWebIdentityTag, "Web identity token file is empty: " << m_tokenFile);
        return; // Keep old cached credentials
    }

    // Build the STS request.
    Aws::STS::Model::AssumeRoleWithWebIdentityRequest request;
    request.SetRoleArn(m_roleArn);
    request.SetRoleSessionName(m_sessionName);
    request.SetWebIdentityToken(token);

    // Use anonymous credentials for the STS client — the web identity token is the authentication.
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.region = m_region;

    auto stsClient = Aws::MakeUnique<Aws::STS::STSClient>(
            SafeSTSWebIdentityTag, Aws::Auth::AWSCredentials(), config
    );

    auto outcome = stsClient->AssumeRoleWithWebIdentity(request);
    if (!outcome.IsSuccess()) {
        AWS_LOGSTREAM_WARN(
                SafeSTSWebIdentityTag,
                "STS AssumeRoleWithWebIdentity failed: " << outcome.GetError().GetMessage()
                                                         << " — returning empty credentials to allow chain fallthrough."
        );
        m_credentials = {};
        return;
    }

    const auto& stsCreds = outcome.GetResult().GetCredentials();
    m_credentials = Aws::Auth::AWSCredentials(
            stsCreds.GetAccessKeyId(),
            stsCreds.GetSecretAccessKey(),
            stsCreds.GetSessionToken(),
            stsCreds.GetExpiration()
    );
    AWS_LOGSTREAM_INFO(
            SafeSTSWebIdentityTag,
            "Successfully assumed role " << m_roleArn << ", credentials expire at "
                                         << stsCreds.GetExpiration().ToGmtString(Aws::Utils::DateFormat::ISO_8601)
    );
}

MyAWSCredentialsProviderChain::MyAWSCredentialsProviderChain() : Aws::Auth::AWSCredentialsProviderChain() {
    AddProvider(Aws::MakeShared<EnvironmentAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProfileConfigFileAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
    AddProvider(Aws::MakeShared<ProcessCredentialsProvider>(DefaultCredentialsProviderChainTag));

    // Add the safe STS web identity provider (bypasses CRT-based implementation).
    // The provider checks env vars internally and returns empty credentials if not configured,
    // allowing the chain to continue to the next provider.
    AddProvider(Aws::MakeShared<SafeSTSWebIdentityCredentialsProvider>(DefaultCredentialsProviderChainTag));

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
