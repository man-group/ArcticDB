/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/sts/STSClient.h>

#include <memory>
#include <mutex>

namespace arcticdb::storage::s3 {

// Custom STS Web Identity provider that calls AssumeRoleWithWebIdentity directly,
// bypassing the CRT-based STSAssumeRoleWebIdentityCredentialsProvider which has
// caching/threading bugs (aws-sdk-cpp PR #3505, issues #3531, #3558, #3562).
// On STS failure, returns empty credentials so the chain continues to the next provider.
class SafeSTSWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider {
  public:
    SafeSTSWebIdentityCredentialsProvider();
    Aws::Auth::AWSCredentials GetAWSCredentials() override;

  private:
    void RefreshIfExpired();
    bool ExpiresSoon() const;

    Aws::String m_roleArn;
    Aws::String m_tokenFile;
    Aws::String m_sessionName;
    Aws::String m_region;
    Aws::Auth::AWSCredentials m_credentials;
    Aws::Client::ClientConfiguration m_stsConfig;
    std::unique_ptr<Aws::STS::STSClient> m_stsClient;
    std::mutex m_credsMutex;
    bool m_initialized;
};

class MyAWSCredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain {
  public:
    MyAWSCredentialsProviderChain();
};

} // namespace arcticdb::storage::s3
