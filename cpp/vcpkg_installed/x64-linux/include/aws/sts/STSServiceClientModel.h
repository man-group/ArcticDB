/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

/* Generic header includes */
#include <aws/sts/STSErrors.h>
#include <aws/core/client/GenericClientConfiguration.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/client/AsyncCallerContext.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/sts/STSEndpointProvider.h>
#include <future>
#include <functional>
/* End of generic header includes */

/* Service model headers required in STSClient header */
#include <aws/sts/model/AssumeRoleResult.h>
#include <aws/sts/model/AssumeRoleWithSAMLResult.h>
#include <aws/sts/model/AssumeRoleWithWebIdentityResult.h>
#include <aws/sts/model/DecodeAuthorizationMessageResult.h>
#include <aws/sts/model/GetAccessKeyInfoResult.h>
#include <aws/sts/model/GetCallerIdentityResult.h>
#include <aws/sts/model/GetFederationTokenResult.h>
#include <aws/sts/model/GetSessionTokenResult.h>
#include <aws/sts/model/GetCallerIdentityRequest.h>
#include <aws/sts/model/GetSessionTokenRequest.h>
/* End of service model headers required in STSClient header */

namespace Aws
{
  namespace Http
  {
    class HttpClient;
    class HttpClientFactory;
  } // namespace Http

  namespace Utils
  {
    template< typename R, typename E> class Outcome;

    namespace Threading
    {
      class Executor;
    } // namespace Threading
  } // namespace Utils

  namespace Auth
  {
    class AWSCredentials;
    class AWSCredentialsProvider;
  } // namespace Auth

  namespace Client
  {
    class RetryStrategy;
  } // namespace Client

  namespace STS
  {
    using STSClientConfiguration = Aws::Client::GenericClientConfiguration;
    using STSEndpointProviderBase = Aws::STS::Endpoint::STSEndpointProviderBase;
    using STSEndpointProvider = Aws::STS::Endpoint::STSEndpointProvider;

    namespace Model
    {
      /* Service model forward declarations required in STSClient header */
      class AssumeRoleRequest;
      class AssumeRoleWithSAMLRequest;
      class AssumeRoleWithWebIdentityRequest;
      class DecodeAuthorizationMessageRequest;
      class GetAccessKeyInfoRequest;
      class GetCallerIdentityRequest;
      class GetFederationTokenRequest;
      class GetSessionTokenRequest;
      /* End of service model forward declarations required in STSClient header */

      /* Service model Outcome class definitions */
      typedef Aws::Utils::Outcome<AssumeRoleResult, STSError> AssumeRoleOutcome;
      typedef Aws::Utils::Outcome<AssumeRoleWithSAMLResult, STSError> AssumeRoleWithSAMLOutcome;
      typedef Aws::Utils::Outcome<AssumeRoleWithWebIdentityResult, STSError> AssumeRoleWithWebIdentityOutcome;
      typedef Aws::Utils::Outcome<DecodeAuthorizationMessageResult, STSError> DecodeAuthorizationMessageOutcome;
      typedef Aws::Utils::Outcome<GetAccessKeyInfoResult, STSError> GetAccessKeyInfoOutcome;
      typedef Aws::Utils::Outcome<GetCallerIdentityResult, STSError> GetCallerIdentityOutcome;
      typedef Aws::Utils::Outcome<GetFederationTokenResult, STSError> GetFederationTokenOutcome;
      typedef Aws::Utils::Outcome<GetSessionTokenResult, STSError> GetSessionTokenOutcome;
      /* End of service model Outcome class definitions */

      /* Service model Outcome callable definitions */
      typedef std::future<AssumeRoleOutcome> AssumeRoleOutcomeCallable;
      typedef std::future<AssumeRoleWithSAMLOutcome> AssumeRoleWithSAMLOutcomeCallable;
      typedef std::future<AssumeRoleWithWebIdentityOutcome> AssumeRoleWithWebIdentityOutcomeCallable;
      typedef std::future<DecodeAuthorizationMessageOutcome> DecodeAuthorizationMessageOutcomeCallable;
      typedef std::future<GetAccessKeyInfoOutcome> GetAccessKeyInfoOutcomeCallable;
      typedef std::future<GetCallerIdentityOutcome> GetCallerIdentityOutcomeCallable;
      typedef std::future<GetFederationTokenOutcome> GetFederationTokenOutcomeCallable;
      typedef std::future<GetSessionTokenOutcome> GetSessionTokenOutcomeCallable;
      /* End of service model Outcome callable definitions */
    } // namespace Model

    class STSClient;

    /* Service model async handlers definitions */
    typedef std::function<void(const STSClient*, const Model::AssumeRoleRequest&, const Model::AssumeRoleOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > AssumeRoleResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::AssumeRoleWithSAMLRequest&, const Model::AssumeRoleWithSAMLOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > AssumeRoleWithSAMLResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::AssumeRoleWithWebIdentityRequest&, const Model::AssumeRoleWithWebIdentityOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > AssumeRoleWithWebIdentityResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::DecodeAuthorizationMessageRequest&, const Model::DecodeAuthorizationMessageOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DecodeAuthorizationMessageResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::GetAccessKeyInfoRequest&, const Model::GetAccessKeyInfoOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetAccessKeyInfoResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::GetCallerIdentityRequest&, const Model::GetCallerIdentityOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetCallerIdentityResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::GetFederationTokenRequest&, const Model::GetFederationTokenOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetFederationTokenResponseReceivedHandler;
    typedef std::function<void(const STSClient*, const Model::GetSessionTokenRequest&, const Model::GetSessionTokenOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetSessionTokenResponseReceivedHandler;
    /* End of service model async handlers definitions */
  } // namespace STS
} // namespace Aws
