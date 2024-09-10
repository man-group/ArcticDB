/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sts/STS_EXPORTS.h>
#include <aws/core/client/GenericClientConfiguration.h>
#include <aws/core/endpoint/DefaultEndpointProvider.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>

#include <aws/sts/STSEndpointRules.h>


namespace Aws
{
namespace STS
{
namespace Endpoint
{
using EndpointParameters = Aws::Endpoint::EndpointParameters;
using Aws::Endpoint::EndpointProviderBase;
using Aws::Endpoint::DefaultEndpointProvider;

using STSClientContextParameters = Aws::Endpoint::ClientContextParameters;

using STSClientConfiguration = Aws::Client::GenericClientConfiguration;
using STSBuiltInParameters = Aws::Endpoint::BuiltInParameters;

/**
 * The type for the STS Client Endpoint Provider.
 * Inherit from this Base class / "Interface" should you want to provide a custom endpoint provider.
 * The SDK must use service-specific type for each service per specification.
 */
using STSEndpointProviderBase =
    EndpointProviderBase<STSClientConfiguration, STSBuiltInParameters, STSClientContextParameters>;

using STSDefaultEpProviderBase =
    DefaultEndpointProvider<STSClientConfiguration, STSBuiltInParameters, STSClientContextParameters>;

/**
 * Default endpoint provider used for this service
 */
class AWS_STS_API STSEndpointProvider : public STSDefaultEpProviderBase
{
public:
    using STSResolveEndpointOutcome = Aws::Endpoint::ResolveEndpointOutcome;

    STSEndpointProvider()
      : STSDefaultEpProviderBase(Aws::STS::STSEndpointRules::GetRulesBlob(), Aws::STS::STSEndpointRules::RulesBlobSize)
    {}

    ~STSEndpointProvider()
    {
    }
};
} // namespace Endpoint
} // namespace STS
} // namespace Aws
