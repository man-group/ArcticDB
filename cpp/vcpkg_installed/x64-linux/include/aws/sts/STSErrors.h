/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/sts/STS_EXPORTS.h>

namespace Aws
{
namespace STS
{
enum class STSErrors
{
  //From Core//
  //////////////////////////////////////////////////////////////////////////////////////////
  INCOMPLETE_SIGNATURE = 0,
  INTERNAL_FAILURE = 1,
  INVALID_ACTION = 2,
  INVALID_CLIENT_TOKEN_ID = 3,
  INVALID_PARAMETER_COMBINATION = 4,
  INVALID_QUERY_PARAMETER = 5,
  INVALID_PARAMETER_VALUE = 6,
  MISSING_ACTION = 7, // SDK should never allow
  MISSING_AUTHENTICATION_TOKEN = 8, // SDK should never allow
  MISSING_PARAMETER = 9, // SDK should never allow
  OPT_IN_REQUIRED = 10,
  REQUEST_EXPIRED = 11,
  SERVICE_UNAVAILABLE = 12,
  THROTTLING = 13,
  VALIDATION = 14,
  ACCESS_DENIED = 15,
  RESOURCE_NOT_FOUND = 16,
  UNRECOGNIZED_CLIENT = 17,
  MALFORMED_QUERY_STRING = 18,
  SLOW_DOWN = 19,
  REQUEST_TIME_TOO_SKEWED = 20,
  INVALID_SIGNATURE = 21,
  SIGNATURE_DOES_NOT_MATCH = 22,
  INVALID_ACCESS_KEY_ID = 23,
  REQUEST_TIMEOUT = 24,
  NETWORK_CONNECTION = 99,

  UNKNOWN = 100,
  ///////////////////////////////////////////////////////////////////////////////////////////

  EXPIRED_TOKEN= static_cast<int>(Aws::Client::CoreErrors::SERVICE_EXTENSION_START_RANGE) + 1,
  INVALID_AUTHORIZATION_MESSAGE,
  INVALID_IDENTITY_TOKEN,
  I_D_P_COMMUNICATION_ERROR,
  I_D_P_REJECTED_CLAIM,
  MALFORMED_POLICY_DOCUMENT,
  PACKED_POLICY_TOO_LARGE,
  REGION_DISABLED
};

class AWS_STS_API STSError : public Aws::Client::AWSError<STSErrors>
{
public:
  STSError() {}
  STSError(const Aws::Client::AWSError<Aws::Client::CoreErrors>& rhs) : Aws::Client::AWSError<STSErrors>(rhs) {}
  STSError(Aws::Client::AWSError<Aws::Client::CoreErrors>&& rhs) : Aws::Client::AWSError<STSErrors>(rhs) {}
  STSError(const Aws::Client::AWSError<STSErrors>& rhs) : Aws::Client::AWSError<STSErrors>(rhs) {}
  STSError(Aws::Client::AWSError<STSErrors>&& rhs) : Aws::Client::AWSError<STSErrors>(rhs) {}

  template <typename T>
  T GetModeledError();
};

namespace STSErrorMapper
{
  AWS_STS_API Aws::Client::AWSError<Aws::Client::CoreErrors> GetErrorForName(const char* errorName);
}

} // namespace STS
} // namespace Aws
