/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <aws/core/auth/AWSCredentialsProviderChain.h>

namespace arcticdb::storage::s3 {

class MyAWSCredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain {
  public:
    MyAWSCredentialsProviderChain();
};

} // namespace arcticdb::storage::s3