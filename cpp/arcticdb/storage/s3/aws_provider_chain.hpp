/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <aws/core/auth/AWSCredentialsProviderChain.h>

namespace arcticdb::storage::s3 {

    class MyAWSCredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain {
    public:
        MyAWSCredentialsProviderChain();
    };

}