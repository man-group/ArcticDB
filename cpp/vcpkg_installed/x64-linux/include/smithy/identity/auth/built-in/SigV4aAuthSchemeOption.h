/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#pragma once

#include <smithy/identity/auth/AuthSchemeOption.h>

namespace smithy {
    struct SigV4aAuthSchemeOption
    {
        static AuthSchemeOption sigV4aAuthSchemeOption;
    };

    AuthSchemeOption SigV4aAuthSchemeOption::sigV4aAuthSchemeOption = AuthSchemeOption("aws.auth#sigv4a");
}