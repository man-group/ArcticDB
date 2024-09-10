/**
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#pragma once

#include <smithy/identity/auth/AuthSchemeOption.h>

namespace smithy {
    struct SigV4AuthSchemeOption
    {
        static AuthSchemeOption sigV4AuthSchemeOption;
    };

    AuthSchemeOption SigV4AuthSchemeOption::sigV4AuthSchemeOption = AuthSchemeOption("aws.auth#sigv4");
}