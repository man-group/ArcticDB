/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

namespace Aws {
    namespace Http {
        /**
         * Enum to represent version of the http protocol to use
         */
        enum class Version {
            HTTP_VERSION_NONE,
            HTTP_VERSION_1_0,
            HTTP_VERSION_1_1,
            HTTP_VERSION_2_0,
            HTTP_VERSION_2TLS,
            HTTP_VERSION_2_PRIOR_KNOWLEDGE,
            HTTP_VERSION_3,
            HTTP_VERSION_3ONLY,
        };
    }
}
