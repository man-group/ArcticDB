/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <stdexcept>
#include <cstdlib>

#ifdef GENERATE_RANDOM_EXCEPTIONS
#define ARCTICDB_DEBUG_THROW(percentage)                                                                               \
    do {                                                                                                               \
        if (static_cast<double>(std::rand()) / RAND_MAX * 100 < percentage) {                                          \
            throw std::runtime_error("Exception intentionally thrown");                                                \
        }                                                                                                              \
    } while (0);
#else
#define ARCTICDB_DEBUG_THROW(percentage)
#endif
