/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/log/log.hpp>

#ifdef ARCTICDB_TRACE_ON
#define ARCTICDB_STR_H(x) #x
#define ARCTICDB_STR_HELPER(x) ARCTICDB_STR_H(x)
#define ARCTICDB_TRACE(logger, ...)                                                                                    \
    logger.trace("[ " __FILE__ ":" ARCTICDB_STR_HELPER(__LINE__) " ]"                                                  \
                                                                 " " __VA_ARGS__)
#else
#define ARCTICDB_TRACE(logger, ...) (void)0
#endif
