/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/log/log.hpp>

#ifdef ARCTICDB_TRACE_ON
#define ARCTICDB_STR_H(x) #x
#define ARCTICDB_STR_HELPER(x) ARCTICDB_STR_H(x)
#define ARCTICDB_TRACE(logger, ...)                                                                                                          \
    logger.trace("[ " __FILE__ ":" ARCTICDB_STR_HELPER(__LINE__) " ]"                                                                       \
                                                                " " __VA_ARGS__)
#else
#define ARCTICDB_TRACE(logger, ...) (void)0
#endif
