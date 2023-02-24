/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#define stringify(x) #x
#define static_to_string(x) stringify(x)
#define source_loc __FILE__ ":" static_to_string(__LINE__)
#define to_id(x) ([]() constexpr { return x; })

#ifndef _WIN32
#define ARCTICDB_UNUSED __attribute__((unused))
#define ARCTICDB_UNREACHABLE  __builtin_unreachable();

#define ARCTICDB_VISIBILITY_HIDDEN __attribute__ ((visibility("hidden")))
#define ARCTICDB_VISIBILITY_DEFAULT  __attribute__ ((visibility ("default")))

#else
#define ARCTICDB_UNUSED
#define ARCTICDB_UNREACHABLE
#define ARCTICDB_VISIBILITY_HIDDEN
#define ARCTICDB_VISIBILITY_DEFAULT
#endif
