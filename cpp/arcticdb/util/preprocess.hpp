/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#ifndef _WIN32
#define ARCTICDB_UNUSED __attribute__((unused))
#define ARCTICDB_UNREACHABLE __builtin_unreachable();

#define ARCTICDB_VISIBILITY_HIDDEN __attribute__((visibility("hidden")))
#define ARCTICDB_VISIBILITY_DEFAULT __attribute__((visibility("default")))

#define ARCTICDB_LIKELY(condition) __builtin_expect(condition, 1)
#define ARCTICDB_UNLIKELY(condition) __builtin_expect(condition, 0)

#else
#define ARCTICDB_UNUSED [[maybe_unused]]
#define ARCTICDB_UNREACHABLE __assume(0);
#define ARCTICDB_VISIBILITY_HIDDEN
#define ARCTICDB_VISIBILITY_DEFAULT

#define ARCTICDB_LIKELY
#define ARCTICDB_UNLIKELY
#endif
