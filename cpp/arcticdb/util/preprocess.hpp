/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#ifndef _WIN32
    #define ARCTICDB_UNUSED __attribute__((unused))
    #define ARCTICDB_UNREACHABLE  __builtin_unreachable();

    #define ARCTICDB_VISIBILITY_HIDDEN __attribute__ ((visibility("hidden")))
    #define ARCTICDB_VISIBILITY_DEFAULT  __attribute__ ((visibility ("default")))

    #define ARCTICDB_LIKELY(condition) __builtin_expect(condition, 1)
    #define ARCTICDB_UNLIKELY(condition) __builtin_expect(condition, 0)

    #if defined (__cplusplus) || defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
    #ifdef __GNUC__
        #define ARCTICDB_FORCE_INLINE inline __attribute__((always_inline))
    #else
        #define ARCTICDB_FORCE_INLINE inline
    #endif

    #define ARCTICDB_PREFETCH(addr) __builtin_prefetch(addr)

    /* result might be undefined when input_num is zero */
    static inline int trailingzeroes(uint64_t input_num) {
    #ifdef __BMI2__
        return _tzcnt_u64(input_num);
    #else
        return __builtin_ctzll(input_num);
    #endif
    }

    /* result might be undefined when input_num is zero */
    static inline int hamming(uint64_t input_num) {
    #ifdef __POPCOUNT__
        return _popcnt64(input_num);
    #else
        return __builtin_popcountll(input_num);
    #endif
    }
#endif

#else
    #include <intrin.h>
    #include <iso646.h>
    #include <cstdint>

    static inline int trailingzeroes(uint64_t n) {
        return _tzcnt_u64(input_num);
    }

    static inline int hamming(uint64_t n) {
    #ifdef _WIN64
        return (int)__popcnt64(n);
    #else
        return (int)(__popcnt((uint32_t)n) + __popcnt((uint32_t)(n >> 32)));
    #endif

    #define ARCTICDB_UNUSED [[maybe_unused]]
    #define ARCTICDB_UNREACHABLE __assume(0);
    #define ARCTICDB_VISIBILITY_HIDDEN
    #define ARCTICDB_VISIBILITY_DEFAULT

    #define ARCTICDB_LIKELY
    #define ARCTICDB_UNLIKELY
    #define ARCTICDB_FORCE_INLINE static __forceinline
    #define ARCTICDB_PREFETCH(addr)
#endif