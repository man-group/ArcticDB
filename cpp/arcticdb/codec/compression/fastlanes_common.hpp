/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <utility>
#include <limits>


#if defined(__clang__)
#define ALWAYS_INLINE __attribute__((always_inline))
    #define VECTOR_HINT __attribute__((vectorcall))
    #define RESTRICT __restrict__
    #define ASSUME_ALIGNED(ptr, alignment) __builtin_assume_aligned(ptr, alignment)
    #define VECTORIZE_LOOP _Pragma("clang loop vectorize(enable) interleave(enable)")
    #define ALIGNED_ACCESS _Pragma("clang loop vectorize_predicate(enable)")
    #define HOT_FUNCTION __attribute__((hot))
    #define ALIGN_HINT(x) alignas(x)
    #define EXPECT(expr, val) __builtin_expect(expr, val)
#elif defined(__GNUC__)
    #define ALWAYS_INLINE __attribute__((always_inline))
    #define VECTOR_HINT __attribute__((vector_size(32)))
    #define RESTRICT __restrict
    #define ASSUME_ALIGNED(ptr, alignment) __builtin_assume_aligned(ptr, alignment)
    #define VECTORIZE_LOOP _Pragma("GCC ivdep")
    #define ALIGNED_ACCESS
    #define HOT_FUNCTION __attribute__((hot))
    #define ALIGN_HINT(x) alignas(x)
    #define EXPECT(expr, val) __builtin_expect(expr, val)
#elif defined(_MSC_VER)
#define ALWAYS_INLINE __forceinline
    #define VECTOR_HINT __vectorcall
    #define RESTRICT __restrict
    #define ASSUME_ALIGNED(ptr, alignment) ptr
    #define VECTORIZE_LOOP
    #define ALIGNED_ACCESS
    #define HOT_FUNCTION
    #define ALIGN_HINT(x) __declspec(align(x))
    #define EXPECT(expr, val) (expr)
#else
#define ALWAYS_INLINE
#define VECTOR_HINT
#define RESTRICT
#define ASSUME_ALIGNED(ptr, alignment) ptr
#define VECTORIZE_LOOP
#define ALIGNED_ACCESS
#define HOT_FUNCTION
#define ALIGN_HINT(x)
#define EXPECT(expr, val) (expr)
#endif

namespace arcticdb {
namespace fastlanes {

constexpr std::size_t FastLanesWidth = 1024;

template<class T, T... inds, class F>
constexpr void loop(std::integer_sequence<T, inds...>, F &&f) {
    (f(std::integral_constant<T, inds>{}), ...);
}
}

template<class T, T count, class F>
constexpr void loop(F &&f) {
    fastlanes::loop(std::make_integer_sequence<T, count>{}, std::forward<F>(f));
}

template<typename T>
constexpr size_t type_bits() {
    return sizeof(T) * std::numeric_limits<uint8_t>::digits;
}

template<typename T>
struct Helper {
    static constexpr size_t num_bits = type_bits<T>();
    static constexpr size_t register_width = fastlanes::FastLanesWidth;
    static constexpr size_t num_lanes = register_width / num_bits;
};

static_assert(Helper<uint64_t>::num_lanes == 16);
static_assert(Helper<uint8_t>::num_lanes == 128);
static_assert(Helper<uint16_t>::num_bits == 16);

constexpr std::array<size_t, 8> FL_ORDER = { 0, 4, 2, 6, 1, 5, 3, 7 };

constexpr size_t transposed_index(size_t index) {
    auto lane = index % 16;
    auto order = (index / 16) % 8;
    auto row = index / 128;

    return (lane * 64) + (FL_ORDER[order] * 8) + row;
}

constexpr size_t index(size_t row, size_t lane) {
    const auto o = row / 8;
    const auto s = row % 8;
    return (FL_ORDER[o] * 16) + (s * 128) + lane;
}

static_assert(transposed_index(1) == 64);
static_assert(transposed_index(57) == 624);
static_assert(transposed_index(1022) == 959);

static_assert(index(1, 0) == 128);
static_assert(transposed_index(57) == 624);
static_assert(transposed_index(1022) == 959);
} // namespace arcticdb