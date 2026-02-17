#pragma once

#if defined(__clang__)
// Inlining for clang
#define ARCTICDB_LAMBDA_INLINE(...) (__VA_ARGS__) __attribute__((always_inline))
#define ARCTICDB_LAMBDA_INLINE_1(m1, ...) (__VA_ARGS__) __attribute__((always_inline)) m1
#define ARCTICDB_LAMBDA_INLINE_2(m1, m2, ...) (__VA_ARGS__) __attribute__((always_inline)) m1 m2
#define ARCTICDB_FLATTEN __attribute__((flatten))
#elif defined(__GNUC__)
// Inlining for recent versions of gcc
#define ARCTICDB_LAMBDA_INLINE(...) [[gnu::always_inline]] (__VA_ARGS__)
#define ARCTICDB_LAMBDA_INLINE_1(m1, ...) [[gnu::always_inline]] (__VA_ARGS__) m1
#define ARCTICDB_LAMBDA_INLINE_2(m1, m2, ...) [[gnu::always_inline]] (__VA_ARGS__) m1 m2
#define ARCTICDB_FLATTEN [[gnu::flatten]]
#elif defined(_MSC_VER) && _MSC_VER >= 1927 && __cplusplus >= 202002L
// MSVC 16.7 and newer specific syntax, requires C++20 language standard to be enabled
#define ARCTICDB_LAMBDA_INLINE(...) (__VA_ARGS__) [[msvc::forceinline]]
#define ARCTICDB_LAMBDA_INLINE_1(m1, ...) (__VA_ARGS__) m1 [[msvc::forceinline]]
#define ARCTICDB_LAMBDA_INLINE_2(m1, m2, ...) (__VA_ARGS__) m1 m2 [[msvc::forceinline]]
// MSVC does not support a flatten equivalent; this is a no-op.
#define ARCTICDB_FLATTEN
#else
#define ARCTICDB_LAMBDA_INLINE(...) (__VA_ARGS__)
#define ARCTICDB_LAMBDA_INLINE_1(m1, ...) (__VA_ARGS__) m1
#define ARCTICDB_LAMBDA_INLINE_2(m1, m2, ...) (__VA_ARGS__) m1 m2
#define ARCTICDB_FLATTEN
#endif

// Example usages of inlining a lambda looks like:
// [&] ARCTICDB_LAMBDA_INLINE(int arg1, int arg2) -> int {
//     return arg+5;
// }
// [&] ARCTICDB_LAMBDA_INLINE_1(noexcept, int arg1, int arg2) -> int {
//     return arg+5;
// }
// [&] ARCTICDB_LAMBDA_INLINE_2(static, noexcept, int arg1, int arg2) -> int {
//     return arg+5;
// }
