#pragma once

#if defined(__clang__)
// Inlining for clang
#define ARCTICDB_LAMBDA_INLINE_MID __attribute__((always_inline))
#elif defined(__GNUC__)
// Inlining for recent versions of gcc
#define ARCTICDB_LAMBDA_INLINE_PRE [[gnu::always_inline]]
#elif defined(_MSC_VER) && _MSC_VER >= 1927 && __cplusplus >= 202002L
// MSVC 16.7 and newer specific syntax, requires C++20 language standard to be enabled
#define ARCTICDB_LAMBDA_INLINE_POST [[msvc::forceinline]]
#endif

#ifndef ARCTICDB_LAMBDA_INLINE_PRE
#define ARCTICDB_LAMBDA_INLINE_PRE
#endif
#ifndef ARCTICDB_LAMBDA_INLINE_MID
#define ARCTICDB_LAMBDA_INLINE_MID
#endif
#ifndef ARCTICDB_LAMBDA_INLINE_POST
#define ARCTICDB_LAMBDA_INLINE_POST
#endif

// Example usage of inlining a lambda looks like:
// [&] ARCTICDB_LAMBDA_INLINE_PRE (int arg) ARCTICDB_LAMBDA_INLINE_MID noexcept ARCTICDB_LAMBDA_INLINE_POST -> int {
//     return arg+5;
// }