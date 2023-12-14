// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/**
 * @file
 * @brief Provide assert macros to use with pre-conditions.
 *
 * @attention These macros are deprecated for public use - they should NOT be used by any callers
 * outside of the SDK.
 *
 * @remark Asserts are turned ON when `NDEBUG` is NOT defined (for Debug build). For Release build,
 * `std::abort()` is directly called if the condition is false, without calling assert().
 *
 */

#pragma once

#include "azure/core/platform.hpp"

#include <cstdlib>
#include <string>

#if defined(NDEBUG)

/*
 * NDEBUG = defined = Build is on Release
 * Define AZURE_ASSERT to call abort directly on exp == false
 */

#define AZURE_ASSERT(exp) \
  do \
  { \
    if (!(exp)) \
    { \
      std::abort(); \
    } \
  } while (0)

#define AZURE_ASSERT_MSG(exp, msg) AZURE_ASSERT(exp)

#else

/*
 * NDEBUG = NOT defined = Build is on Debug
 * Define AZURE_ASSERT to call assert to provide better debug experience.
 */

#include <cassert>

/** @brief Azure specific assert macro.*/
#define AZURE_ASSERT(exp) assert((exp))
/** @brief Azure specific assert macro with message.*/
#define AZURE_ASSERT_MSG(exp, msg) assert(((void)msg, (exp)))

#endif

namespace Azure { namespace Core { namespace _internal {
  [[noreturn]] void AzureNoReturnPath(std::string const& msg);
}}} // namespace Azure::Core::_internal

/** @brief Assert that the exp parameter is always false. */
#define AZURE_ASSERT_FALSE(exp) AZURE_ASSERT(!(exp))
/** @brief Indicate that the code cannot be reached. */
#define AZURE_UNREACHABLE_CODE() ::Azure::Core::_internal::AzureNoReturnPath("unreachable code!")
/** @brief Indicate that the function is not implemented. */
#define AZURE_NOT_IMPLEMENTED() ::Azure::Core::_internal::AzureNoReturnPath("not implemented code!")
