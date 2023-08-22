/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/ostream.h>
#include <arcticdb/entity/variant_key.hpp>

#define MAKE_GTEST_FMT(our_type, fstr) namespace testing::internal { \
template<> inline void PrintTo(const our_type&val, ::std::ostream* os) { fmt::print(*os, fstr, val); } \
}

// For the most common types, format them by default:
MAKE_GTEST_FMT(arcticdb::entity::RefKey, "{}")
MAKE_GTEST_FMT(arcticdb::entity::AtomKeyImpl, "{}")
MAKE_GTEST_FMT(arcticdb::entity::VariantKey, "VariantKey({})")
MAKE_GTEST_FMT(arcticdb::entity::VariantId, "VariantId({})")

// FUTURE (C++20): with capabilities, we can write a generic PrintTo that covers all fmt::format-able types that is
// not ambiguous with the built-in

// Macro to skip tests when running on Windows
#ifdef _WIN32
#define SKIP_WIN(REASON) GTEST_SKIP() << "Skipping test on Windows, reason: " << '[' << #REASON << ']'
#else
#define SKIP_WIN(REASON) (void)0
#endif

// Macro to skip tests when running on Mac
#ifdef __APPLE__
#define SKIP_MAC(REASON) GTEST_SKIP() << "Skipping test on Mac, reason: " << '[' << #REASON << ']'
#else
#define SKIP_MAC(REASON) (void)0
#endif
