/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

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
