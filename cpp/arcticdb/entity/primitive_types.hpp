/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#ifdef _WIN32
// `ssize_t` is defined in `sys/types.h` but it is not ISO C (it simply is POSIX), hence its is not defined natively by
// MSVC. See: https://learn.microsoft.com/en-us/windows/win32/winprog/windows-data-types
#include <BaseTsd.h>
using ssize_t = SSIZE_T;
#endif

namespace arcticdb {

using NumericId = int64_t;
using UnsignedId = uint64_t;

namespace entity {

using VersionId = uint64_t;
using SignedVersionId = int64_t;
using GenerationId = VersionId;
using timestamp = int64_t;
using shape_t = int64_t;
using stride_t = int64_t;
using position_t = int64_t;

} // namespace entity

} // namespace arcticdb
