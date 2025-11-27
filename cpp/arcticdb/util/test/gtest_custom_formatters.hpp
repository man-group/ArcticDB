/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/ostream.h>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/ref_key.hpp>

#define MAKE_GTEST_FMT(our_type, fstr)                                                                                 \
    namespace testing::internal {                                                                                      \
    inline void PrintTo(const our_type& val, ::std::ostream* os) { fmt::print(*os, fstr, val); }                       \
    }

MAKE_GTEST_FMT(arcticdb::entity::RefKey, "{}")
MAKE_GTEST_FMT(arcticdb::entity::AtomKeyImpl, "{}")
MAKE_GTEST_FMT(arcticdb::entity::VariantKey, "VariantKey({})")
MAKE_GTEST_FMT(arcticdb::VariantId, "VariantId({})")
MAKE_GTEST_FMT(arcticdb::SegmentInMemory, "{}")
MAKE_GTEST_FMT(arcticdb::pipelines::RowRange, "{}")
MAKE_GTEST_FMT(arcticdb::pipelines::ColRange, "{}")

// FUTURE (C++20): with capabilities, we can write a generic PrintTo that covers all fmt::format-able types that is
// not ambiguous with the built-in