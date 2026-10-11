/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstddef>
#include <ostream>

#include <arcticdb/util/hash.hpp>  // TODO: ref_key needs hash to be included. Did I break that?
#include <arcticdb/entity/ref_key.hpp>
#include <arcticdb/entity/variant_key.hpp>

namespace arcticdb {
class SegmentInMemory;

namespace pipelines {
struct RowRange;
struct ColRange;
}
}

#define MAKE_GTEST_FMT_DECL(our_type, fstr)                                                                                 \
    namespace testing::internal {                                                                                      \
    void PrintTo(const our_type& val, ::std::ostream* os); \
    }

#define MAKE_GTEST_FMT(our_type, fstr)                                                                                 \
    namespace testing::internal {                                                                                      \
    void PrintTo(const our_type& val, ::std::ostream* os) { fmt::print(*os, fstr, val); } \
    }

MAKE_GTEST_FMT_DECL(arcticdb::entity::RefKey, "{}")
MAKE_GTEST_FMT_DECL(arcticdb::entity::AtomKeyImpl, "{}")
MAKE_GTEST_FMT_DECL(arcticdb::entity::VariantKey, "VariantKey({})")
MAKE_GTEST_FMT_DECL(arcticdb::VariantId, "VariantId({})")
MAKE_GTEST_FMT_DECL(arcticdb::SegmentInMemory, "{}")
MAKE_GTEST_FMT_DECL(arcticdb::pipelines::RowRange, "{}")
MAKE_GTEST_FMT_DECL(arcticdb::pipelines::ColRange, "{}")

// FUTURE (C++20): with capabilities, we can write a generic PrintTo that covers all fmt::format-able types that is
// not ambiguous with the built-in
