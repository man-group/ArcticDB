/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <bitmagic/bm.h>
#include <bitmagic/bmserial.h>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/bitset.hpp>

#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/version_store_api.hpp>

using namespace arcticdb;
constexpr auto UNKNOWN = SortedValue::UNKNOWN;
constexpr auto ASCENDING = SortedValue::ASCENDING;
constexpr auto DESCENDING = SortedValue::DESCENDING;
constexpr auto UNSORTED = SortedValue::UNSORTED;

TEST(Sorting, ExistingUnknown) {
    //               existing_frame, input_frame
    ASSERT_EQ(deduce_sorted(UNKNOWN, UNKNOWN), UNKNOWN);
    ASSERT_EQ(deduce_sorted(UNKNOWN, ASCENDING), UNKNOWN);
    ASSERT_EQ(deduce_sorted(UNKNOWN, DESCENDING), UNKNOWN);
    ASSERT_EQ(deduce_sorted(UNKNOWN, UNSORTED), UNSORTED);
}

TEST(Sorting, ExistingAscending) {
    //                 existing_frame, input_frame
    ASSERT_EQ(deduce_sorted(ASCENDING, UNKNOWN), UNKNOWN);
    ASSERT_EQ(deduce_sorted(ASCENDING, ASCENDING), ASCENDING);
    ASSERT_EQ(deduce_sorted(ASCENDING, DESCENDING), UNSORTED);
    ASSERT_EQ(deduce_sorted(ASCENDING, UNSORTED), UNSORTED);
}

TEST(Sorting, ExistingDescending) {
    //                  existing_frame, input_frame
    ASSERT_EQ(deduce_sorted(DESCENDING, UNKNOWN), UNKNOWN);
    ASSERT_EQ(deduce_sorted(DESCENDING, ASCENDING), UNSORTED);
    ASSERT_EQ(deduce_sorted(DESCENDING, DESCENDING), DESCENDING);
    ASSERT_EQ(deduce_sorted(DESCENDING, UNSORTED), UNSORTED);
}

TEST(Sorting, ExistingUnsorted) {
    //                existing_frame, input_frame
    ASSERT_EQ(deduce_sorted(UNSORTED, UNKNOWN), UNSORTED);
    ASSERT_EQ(deduce_sorted(UNSORTED, ASCENDING), UNSORTED);
    ASSERT_EQ(deduce_sorted(UNSORTED, DESCENDING), UNSORTED);
    ASSERT_EQ(deduce_sorted(UNSORTED, UNSORTED), UNSORTED);
}