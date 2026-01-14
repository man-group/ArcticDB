/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/format_date.hpp>
#include <arcticdb/util/constants.hpp>
#include <limits>

TEST(FormatDate, ZeroTs) { ASSERT_EQ("1970-01-01 00:00:00.000000000", arcticdb::util::format_timestamp(0)); }

TEST(FormatDate, PrependZero) {
    ASSERT_EQ("2025-06-09 08:06:09.000000000", arcticdb::util::format_timestamp(1749456369000000000));
    ASSERT_EQ("2025-06-09 08:06:09.000000001", arcticdb::util::format_timestamp(1749456369000000000 + 1));
    ASSERT_EQ("2025-06-09 00:00:00.000000000", arcticdb::util::format_timestamp(1749427200000000000));
}

TEST(FormatDate, PreEpoch) {
    ASSERT_EQ("1969-12-31 23:59:59.999999999", arcticdb::util::format_timestamp(-1));
    ASSERT_EQ("1969-12-31 23:59:59.000000000", arcticdb::util::format_timestamp(-1'000'000'000));
}

TEST(FormatDate, April2821) {
    ASSERT_EQ("2021-04-28 16:11:35.213000000", arcticdb::util::format_timestamp(1619626295213000000));
}

TEST(FormatDate, LargestInt64ns) {
    ASSERT_EQ("2262-04-11 23:47:16.854775807", arcticdb::util::format_timestamp(std::numeric_limits<int64_t>::max()));
}

TEST(FormatDate, NaT) { ASSERT_EQ("NaT", arcticdb::util::format_timestamp(arcticdb::NaT)); }