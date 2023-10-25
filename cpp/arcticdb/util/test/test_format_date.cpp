/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/format_date.hpp>

TEST(FormatDate, ZeroTs) {
    using namespace arcticdb;
    ASSERT_EQ("1970-01-01 00:00:00.0", util::format_timestamp(0));
}

TEST(FormatDate, April2821) {
    using namespace arcticdb;
#ifdef __linux__
    ASSERT_EQ("2021-04-28 16:11:35.213", util::format_timestamp(1619626295213000000));
#else
    ASSERT_EQ("2021-04-28 16:11:35.0", util::format_timestamp(1619626295213000000));
#endif
}