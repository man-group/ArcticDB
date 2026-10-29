/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/preconditions.hpp>

using ErrorCode = arcticdb::ErrorCode;

TEST(ErrorCode, DoesThrow) {
    ASSERT_THROW(arcticdb::schema::raise<ErrorCode::E_INCOMPATIBLE_OBJECTS>("msg {}", 1), arcticdb::SchemaException);
    ASSERT_THROW(
            arcticdb::schema::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(false, "msg {}", 2), arcticdb::SchemaException
    );
}

// FUTURE: Find a way to test the static_assert in detail::Raise?
