/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cmath>

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>

#ifdef _WIN32
#define SKIP_WIN(REASON) RC_SUCCEED(#REASON)
#else
#define SKIP_WIN(REASON) (void)0
#endif

/* Filtering isin/isnotin rely on integers of different signs and widths hashing to the same value if they are
 * semantically equivalent. This is not guaranteed by the standard, but experimentally:
 * std::hash<T>{}(t) == static_cast<size_t>(t) for t >= 0
 *                      (std::numeric_limits<size_t>::max() - static_cast<size_t>(std::abs(t))) + 1 for t < 0
 * where T is an integral type, and where the second line is equivalent to the first, but signals the intent
 * more clearly.
 * This means that when std::numeric_limits<size_t>::max() == std::numeric_limits<uint64_t>::max()
 * there are semantically different uint64_t/int*_t values that hash to the same thing e.g.
 * std::hash<uint64_t>{}(std::numeric_limits<uint64_t>::max()) == std::hash<T>{int8_t}(-1)
 * When comparing uint64_t's to signed types, the signed type is always promoted to int64_t.
 * The IsIn and IsNotIn operators then have special logic to check the most significant bit of the uint64_t
 * and the sign of the int64_t to avoid these clashes. */

RC_GTEST_PROP(SetMemberShip, uint8_t, (uint8_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    RC_ASSERT(std::hash<uint8_t>{}(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, uint16_t, (uint16_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    RC_ASSERT(std::hash<uint16_t>{}(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, uint32_t, (uint32_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    RC_ASSERT(std::hash<uint32_t>{}(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, uint64_t, (uint64_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    RC_ASSERT(std::hash<uint64_t>{}(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, int8_t, (int8_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    auto hasher = std::hash<int8_t>{};
    if (i < 0)
        RC_ASSERT(hasher(i) == (std::numeric_limits<size_t>::max() - static_cast<size_t>(std::abs(i))) + 1);
    else
        RC_ASSERT(hasher(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, int16_t, (int16_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    auto hasher = std::hash<int16_t>{};
    if (i < 0)
        RC_ASSERT(hasher(i) == (std::numeric_limits<size_t>::max() - static_cast<size_t>(std::abs(i))) + 1);
    else
        RC_ASSERT(hasher(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, int32_t, (int32_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    auto hasher = std::hash<int32_t>{};
    if (i < 0)
        RC_ASSERT(hasher(i) == (std::numeric_limits<size_t>::max() - static_cast<size_t>(std::abs(i))) + 1);
    else
        RC_ASSERT(hasher(i) == static_cast<size_t>(i));
}

RC_GTEST_PROP(SetMemberShip, int64_t, (int64_t i)) {
    SKIP_WIN("IsIn IsNotIn Operators not supported");
    auto hasher = std::hash<int64_t>{};
    if (i < 0)
        RC_ASSERT(hasher(i) == (std::numeric_limits<size_t>::max() - static_cast<size_t>(std::abs(i))) + 1);
    else
        RC_ASSERT(hasher(i) == static_cast<size_t>(i));
}
