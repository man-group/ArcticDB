/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/lossless_cast.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace {
using arcticdb::util::lossless_cast;
using arcticdb::util::losslessly_convertible_to;

enum class UnsignedEnum : uint32_t { max = std::numeric_limits<uint32_t>::max() };
enum class SignedEnum : int32_t { min = std::numeric_limits<int32_t>::min() };
enum class WideEnum : uint64_t { max = std::numeric_limits<uint64_t>::max() };

template<typename To, typename From>
concept lossless_castable = requires(From from) { lossless_cast<To>(from); };
} // namespace

TEST(LosslessCast, AcceptsConversionsThatKeepEveryValue) {
    static_assert(losslessly_convertible_to<uint32_t, uint64_t>);
    static_assert(losslessly_convertible_to<uint32_t, int64_t>);
    static_assert(losslessly_convertible_to<int32_t, int64_t>);
    static_assert(losslessly_convertible_to<uint64_t, uint64_t>);
    static_assert(losslessly_convertible_to<UnsignedEnum, size_t>);
    static_assert(losslessly_convertible_to<SignedEnum, int64_t>);
}

TEST(LosslessCast, RejectsConversionsThatCanLoseValues) {
    static_assert(!losslessly_convertible_to<uint64_t, uint32_t>);
    static_assert(!losslessly_convertible_to<int32_t, uint64_t>);
    static_assert(!losslessly_convertible_to<uint32_t, int32_t>);
    static_assert(!losslessly_convertible_to<uint64_t, int64_t>);
    static_assert(!losslessly_convertible_to<SignedEnum, size_t>);
    static_assert(!losslessly_convertible_to<WideEnum, uint32_t>);
    static_assert(!losslessly_convertible_to<float, double>);
    static_assert(!losslessly_convertible_to<uint32_t, UnsignedEnum>);
}

TEST(LosslessCast, CastsOnlyAcceptedConversions) {
    static_assert(lossless_castable<size_t, UnsignedEnum>);
    static_assert(lossless_castable<int64_t, uint32_t>);
    static_assert(!lossless_castable<size_t, SignedEnum>);
    static_assert(!lossless_castable<uint32_t, uint64_t>);
    static_assert(std::is_same_v<decltype(lossless_cast<size_t>(UnsignedEnum::max)), size_t>);
    static_assert(lossless_cast<size_t>(UnsignedEnum::max) == std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(lossless_cast<int64_t>(uint32_t{4'000'000'000}), int64_t{4'000'000'000});
}
