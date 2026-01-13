/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/type_conversion.hpp>

TEST(Comparisons, Simple) {
    using namespace arcticdb;

    // double/T and T/double always become double/double
    using double_double = Comparable<double, double>;
    static_assert(std::is_same_v<double_double::left_type, double>);
    static_assert(std::is_same_v<double_double::right_type, double>);
    using double_float = Comparable<double, float>;
    static_assert(std::is_same_v<double_float::left_type, double>);
    static_assert(std::is_same_v<double_float::right_type, double>);
    using float_double = Comparable<float, double>;
    static_assert(std::is_same_v<float_double::left_type, double>);
    static_assert(std::is_same_v<float_double::right_type, double>);
    using double_uint64_t = Comparable<double, uint64_t>;
    static_assert(std::is_same_v<double_uint64_t::left_type, double>);
    static_assert(std::is_same_v<double_uint64_t::right_type, double>);
    using uint64_t_double = Comparable<uint64_t, double>;
    static_assert(std::is_same_v<uint64_t_double::left_type, double>);
    static_assert(std::is_same_v<uint64_t_double::right_type, double>);
    using double_int = Comparable<double, int>;
    static_assert(std::is_same_v<double_int::left_type, double>);
    static_assert(std::is_same_v<double_int::right_type, double>);
    using int_double = Comparable<int, double>;
    static_assert(std::is_same_v<int_double::left_type, double>);
    static_assert(std::is_same_v<int_double::right_type, double>);

    // float/T and T/float always become float/float for T != double
    using float_float = Comparable<float, float>;
    static_assert(std::is_same_v<float_float::left_type, float>);
    static_assert(std::is_same_v<float_float::right_type, float>);
    using float_uint64_t = Comparable<float, uint64_t>;
    static_assert(std::is_same_v<float_uint64_t::left_type, float>);
    static_assert(std::is_same_v<float_uint64_t::right_type, float>);
    using uint64_t_float = Comparable<uint64_t, float>;
    static_assert(std::is_same_v<uint64_t_float::left_type, float>);
    static_assert(std::is_same_v<uint64_t_float::right_type, float>);
    using float_int = Comparable<float, int>;
    static_assert(std::is_same_v<float_int::left_type, float>);
    static_assert(std::is_same_v<float_int::right_type, float>);
    using int_float = Comparable<int, float>;
    static_assert(std::is_same_v<int_float::left_type, float>);
    static_assert(std::is_same_v<int_float::right_type, float>);

    // Anything with a uint64 and a signed type gets promoted to the special comparison
    // type (int64_t, uint64_t) or (uint64_t, int64_t)
    using left_uint64_comparable_signed = Comparable<uint64_t, int32_t>;
    static_assert(std::is_same_v<left_uint64_comparable_signed::left_type, uint64_t>);
    static_assert(std::is_same_v<left_uint64_comparable_signed::right_type, int64_t>);

    // Otherwise the non-uint64_t unsigned type stays the same
    using left_uint64_comparable_unsigned = Comparable<uint64_t, uint32_t>;
    static_assert(std::is_same_v<left_uint64_comparable_unsigned::left_type, uint64_t>);
    static_assert(std::is_same_v<left_uint64_comparable_unsigned::right_type, uint32_t>);

    using right_uint64_t_comparable_signed = Comparable<int32_t, uint64_t>;
    static_assert(std::is_same_v<right_uint64_t_comparable_signed::left_type, int64_t>);
    static_assert(std::is_same_v<right_uint64_t_comparable_signed::right_type, uint64_t>);

    using right_uint64_t_comparable_unsigned = Comparable<uint32_t, uint64_t>;
    static_assert(std::is_same_v<right_uint64_t_comparable_unsigned::left_type, uint32_t>);
    static_assert(std::is_same_v<right_uint64_t_comparable_unsigned::right_type, uint64_t>);

    // Smaller types where is_signed(left) and is_signed(right) are the same are promoted to the
    // common type
    using both_unsigned_common_type_1 = Comparable<uint32_t, uint16_t>;
    static_assert(std::is_same_v<both_unsigned_common_type_1::left_type, uint32_t>);
    static_assert(std::is_same_v<both_unsigned_common_type_1::right_type, uint32_t>);

    using both_signed_common_type_1 = Comparable<int32_t, int16_t>;
    static_assert(std::is_same_v<both_signed_common_type_1::left_type, int32_t>);
    static_assert(std::is_same_v<both_signed_common_type_1::right_type, int32_t>);

    // Otherwise the unsigned is promoted to the next signed type up that can hold it
    using mixed_signed_unsigned_1 = Comparable<uint32_t, int16_t>;
    static_assert(std::is_same_v<mixed_signed_unsigned_1::left_type, int64_t>);
    static_assert(std::is_same_v<mixed_signed_unsigned_1::right_type, int16_t>);

    using mixed_signed_unsigned_2 = Comparable<uint16_t, int32_t>;
    static_assert(std::is_same_v<mixed_signed_unsigned_2::left_type, int32_t>);
    static_assert(std::is_same_v<mixed_signed_unsigned_2::right_type, int32_t>);
}