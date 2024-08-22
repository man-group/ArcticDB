/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/operation_types.hpp>

TEST(ArithmeticTypePromotion, Abs) {
    using namespace arcticdb;
    // Floating point and unsigned integer types should promote to themselves
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<float,     AbsOperator>::type, float>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<double,    AbsOperator>::type, double>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint8_t,   AbsOperator>::type, uint8_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint16_t,  AbsOperator>::type, uint16_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint32_t,  AbsOperator>::type, uint32_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint64_t,  AbsOperator>::type, uint64_t>);
    // Signed integer types should promote to a signed type of double the width, capped at int64_t
    // This is because std::abs(std::numeric_limits<intn_t>::min()) == std::numeric_limits<intn_t>::max() + 1
    // for n in {8, 16, 32, 64}. We accept that there will be overflow for int64_t for a single value.
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int8_t,    AbsOperator>::type, int16_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int16_t,   AbsOperator>::type, int32_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int32_t,   AbsOperator>::type, int64_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int64_t,   AbsOperator>::type, int64_t>);
}

TEST(ArithmeticTypePromotion, Neg) {
    using namespace arcticdb;
    // Floating point types should promote to themselves
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<float,     NegOperator>::type, float>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<double,    NegOperator>::type, double>);
    // Integer types should promote to a signed type of double the width, capped at int64_t. For signed integers,
    // this is because -std::numeric_limits<intn_t>::min() == std::numeric_limits<intn_t>::max() + 1
    // for n in {8, 16, 32, 64}. We accept that there will be overflow for int64_t for a single value, and for uint64_t
    // values greater than std::numeric_limits<int64_t>::max()
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int8_t,    NegOperator>::type, int16_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int16_t,   NegOperator>::type, int32_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int32_t,   NegOperator>::type, int64_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<int64_t,   NegOperator>::type, int64_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint8_t,   NegOperator>::type, int16_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint16_t,  NegOperator>::type, int32_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint32_t,  NegOperator>::type, int64_t>);
    static_assert(std::is_same_v<unary_arithmetic_promoted_type<uint64_t,  NegOperator>::type, int64_t>);
}

TEST(ArithmeticTypePromotion, Plus) {
    using namespace arcticdb;
    // Floating point types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  float,  PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, float,  PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, double, PlusOperator>::type, double>);
    // Unsigned types should promote to an unsigned type one size larger than the biggest provided, capped at uint64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint8_t,  PlusOperator>::type, uint16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint16_t, PlusOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint32_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint64_t, PlusOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint8_t,  PlusOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint16_t, PlusOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint32_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint64_t, PlusOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint8_t,  PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint16_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint32_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint64_t, PlusOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint8_t,  PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint16_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint32_t, PlusOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint64_t, PlusOperator>::type, uint64_t>);
    // Signed types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int8_t,  PlusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int8_t,  PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int64_t, PlusOperator>::type, int64_t>);
    // Mixed signed and unsigned types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int8_t,  PlusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int8_t,  PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint8_t,  PlusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint8_t,  PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint16_t, PlusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint64_t, PlusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint8_t,  PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint16_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint32_t, PlusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint64_t, PlusOperator>::type, int64_t>);
    // Mixed integral and floating point types should promote to the floating point type
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, float, PlusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint8_t,  PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint16_t, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint32_t, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint64_t, PlusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, float, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, float, PlusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int8_t,  PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int16_t, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int32_t, PlusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int64_t, PlusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, double, PlusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint8_t,  PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint16_t, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint32_t, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint64_t, PlusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, double, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, double, PlusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int8_t,  PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int16_t, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int32_t, PlusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int64_t, PlusOperator>::type, double>);
}

TEST(ArithmeticTypePromotion, Minus) {
    using namespace arcticdb;
    // Floating point types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  float,  MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, float,  MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, double, MinusOperator>::type, double>);
    // Unsigned types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint8_t,  MinusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint8_t,  MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint64_t, MinusOperator>::type, int64_t>);
    // Signed types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int8_t,  MinusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int8_t,  MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int64_t, MinusOperator>::type, int64_t>);
    // Mixed signed and unsigned types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int8_t,  MinusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int8_t,  MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint8_t,  MinusOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint8_t,  MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint16_t, MinusOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint64_t, MinusOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint8_t,  MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint16_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint32_t, MinusOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint64_t, MinusOperator>::type, int64_t>);
    // Mixed integral and floating point types should promote to the floating point type
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, float, MinusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint8_t,  MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint16_t, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint32_t, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint64_t, MinusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, float, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, float, MinusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int8_t,  MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int16_t, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int32_t, MinusOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int64_t, MinusOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, double, MinusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint8_t,  MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint16_t, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint32_t, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint64_t, MinusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, double, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, double, MinusOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int8_t,  MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int16_t, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int32_t, MinusOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int64_t, MinusOperator>::type, double>);
}

TEST(ArithmeticTypePromotion, Times) {
    using namespace arcticdb;
    // Floating point types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  float,  TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, float,  TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, double, TimesOperator>::type, double>);
    // Unsigned types should promote to an unsigned type one size larger than the biggest provided, capped at uint64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint8_t,  TimesOperator>::type, uint16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint16_t, TimesOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint32_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint64_t, TimesOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint8_t,  TimesOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint16_t, TimesOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint32_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint64_t, TimesOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint8_t,  TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint16_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint32_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint64_t, TimesOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint8_t,  TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint16_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint32_t, TimesOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint64_t, TimesOperator>::type, uint64_t>);
    // Signed types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int8_t,  TimesOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int8_t,  TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int64_t, TimesOperator>::type, int64_t>);
    // Mixed signed and unsigned types should promote to a signed type one size larger than the biggest provided, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int8_t,  TimesOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int8_t,  TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint8_t,  TimesOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint8_t,  TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint16_t, TimesOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint64_t, TimesOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint8_t,  TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint16_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint32_t, TimesOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint64_t, TimesOperator>::type, int64_t>);
    // Mixed integral and floating point types should promote to the floating point type
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, float, TimesOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint8_t,  TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint16_t, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint32_t, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint64_t, TimesOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, float, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, float, TimesOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int8_t,  TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int16_t, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int32_t, TimesOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int64_t, TimesOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, double, TimesOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint8_t,  TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint16_t, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint32_t, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint64_t, TimesOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, double, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, double, TimesOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int8_t,  TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int16_t, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int32_t, TimesOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int64_t, TimesOperator>::type, double>);
}

TEST(ArithmeticTypePromotion, Divide) {
    using namespace arcticdb;
    // Floating point types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  float,  DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float,  double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, float,  DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, double, DivideOperator>::type, double>);
    // Unsigned types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint8_t,  DivideOperator>::type, uint8_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint16_t, DivideOperator>::type, uint16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint32_t, DivideOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  uint64_t, DivideOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint8_t,  DivideOperator>::type, uint16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint16_t, DivideOperator>::type, uint16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint32_t, DivideOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, uint64_t, DivideOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint8_t,  DivideOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint16_t, DivideOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint32_t, DivideOperator>::type, uint32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, uint64_t, DivideOperator>::type, uint64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint8_t,  DivideOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint16_t, DivideOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint32_t, DivideOperator>::type, uint64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, uint64_t, DivideOperator>::type, uint64_t>);
    // Signed types should promote to the larger type width
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int8_t,  DivideOperator>::type, int8_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int16_t, DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int32_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int8_t,  DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int16_t, DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int32_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int8_t,  DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int16_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int32_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int8_t,  DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int16_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, int64_t, DivideOperator>::type, int64_t>);
    // Mixed signed and unsigned types should promote to a signed type capable of exactly representing both types, capped at int64_t
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int8_t,  DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int16_t, DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int32_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int8_t,  DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int16_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int32_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int8_t,  DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int16_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int8_t,  DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int16_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, int64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint8_t,  DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint16_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  uint64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint8_t,  DivideOperator>::type, int16_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint16_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, uint64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint8_t,  DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint16_t, DivideOperator>::type, int32_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, uint64_t, DivideOperator>::type, int64_t>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint8_t,  DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint16_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint32_t, DivideOperator>::type, int64_t>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, uint64_t, DivideOperator>::type, int64_t>);
    // Mixed integral and floating point types should promote to the floating point type
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, float, DivideOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint8_t,  DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint16_t, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint32_t, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, uint64_t, DivideOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, float, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, float, DivideOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int8_t,  DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int16_t, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int32_t, DivideOperator>::type, float>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<float, int64_t, DivideOperator>::type, float>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint8_t,  double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint16_t, double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint32_t, double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<uint64_t, double, DivideOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint8_t,  DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint16_t, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint32_t, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, uint64_t, DivideOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<int8_t,  double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int16_t, double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int32_t, double, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<int64_t, double, DivideOperator>::type, double>);

    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int8_t,  DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int16_t, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int32_t, DivideOperator>::type, double>);
    static_assert(std::is_same_v<type_arithmetic_promoted_type<double, int64_t, DivideOperator>::type, double>);
}
