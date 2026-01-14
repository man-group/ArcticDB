/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <array>
#include <string>
#include <string_view>
#include <cstdint>
#include <arcticdb/util/constructors.hpp>

namespace arcticdb {

namespace util {
/**
 * The decimal class is binary compatible with pyarrow's layout for 128 bit decimals. The format keeps
 * the number as 128 bit integer in two's complement. The scale is not part of the format.
 * https://github.com/apache/arrow/blob/45918a90a6ca1cf3fd67c256a7d6a240249e555a/cpp/src/arrow/util/decimal.h
 * https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
 * Little endian order is used for the words and the bits inside each word.
 */
class Decimal {
  public:
    Decimal();
    /**
     * Construct decimal from a string
     * @param number String representation of a decimal number
     * @note Zeros after the decimal point in \p number matter.
     * @see Decimal::operator==()
     */
    explicit Decimal(std::string_view number);
    ARCTICDB_MOVE_COPY_DEFAULT(Decimal);

    /**
     * Convert decimal to string
     * @param scale Positive scale means the numbers after the decimal point.
     *  Negative scale acts as multiplying by 10^(abs(scale))
     * @example
     *  Decimal("123").to_string(1) = "12.3"
     *  Decimal("123").to_string(-1) = "1230"
     *  Decimal("123").to_string(0) = "123"
     */
    [[nodiscard]] std::string to_string(int scale) const;
    [[nodiscard]] bool is_negative() const;
    [[nodiscard]] Decimal operator-() const;
    /**
     * Compare two decimals. Since the format does not keep decimal point position, the comparison is byte-wise.
     * @example
     *  Decimal("1") == Decimal("1.0") -> false
     *  Decimal("100") == Decimal("1.00") -> true
     *  Decimal("1.000E6") == Decimal("1000000") -> true
     *  Decimal("1.000E-6") == Decimal("1000") -> true
     */
    [[nodiscard]] bool operator==(const Decimal&) const;
    void negate();

    constexpr static int max_scale = 38;

  private:
    void push_chunk(std::string_view);

    enum { LEAST_SIGNIFICANT_WORD_INDEX = 0, MOST_SIGNIFICANT_WORD_INDEX = 1 };
    std::array<uint64_t, 2> data_;
};
} // namespace util
} // namespace arcticdb