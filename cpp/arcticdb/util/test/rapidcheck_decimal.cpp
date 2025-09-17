/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/util/test/rapidcheck_generators.hpp>
#include <arcticdb/util/decimal.hpp>
#include <arrow/util/decimal.h>

RC_GTEST_PROP(Decimal, BinaryCompatibleWithArrow, ()) {
    static_assert(sizeof(arrow::Decimal128) == sizeof(arcticdb::util::Decimal));
    const std::string& decimal_string = *gen_arrow_decimal128_string();
    const arrow::Decimal128 arrow_decimal(decimal_string);
    const arcticdb::util::Decimal arctic_decimal(decimal_string);

    RC_ASSERT(arctic_decimal.to_string(0) == arrow_decimal.ToString(0));
    RC_ASSERT(
            std::memcmp(
                    static_cast<const void*>(&arctic_decimal),
                    static_cast<const void*>(&arrow_decimal),
                    sizeof(arrow_decimal)
            ) == 0
    );
}