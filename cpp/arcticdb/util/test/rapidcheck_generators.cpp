/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck_generators.hpp>
/* Skipped until arrow dependency in vcpkg can be re-added
 * Broken by https://github.com/microsoft/vcpkg/pull/44653
 * We don't seem to be the only people affected https://github.com/microsoft/vcpkg/issues/48496
#include <arrow/util/decimal.h>

rc::Gen<arrow::Decimal128> rc::Arbitrary<arrow::Decimal128>::arbitrary() {
    return rc::gen::map<std::array<uint64_t, 2>>([](std::array<uint64_t, 2> data) {
        static_assert(sizeof(arrow::Decimal128) == sizeof(data));
        arrow::Decimal128 d;
        std::memcpy(static_cast<void*>(&d), static_cast<void*>(data.data()), sizeof(d));
        return d;
    });
}

rc::Gen<std::string> gen_arrow_decimal128_string() {
    return rc::gen::mapcat(rc::gen::arbitrary<arrow::Decimal128>(), [](arrow::Decimal128 d) {
        const int digit_count = d.IsNegative() ? static_cast<int>(d.ToString(0).length() - 1)
                                               : static_cast<int>(d.ToString(0).length());
        const int allowed_scale_abs = arrow::Decimal128::kMaxScale - digit_count;
        return rc::gen::map(rc::gen::inRange<int>(-allowed_scale_abs, allowed_scale_abs), [d](int scale) {
            return d.ToString(scale);
        });
    });
}
 */