/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/Poly.h>

namespace arcticdb {

struct Fraction {
    double numerator_{0.0};
    uint64_t denominator_{0};

    double to_double() const
    {
        return denominator_ == 0 ? std::numeric_limits<double>::quiet_NaN()
                                 : numerator_ / static_cast<double>(denominator_);
    }
};

struct CountAndTotals {
    std::vector<Fraction> fractions_;
};

struct IAggregation {
    template<class Base>
    struct Interface : Base {
        void aggregate(const std::optional<ColumnWithStrings>& input_column,
            const std::vector<size_t>& groups,
            size_t unique_values)
        {
            folly::poly_call<0>(*this, input_column, groups, unique_values);
        }

        std::optional<DataType> finalize(SegmentInMemory& seg, bool dynamic_schema, size_t unique_values)
        {
            return folly::poly_call<1>(*this, seg, dynamic_schema, unique_values);
        }

        [[nodiscard]] ColumnName get_input_column_name() const
        {
            return folly::poly_call<2>(*this);
        };

        [[nodiscard]] ColumnName get_output_column_name() const
        {
            return folly::poly_call<3>(*this);
        };

        void set_data_type(DataType data_type_)
        {
            folly::poly_call<4>(*this, data_type_);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::aggregate,
        &T::finalize,
        &T::get_input_column_name,
        &T::get_output_column_name,
        &T::set_data_type>;
};

using Aggregation = folly::Poly<IAggregation>;

struct IAggregationFactory {
    template<class Base>
    struct Interface : Base {

        [[nodiscard]] Aggregation construct() const
        {
            return folly::poly_call<0>(*this);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::construct>;
};

using AggregationFactory = folly::Poly<IAggregationFactory>;

} //namespace arcticdb