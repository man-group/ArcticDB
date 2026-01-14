/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/Poly.h>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct IGroupingAggregatorData {
    template<class Base>
    struct Interface : Base {
        void add_data_type(DataType data_type) { folly::poly_call<0>(*this, data_type); }

        DataType get_output_data_type() { return folly::poly_call<1>(*this); };

        void aggregate(const ColumnWithStrings& input_column, const std::vector<size_t>& groups, size_t unique_values) {
            folly::poly_call<2>(*this, input_column, groups, unique_values);
        }
        [[nodiscard]] SegmentInMemory finalize(
                const ColumnName& output_column_name, bool dynamic_schema, size_t unique_values
        ) {
            return folly::poly_call<3>(*this, output_column_name, dynamic_schema, unique_values);
        }

        /// @returns std::nullopt if the aggregation's default value is the same as the default value of the underlying
        ///   type. If the aggregation type has a special default value return it encoded in an Value object. This value
        ///   will later be used by the NullValueReducer to fill in sparse data.
        [[nodiscard]] std::optional<Value> get_default_value() { return folly::poly_call<4>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<
            &T::add_data_type, &T::get_output_data_type, &T::aggregate, &T::finalize, &T::get_default_value>;
};

using GroupingAggregatorData = folly::Poly<IGroupingAggregatorData>;

struct IGroupingAggregator {
    template<class Base>
    struct Interface : Base {
        [[nodiscard]] ColumnName get_input_column_name() const { return folly::poly_call<0>(*this); };
        [[nodiscard]] ColumnName get_output_column_name() const { return folly::poly_call<1>(*this); };
        [[nodiscard]] GroupingAggregatorData get_aggregator_data() const { return folly::poly_call<2>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::get_input_column_name, &T::get_output_column_name, &T::get_aggregator_data>;
};

using GroupingAggregator = folly::Poly<IGroupingAggregator>;

struct IColumnStatsAggregatorData {
    template<class Base>
    struct Interface : Base {
        void aggregate(const ColumnWithStrings& input_column) { folly::poly_call<0>(*this, input_column); }
        SegmentInMemory finalize(const std::vector<ColumnName>& output_column_names) const {
            return folly::poly_call<1>(*this, output_column_names);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::aggregate, &T::finalize>;
};

using ColumnStatsAggregatorData = folly::Poly<IColumnStatsAggregatorData>;

struct IColumnStatsAggregator {
    template<class Base>
    struct Interface : Base {
        [[nodiscard]] ColumnName get_input_column_name() const { return folly::poly_call<0>(*this); };
        [[nodiscard]] std::vector<ColumnName> get_output_column_names() const { return folly::poly_call<1>(*this); };
        [[nodiscard]] ColumnStatsAggregatorData get_aggregator_data() const { return folly::poly_call<2>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::get_input_column_name, &T::get_output_column_names, &T::get_aggregator_data>;
};

using ColumnStatsAggregator = folly::Poly<IColumnStatsAggregator>;

} // namespace arcticdb
