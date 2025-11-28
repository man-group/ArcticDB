/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {

VariantData transform_to_placeholder(VariantData data) {
    // There is a further optimization we can do here which is to return FullResult
    // when the number of set bits is the same as the processing units row-count,
    // however that would need to be maintained by the processing unit and
    // modified with each transformation. It would be worth it in the scenario
    // where a true result is very common, but is a premature change to make at this point.
    return util::variant_match(
            data,
            [](const util::BitSet& bitset) -> VariantData {
                if (bitset.count() == 0) {
                    return VariantData{EmptyResult{}};
                } else {
                    return VariantData{bitset};
                }
            },
            [](auto v) -> VariantData { return VariantData{v}; }
    );
}

// Unary and binary boolean operations work on bitsets, full results and empty results
// A column is also a valid input, provided it is of type bool
// This conversion could be done within visit_binary_boolean with an explosion of the number of visitations to handle
// To avoid that, this function:
//  - returns util::Bitset, FullResult, and EmptyResult as provided
//  - throws if the input VariantData is a Value, a ValueSet, or a non-bool column
//  - returns a util::BitSet if the input is a bool column
VariantData transform_to_bitset(const VariantData& data) {
    return std::visit(
            util::overload{
                    [&](const std::shared_ptr<Value>&) -> VariantData {
                        util::raise_rte("Value inputs cannot be input to boolean operations");
                    },
                    [&](const std::shared_ptr<ValueSet>&) -> VariantData {
                        util::raise_rte("ValueSet inputs cannot be input to boolean operations");
                    },
                    [&](const ColumnWithStrings& column_with_strings) -> VariantData {
                        util::BitSet output_bitset;
                        details::visit_type(
                                column_with_strings.column_->type().data_type(),
                                [&column_with_strings, &output_bitset](auto col_tag) {
                                    using type_info = ScalarTypeInfo<decltype(col_tag)>;
                                    if constexpr (is_bool_type(type_info::data_type)) {
                                        arcticdb::transform<typename type_info::TDT>(
                                                *column_with_strings.column_,
                                                output_bitset,
                                                false,
                                                [](auto input_value) -> bool { return input_value; }
                                        );
                                    } else {
                                        util::raise_rte(
                                                "Cannot convert column '{}' of type {} to a bitset",
                                                column_with_strings.column_name_,
                                                get_user_friendly_type_string(column_with_strings.column_->type())
                                        );
                                    }
                                }
                        );
                        return output_bitset;
                    },
                    [](const auto& d) -> VariantData {
                        // util::BitSet, FullResult, or EmptyResult
                        return d;
                    }
            },
            data
    );
}

} // namespace arcticdb
