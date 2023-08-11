/*
 * Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch.hpp>

namespace arcticdb {

VariantData transform_to_placeholder(VariantData data) {
    // There is a further optimization we can do here which is to return FullResult
    // when the number of set bits is the same as the processing units row-count,
    // however that would need to be maintained by the processing unit and
    // modified with each transformation. It would be worth it in the scenario
    // where a true result is very common, but is a premature change to make at this point.
    return util::variant_match(data,
                               [](std::shared_ptr<util::BitSet> bitset) -> VariantData {
                                   if (bitset->count() == 0) {
                                       return VariantData{EmptyResult{}};
                                   } else {
                                       return VariantData{bitset};
                                   }
                               },
                               [](auto v) -> VariantData {
                                   return VariantData{v};
                               }
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
    return std::visit(util::overload{
            [&] (const std::shared_ptr<Value>&) -> VariantData {
                util::raise_rte("Value inputs cannot be input to boolean operations");
            },
            [&] (const std::shared_ptr<ValueSet>&) -> VariantData {
                util::raise_rte("ValueSet inputs cannot be input to boolean operations");
            },
            [&] (const ColumnWithStrings& column_with_strings) -> VariantData {
                auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(column_with_strings.column_->row_count()));
                column_with_strings.column_->type().visit_tag([&column_with_strings, &output] (auto column_desc_tag) {
                    using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
                    using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
                    using ColumnType =  typename ColumnTagType::raw_type;
                    if constexpr (is_bool_type(ColumnTagType::data_type)) {
                        auto column_data = column_with_strings.column_->data();
                        util::BitSet::bulk_insert_iterator inserter(*output);
                        auto pos = 0u;
                        while (auto block = column_data.next<ColumnDescriptorType>()) {
                            auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                            const auto row_count = block.value().row_count();
                            for (auto i = 0u; i < row_count; ++i, ++pos) {
                                if(*ptr++)
                                    inserter = pos;
                            }
                        }
                        inserter.flush();
                    } else {
                        util::raise_rte("Cannot convert column of type {} to a bitset", column_with_strings.column_->type());
                    }
                });
                return output;
            },
            [](const auto& d) -> VariantData {
                // util::BitSet, FullResult, or EmptyResult
                return d;
            }
    }, data);
}


}