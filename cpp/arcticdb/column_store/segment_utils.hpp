/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <folly/container/F14Set.h>
#include <arcticdb/util/third_party/emilib_set.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {
emilib::HashSet<StringPool::offset_t> unique_values_for_string_column(const Column &column) {
    auto column_data = column.data();
    return column_data.type().visit_tag([&](auto type_desc_tag) -> emilib::HashSet<StringPool::offset_t> {
        using TDT = decltype(type_desc_tag);
        using DTT = typename TDT::DataTypeTag;
        if constexpr(is_sequence_type(DTT::data_type)) {
            using RawType = typename TDT::DataTypeTag::raw_type;
            emilib::HashSet<StringPool::offset_t> output;
            // Guessing that unique values is a third of the column length
            static auto map_reserve_ratio = ConfigsMap::instance()->get_int("UniqueColumns.AllocationRatio", 3);
            output.reserve(column.row_count() / map_reserve_ratio);

            while (auto block = column_data.next<TDT>()) {
                auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                const auto row_count = block->row_count();
                for (auto i = 0u; i < row_count; ++i) {
                    output.insert(*ptr++);
                }
            }
            return output;
        } else {
            util::raise_rte("Column {} is not a string type column");
        }
    });
}

}