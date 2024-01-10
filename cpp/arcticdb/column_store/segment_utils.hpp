/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/column.hpp>
#ifdef ARCTICDB_USING_CONDA
    #include <robin_hood.h>
#else
    #include <arcticdb/util/third_party/robin_hood.hpp>
#endif
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {
robin_hood::unordered_set<entity::position_t> unique_values_for_string_column(const Column &column) {
    auto column_data = column.data();
    return column_data.type().visit_tag([&](auto type_desc_tag) -> robin_hood::unordered_set<entity::position_t> {
        using TDT = decltype(type_desc_tag);
        using DTT = typename TDT::DataTypeTag;
        if constexpr(is_sequence_type(DTT::data_type)) {
            using RawType = typename TDT::DataTypeTag::raw_type;
            robin_hood::unordered_set<entity::position_t> output;
            // Guessing that unique values is a third of the column length
            static auto map_reserve_ratio = ConfigsMap::instance()->get_int("UniqueColumns.AllocationRatio", 3);
            output.reserve(column.row_count() / map_reserve_ratio);

            while (auto block = column_data.next<TDT>()) {
                auto ptr = reinterpret_cast<const RawType *>(block->data());
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
