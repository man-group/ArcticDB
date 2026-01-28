/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/segment_utils.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>

namespace arcticdb {

ankerl::unordered_dense::set<entity::position_t> unique_values_for_string_column(const Column& column) {
    ankerl::unordered_dense::set<entity::position_t> output_set;
    // Guessing that unique values is a third of the column length
    // TODO would be useful to have actual unique count here from stats
    static auto map_reserve_ratio = ConfigsMap::instance()->get_int("UniqueColumns.AllocationRatio", 3);
    output_set.reserve(column.row_count() / map_reserve_ratio);

    details::visit_type(column.type().data_type(), [&](auto col_desc_tag) {
        using type_info = ScalarTypeInfo<decltype(col_desc_tag)>;
        if constexpr (is_sequence_type(type_info::data_type)) {
            arcticdb::for_each<typename type_info::TDT>(column, [&output_set](auto value) {
                output_set.emplace(value);
            });
        } else {
            util::raise_rte("Column {} is not a string type column");
        }
    });
    return output_set;
}

} // namespace arcticdb
