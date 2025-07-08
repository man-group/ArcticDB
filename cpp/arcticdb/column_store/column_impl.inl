/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column.hpp>
namespace arcticdb {
template<typename T>
std::optional<T> Column::scalar_at(position_t row) const {
    auto physical_row = get_physical_row(row);
    if(!physical_row)
        return std::nullopt;
    return *data_.buffer().ptr_cast<T>(bytes_offset(*physical_row), sizeof(T));
}

template<typename T>
std::vector<T> Column::clone_scalars_to_vector() const {
    auto values = std::vector<T>();
    values.reserve(row_count());
    const auto& buffer = data_.buffer();
    for (auto i=0u; i<row_count(); ++i){
        values.push_back(*buffer.ptr_cast<T>(i*item_size(), sizeof(T)));
    }
    return values;
}
} // namespace arcticdb