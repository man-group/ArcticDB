/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/aggregation_utils.hpp>

namespace arcticdb {

void add_data_type_impl(entity::DataType data_type, std::optional<entity::DataType>& current_data_type) {
    if (current_data_type.has_value()) {
        const auto common_type = promotable_type(make_scalar_type(*current_data_type), make_scalar_type(data_type));
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                common_type.has_value(),
                "Cannot perform aggregation on column, incompatible types present: {} and {}",
                entity::TypeDescriptor(*current_data_type, 0),
                entity::TypeDescriptor(data_type, 0)
        );
        current_data_type = common_type->data_type();
    } else {
        current_data_type = data_type;
    }
}

} // namespace arcticdb
