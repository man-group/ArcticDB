/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/type_handler.hpp>

namespace arcticdb::entity {

Dimension as_dim_checked(uint8_t d) {
    if (d > static_cast<uint8_t>(Dimension::Dim2)) {
        throw std::invalid_argument(fmt::format("Invalid dimension %d", static_cast<uint32_t>(d)));
    }
    return static_cast<Dimension>(d);
}

std::string_view datatype_to_str(const DataType dt) {
    switch (dt) {
#define TO_STR(ARG) case DataType::ARG: return std::string_view(#ARG);
        TO_STR(UINT8)
        TO_STR(UINT16)
        TO_STR(UINT32)
        TO_STR(UINT64)
        TO_STR(INT8)
        TO_STR(INT16)
        TO_STR(INT32)
        TO_STR(INT64)
        TO_STR(FLOAT32)
        TO_STR(FLOAT64)
        TO_STR(BOOL8)
        TO_STR(NANOSECONDS_UTC64)
        TO_STR(ASCII_FIXED64)
        TO_STR(ASCII_DYNAMIC64)
        TO_STR(UTF_FIXED64)
        TO_STR(UTF_DYNAMIC64)
        TO_STR(EMPTYVAL)
        TO_STR(BOOL_OBJECT8)
        TO_STR(UTF_DYNAMIC32)
#undef TO_STR
        default:return std::string_view("UNKNOWN");
    }
}

std::size_t internal_data_type_size(const TypeDescriptor& td) {
    return get_type_size(td.data_type());
}

std::size_t external_data_type_size(const TypeDescriptor& td, OutputFormat output_format) {
    auto handler = TypeHandlerRegistry::instance()->get_handler(output_format, td);
    return handler ? handler->type_size() : internal_data_type_size(td);
}

std::size_t data_type_size(const TypeDescriptor& td, OutputFormat output_format, DataTypeMode mode) {
    return mode == DataTypeMode::EXTERNAL ? external_data_type_size(td, output_format) : internal_data_type_size(td);
}

} // namespace arcticdb
