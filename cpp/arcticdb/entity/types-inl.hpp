/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_TYPES_H_
#error "This should only be included by types.hpp"
#endif

#include <fmt/format.h>
#include <variant>

namespace arcticdb::entity {

namespace details {

template<class DimType, class Callable>
constexpr auto visit_dim(DataType dt, Callable &&c) {
    switch (dt) {
#define DT_CASE(__T__) case DataType::__T__: \
        return c(TypeDescriptorTag<DataTypeTag<DataType::__T__>, DimType>());
        DT_CASE(UINT8)
        DT_CASE(UINT16)
        DT_CASE(UINT32)
        DT_CASE(UINT64)
        DT_CASE(INT8)
        DT_CASE(INT16)
        DT_CASE(INT32)
        DT_CASE(INT64)
        DT_CASE(FLOAT32)
        DT_CASE(FLOAT64)
        DT_CASE(BOOL8)
        DT_CASE(NANOSECONDS_UTC64)
        DT_CASE(ASCII_FIXED64)
        DT_CASE(ASCII_DYNAMIC64)
        DT_CASE(UTF_FIXED64)
        DT_CASE(UTF_DYNAMIC64)
        DT_CASE(UTF_DYNAMIC32)
        DT_CASE(EMPTYVAL)
        DT_CASE(BOOL_OBJECT8)
#undef DT_CASE
        default: util::raise_rte("Invalid dtype {}:{} - '{}' in visit dim", int(slice_value_type(dt)), int(slice_bit_size(dt)), datatype_to_str(dt));
    }
}

template<class Callable>
auto visit_type(DataType dt, Callable &&c) {
    switch (dt) {
#define DT_CASE(__T__) case DataType::__T__: \
    return c(DataTypeTag<DataType::__T__>());
        DT_CASE(UINT8)
        DT_CASE(UINT16)
        DT_CASE(UINT32)
        DT_CASE(UINT64)
        DT_CASE(INT8)
        DT_CASE(INT16)
        DT_CASE(INT32)
        DT_CASE(INT64)
        DT_CASE(FLOAT32)
        DT_CASE(FLOAT64)
        DT_CASE(BOOL8)
        DT_CASE(NANOSECONDS_UTC64)
        DT_CASE(ASCII_FIXED64)
        DT_CASE(ASCII_DYNAMIC64)
        DT_CASE(UTF_FIXED64)
        DT_CASE(UTF_DYNAMIC64)
        DT_CASE(UTF_DYNAMIC32)
        DT_CASE(EMPTYVAL)
        DT_CASE(BOOL_OBJECT8)
#undef DT_CASE
    default: util::raise_rte("Invalid dtype {}:{} '{}' in visit type", int(slice_value_type(dt)), int(slice_bit_size(dt)), datatype_to_str(dt));
    }
}

} // namespace details

template<class Callable>
constexpr auto TypeDescriptor::visit_tag(Callable &&callable) const {
    switch (dimension_) {
        case Dimension::Dim0: return details::visit_dim<DimensionTag<Dimension::Dim0>>(data_type_, callable);
        case Dimension::Dim1: return details::visit_dim<DimensionTag<Dimension::Dim1>>(data_type_, callable);
        case Dimension::Dim2: return details::visit_dim<DimensionTag<Dimension::Dim2>>(data_type_, callable);
        default: throw std::invalid_argument(fmt::format("Invalid dimension %d", static_cast<uint32_t>(dimension_)));
    }
}

constexpr TypeDescriptor null_type_descriptor() {
    return {DataType(ValueType::UNKNOWN_VALUE_TYPE), Dimension::Dim0};
}

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::entity::DataType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(const arcticdb::entity::DataType dt, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "{}", datatype_to_str(dt));
    }
};

template<>
struct formatter<arcticdb::entity::Dimension> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(const arcticdb::entity::Dimension dim, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<uint32_t >(dim));
    }
};

template<>
struct formatter<arcticdb::entity::TypeDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(const arcticdb::entity::TypeDescriptor &td, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "TD<type={}, dim={}>", td.data_type_, td.dimension_);
    }
};

template<>
struct formatter<arcticdb::StreamId> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(const arcticdb::StreamId &tsid, FormatContext &ctx) const {
        return std::visit([&ctx](auto &&val) {
            return fmt::format_to(ctx.out(), "{}", val);
        }, tsid);
    }
};
}
