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
#include <fmt/ranges.h>
#include <variant>

namespace arcticdb::entity {

namespace details {

template<class DimType, class Callable>
auto visit_dim(DataType dt, Callable &&c) {
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
        DT_CASE(EMPTYVAL)
#undef DT_CASE
    default: util::raise_rte("Invalid dtype '{}' in visit dim", datatype_to_str(dt));
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
        DT_CASE(EMPTYVAL)
#undef DT_CASE
    default: util::raise_rte("Invalid dtype '{}' in visit type", datatype_to_str(dt));
    }
}

} // namespace details

template<class Callable>
auto TypeDescriptor::visit_tag(Callable &&callable) const {
    switch (dimension_) {
#define DIM_CASE(__D__) case Dimension::__D__: \
    return details::visit_dim<DimensionTag<Dimension::__D__>>(data_type_, callable)
        DIM_CASE(Dim0);
        DIM_CASE(Dim1);
        DIM_CASE(Dim2);
#undef DIM_CASE
        default:throw std::invalid_argument(fmt::format("Invalid dimension %d", static_cast<uint32_t>(dimension_)));
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
    auto format(arcticdb::entity::DataType dt, FormatContext &ctx) const {
        return format_to(ctx.out(), datatype_to_str(dt));
    }
};

template<>
struct formatter<arcticdb::entity::Dimension> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::entity::Dimension dim, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}", static_cast<uint32_t >(dim));
    }
};

template<>
struct formatter<arcticdb::entity::TypeDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::TypeDescriptor &td, FormatContext &ctx) const {
        return format_to(ctx.out(), "TD<type={}, dim={}>", td.data_type_, td.dimension_);
    }
};

template<>
struct formatter<arcticdb::entity::Field> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::Field &fd, FormatContext &ctx) const {
        if (!fd.name().empty())
            return format_to(ctx.out(), "FD<name={}, type={}>", fd.name(), fd.type());
        else
            return format_to(ctx.out(), "FD<type={}>", fd.type());
    }
};

template<>
struct formatter<arcticdb::entity::IndexDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::IndexDescriptor &idx, FormatContext &ctx) const {
        return format_to(ctx.out(), "IDX<size={}, kind={}>", idx.field_count(), static_cast<char>(idx.type()));
    }
};
template<>
struct formatter<arcticdb::entity::StreamId> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::entity::StreamId &tsid, FormatContext &ctx) const {
        return std::visit([&ctx](auto &&val) {
            return format_to(ctx.out(), "{}", val);
        }, tsid);
    }
};

template<>
struct formatter<arcticdb::proto::descriptors::TypeDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::proto::descriptors::TypeDescriptor& type_desc, FormatContext &ctx) const {
        auto td = arcticdb::entity::type_desc_from_proto(type_desc);
        return format_to(ctx.out(), "{}", td);
    }
};

template<>
struct formatter<arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor& field_desc, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}: {}", field_desc.name(), field_desc.type_desc());
    }
};
}
