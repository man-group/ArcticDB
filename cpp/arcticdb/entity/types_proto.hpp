/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

#include <descriptors.pb.h>

namespace arcticdb::proto {
    namespace descriptors = arcticc::pb2::descriptors_pb2;
}

namespace arcticdb::entity {

    using FieldProto = arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor;

    bool operator==(const FieldProto& left, const FieldProto& right);
    bool operator<(const FieldProto& left, const FieldProto& right);



    arcticdb::proto::descriptors::SortedValue sorted_value_to_proto(SortedValue sorted);

    SortedValue sorted_value_from_proto(arcticdb::proto::descriptors::SortedValue sorted_proto);


    void set_data_type(DataType data_type, arcticdb::proto::descriptors::TypeDescriptor& type_desc);


    DataType get_data_type(const arcticdb::proto::descriptors::TypeDescriptor& type_desc);

    TypeDescriptor type_desc_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc);

    DataType data_type_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc);


    arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor field_proto(DataType dt, Dimension dim, std::string_view name);



    struct IndexDescriptor {
        using Proto = arcticdb::proto::descriptors::IndexDescriptor;

        Proto data_;
        using Type = arcticdb::proto::descriptors::IndexDescriptor::Type;

        static constexpr Type UNKNOWN = arcticdb::proto::descriptors::IndexDescriptor_Type_UNKNOWN;
        static constexpr Type EMPTY = arcticdb::proto::descriptors::IndexDescriptor_Type_EMPTY;
        static constexpr Type ROWCOUNT = arcticdb::proto::descriptors::IndexDescriptor_Type_ROWCOUNT;
        static constexpr Type STRING = arcticdb::proto::descriptors::IndexDescriptor_Type_STRING;
        static constexpr Type TIMESTAMP = arcticdb::proto::descriptors::IndexDescriptor_Type_TIMESTAMP;

        using TypeChar = char;

        IndexDescriptor() = default;
        ARCTICDB_MOVE_COPY_DEFAULT(IndexDescriptor)
        IndexDescriptor(size_t field_count, Type type);
        explicit IndexDescriptor(arcticdb::proto::descriptors::IndexDescriptor data);
        bool uninitialized() const;
        const Proto& proto() const;
        size_t field_count() const;
        Type type() const;
        void set_type(Type type);
        friend bool operator==(const IndexDescriptor& left, const IndexDescriptor& right);
    };

    IndexDescriptor::TypeChar to_type_char(IndexDescriptor::Type type);
    IndexDescriptor::Type from_type_char(IndexDescriptor::TypeChar type);

    void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id);

} // namespace arcticdb::entity


namespace fmt {

    template<>
    struct formatter<arcticdb::proto::descriptors::TypeDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::proto::descriptors::TypeDescriptor& type_desc, FormatContext& ctx) const {
            auto td = arcticdb::entity::type_desc_from_proto(type_desc);
            return fmt::format_to(ctx.out(), "{}", td);
        }
    };

    template<>
    struct formatter<arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor& field_desc, FormatContext& ctx) const {
            return fmt::format_to(ctx.out(), "{}: {}", field_desc.name(), field_desc.type_desc());
        }
    };

    template<>
    struct formatter<arcticdb::entity::IndexDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::entity::IndexDescriptor& idx, FormatContext& ctx) const {
            return fmt::format_to(ctx.out(), "IDX<size={}, kind={}>", idx.field_count(), static_cast<char>(idx.type()));
        }
    };

    template<>
    struct formatter<arcticdb::entity::Field> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::entity::Field& fd, FormatContext& ctx) const {
            if (!fd.name().empty())
                return fmt::format_to(ctx.out(), "FD<name={}, type={}>", fd.name(), fd.type());
            else
                return fmt::format_to(ctx.out(), "FD<type={}>", fd.type());
        }
    };
}
