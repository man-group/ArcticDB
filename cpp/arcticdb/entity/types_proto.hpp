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



    inline arcticdb::proto::descriptors::SortedValue sorted_value_to_proto(SortedValue sorted) {
        switch (sorted) {
        case SortedValue::UNSORTED:
            return arcticdb::proto::descriptors::SortedValue::UNSORTED;
        case SortedValue::DESCENDING:
            return arcticdb::proto::descriptors::SortedValue::DESCENDING;
        case SortedValue::ASCENDING:
            return arcticdb::proto::descriptors::SortedValue::ASCENDING;
        default:
            return arcticdb::proto::descriptors::SortedValue::UNKNOWN;
        }
    }

    inline SortedValue sorted_value_from_proto(arcticdb::proto::descriptors::SortedValue sorted_proto) {
        switch (sorted_proto) {
        case arcticdb::proto::descriptors::SortedValue::UNSORTED:
            return SortedValue::UNSORTED;
        case arcticdb::proto::descriptors::SortedValue::DESCENDING:
            return SortedValue::DESCENDING;
        case arcticdb::proto::descriptors::SortedValue::ASCENDING:
            return SortedValue::ASCENDING;
        default:
            return SortedValue::UNKNOWN;
        }
    }


    inline void set_data_type(DataType data_type, arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
        type_desc.set_size_bits(
            static_cast<arcticdb::proto::descriptors::TypeDescriptor_SizeBits>(
                static_cast<std::uint8_t>(slice_bit_size(data_type))));
        type_desc.set_value_type(
            static_cast<arcticdb::proto::descriptors::TypeDescriptor_ValueType>(
                static_cast<std::uint8_t>(slice_value_type(data_type))));
    }


    [[nodiscard]]
    inline auto to_proto(const TypeDescriptor& desc)
        -> arcticdb::proto::descriptors::TypeDescriptor
    {
        arcticdb::proto::descriptors::TypeDescriptor output;
        output.set_dimension(static_cast<std::uint32_t>(desc.dimension_));
        set_data_type(desc.data_type_, output);

        return output;
    }



    inline DataType get_data_type(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
        return combine_data_type(
            static_cast<ValueType>(static_cast<uint8_t>(type_desc.value_type())),
            static_cast<SizeBits>(static_cast<uint8_t>(type_desc.size_bits()))
        );
    }

    inline TypeDescriptor type_desc_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
        return {
            combine_data_type(
                static_cast<ValueType>(static_cast<uint8_t>(type_desc.value_type())),
                static_cast<SizeBits>(static_cast<uint8_t>(type_desc.size_bits()))
            ),
            static_cast<Dimension>(static_cast<uint8_t>(type_desc.dimension()))
        };
    }

    inline DataType data_type_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
        return type_desc_from_proto(type_desc).data_type();
    }


    inline arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor field_proto(DataType dt, Dimension dim, std::string_view name) {
        arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor output;
        if (!name.empty())
            output.set_name(name.data(), name.size());

        auto output_desc = output.mutable_type_desc();
        output_desc->set_dimension(static_cast<uint32_t>(dim));
        output_desc->set_size_bits(static_cast<arcticdb::proto::descriptors::TypeDescriptor_SizeBits>(
            static_cast<std::uint8_t>(slice_bit_size(dt))));

        output_desc->set_value_type(
            static_cast<arcticdb::proto::descriptors::TypeDescriptor_ValueType>(
                static_cast<std::uint8_t>(slice_value_type(dt))));

        return output;
    }



    struct IndexDescriptor {
        using Proto = arcticdb::proto::descriptors::IndexDescriptor;

        Proto data_;
        using Type = arcticdb::proto::descriptors::IndexDescriptor::Type;

        static const Type UNKNOWN = arcticdb::proto::descriptors::IndexDescriptor_Type_UNKNOWN;
        static const Type ROWCOUNT = arcticdb::proto::descriptors::IndexDescriptor_Type_ROWCOUNT;
        static const Type STRING = arcticdb::proto::descriptors::IndexDescriptor_Type_STRING;
        static const Type TIMESTAMP = arcticdb::proto::descriptors::IndexDescriptor_Type_TIMESTAMP;

        using TypeChar = char;

        IndexDescriptor() = default;
        IndexDescriptor(size_t field_count, Type type) {
            data_.set_kind(type);
            data_.set_field_count(static_cast<uint32_t>(field_count));
        }

        explicit IndexDescriptor(arcticdb::proto::descriptors::IndexDescriptor data)
            : data_(std::move(data)) {
        }

        bool uninitialized() const {
            return data_.field_count() == 0 && data_.kind() == Type::IndexDescriptor_Type_UNKNOWN;
        }

        const Proto& proto() const {
            return data_;
        }

        size_t field_count() const {
            return static_cast<size_t>(data_.field_count());
        }

        Type type() const {
            return data_.kind();
        }

        void set_type(Type type) {
            data_.set_kind(type);
        }

        ARCTICDB_MOVE_COPY_DEFAULT(IndexDescriptor)

            friend bool operator==(const IndexDescriptor& left, const IndexDescriptor& right) {
            return left.type() == right.type();
        }
    };

    constexpr IndexDescriptor::TypeChar to_type_char(IndexDescriptor::Type type) {
        switch (type) {
        case IndexDescriptor::TIMESTAMP:return 'T';
        case IndexDescriptor::ROWCOUNT:return 'R';
        case IndexDescriptor::STRING:return 'S';
        case IndexDescriptor::UNKNOWN:return 'U';
        default:util::raise_rte("Unknown index type: {}", int(type));
        }
    }

    constexpr IndexDescriptor::Type from_type_char(IndexDescriptor::TypeChar type) {
        switch (type) {
        case 'T': return IndexDescriptor::TIMESTAMP;
        case 'R': return IndexDescriptor::ROWCOUNT;
        case 'S': return IndexDescriptor::STRING;
        case 'U': return IndexDescriptor::UNKNOWN;
        default:util::raise_rte("Unknown index type: {}", int(type));
        }
    }

    inline void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id) {
        std::visit([&pb_desc](auto&& arg) {
            using IdType = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<IdType, NumericId>)
                pb_desc.set_num_id(arg);
            else if constexpr (std::is_same_v<IdType, StringId>)
                pb_desc.set_str_id(arg);
            else
                util::raise_rte("Encoding unknown descriptor type");
            }, id);
    }

} // namespace arcticdb::entity


namespace fmt {

    template<>
    struct formatter<arcticdb::proto::descriptors::TypeDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::proto::descriptors::TypeDescriptor& type_desc, FormatContext& ctx) const {
            auto td = arcticdb::entity::type_desc_from_proto(type_desc);
            return format_to(ctx.out(), "{}", td);
        }
    };

    template<>
    struct formatter<arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor& field_desc, FormatContext& ctx) const {
            return format_to(ctx.out(), "{}: {}", field_desc.name(), field_desc.type_desc());
        }
    };

    template<>
    struct formatter<arcticdb::entity::IndexDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::entity::IndexDescriptor& idx, FormatContext& ctx) const {
            return format_to(ctx.out(), "IDX<size={}, kind={}>", idx.field_count(), static_cast<char>(idx.type()));
        }
    };

    template<>
    struct formatter<arcticdb::entity::Field> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::entity::Field& fd, FormatContext& ctx) const {
            if (!fd.name().empty())
                return format_to(ctx.out(), "FD<name={}, type={}>", fd.name(), fd.type());
            else
                return format_to(ctx.out(), "FD<type={}>", fd.type());
        }
    };
}
