/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/types_proto.hpp>
#include <google/protobuf/util/message_differencer.h>

namespace arcticdb::entity {

bool operator==(const FieldProto& left, const FieldProto& right) {
    google::protobuf::util::MessageDifferencer diff;
    return diff.Compare(left, right);
}

bool operator<(const FieldProto& left, const FieldProto& right) { return left.name() < right.name(); }

arcticdb::proto::descriptors::SortedValue sorted_value_to_proto(SortedValue sorted) {
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

SortedValue sorted_value_from_proto(arcticdb::proto::descriptors::SortedValue sorted_proto) {
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

void set_data_type(DataType data_type, arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
    type_desc.set_size_bits(size_bits_proto_from_data_type(data_type));
    type_desc.set_value_type(value_proto_from_data_type(data_type));
}

[[nodiscard]] arcticdb::proto::descriptors::TypeDescriptor type_descriptor_to_proto(const TypeDescriptor& desc) {
    arcticdb::proto::descriptors::TypeDescriptor output;
    output.set_dimension(static_cast<std::uint32_t>(desc.dimension_));
    set_data_type(desc.data_type_, output);
    return output;
}

TypeDescriptor type_desc_from_proto(const arcticdb::proto::descriptors::TypeDescriptor& type_desc) {
    return {combine_data_type(
                    static_cast<ValueType>(static_cast<uint8_t>(type_desc.value_type())),
                    static_cast<SizeBits>(static_cast<uint8_t>(type_desc.size_bits()))
            ),
            static_cast<Dimension>(static_cast<uint8_t>(type_desc.dimension()))};
}

arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor field_proto(
        DataType dt, Dimension dim, std::string_view name
) {
    arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor output;
    if (!name.empty())
        output.set_name(name.data(), name.size());

    auto output_desc = output.mutable_type_desc();
    output_desc->set_dimension(static_cast<uint32_t>(dim));
    output_desc->set_size_bits(static_cast<arcticdb::proto::descriptors::TypeDescriptor_SizeBits>(
            static_cast<std::uint8_t>(slice_bit_size(dt))
    ));

    output_desc->set_value_type(static_cast<arcticdb::proto::descriptors::TypeDescriptor_ValueType>(
            static_cast<std::uint8_t>(slice_value_type(dt))
    ));

    return output;
}

void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id) {
    std::visit(
            [&pb_desc](auto&& arg) {
                using IdType = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<IdType, NumericId>)
                    pb_desc.set_num_id(arg);
                else if constexpr (std::is_same_v<IdType, StringId>)
                    pb_desc.set_str_id(arg);
                else
                    util::raise_rte("Encoding unknown descriptor type");
            },
            id
    );
}

const char* index_type_to_str(IndexDescriptor::Type type) {
    switch (type) {
    case IndexDescriptor::Type::EMPTY:
        return "Empty";
    case IndexDescriptor::Type::TIMESTAMP:
        return "Timestamp";
    case IndexDescriptor::Type::ROWCOUNT:
        return "Row count";
    case IndexDescriptor::Type::STRING:
        return "String";
    case IndexDescriptor::Type::UNKNOWN:
        return "Unknown";
    default:
        util::raise_rte("Unknown index type: {}", int(type));
    }
}
} // namespace arcticdb::entity
