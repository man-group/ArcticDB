/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/variant.hpp>


namespace arcticdb {

using namespace arcticdb::entity;

inline arcticdb::proto::descriptors::AtomKey encode_key(const AtomKey &key) {
    arcticdb::proto::descriptors::AtomKey output;
    util::variant_match(key.id(),
                        [&](const StringId &id) { output.set_string_id(id); },
                        [&](const NumericId &id) { output.set_numeric_id(id); });
    output.set_version_id(key.version_id());
    output.set_creation_ts(key.creation_ts());
    output.set_content_hash(key.content_hash());

    util::variant_match(key.start_index(),
                        [&](const StringId &id) { output.set_string_start(id); },
                        [&](const NumericId &id) { output.set_numeric_start(id); });
    util::variant_match(key.end_index(),
                        [&](const StringId &id) { output.set_string_end(id); },
                        [&](const NumericId &id) { output.set_numeric_end(id); });

    output.set_key_type(arcticdb::proto::descriptors::KeyType (int(key.type())));
    return output;
}

inline AtomKey decode_key(const arcticdb::proto::descriptors::AtomKey& input) {
    StreamId stream_id = input.id_case() == input.kNumericId ? StreamId(input.numeric_id()) : StreamId(input.string_id());
    IndexValue index_start = input.index_start_case() == input.kNumericStart ? IndexValue(input.numeric_start()) : IndexValue(input.string_start());
    IndexValue index_end = input.index_end_case() == input.kNumericEnd ? IndexValue(input.numeric_end() ): IndexValue(input.string_end());

    return atom_key_builder()
         .version_id(input.version_id())
         .creation_ts(timestamp(input.creation_ts()))
         .content_hash(input.content_hash())
         .start_index(index_start)
         .end_index(index_end)
         .build(stream_id, KeyType(input.key_type()));
}

[[nodiscard]] arcticdb::proto::descriptors::StreamDescriptor copy_stream_descriptor_to_proto(const StreamDescriptor& desc) {
    using Proto = arcticdb::proto::descriptors::StreamDescriptor;

    Proto proto;
    proto.set_in_bytes(desc.uncompressed_bytes());
    proto.set_out_bytes(desc.compressed_bytes());
    proto.set_sorted(arcticdb::proto::descriptors::SortedValue(desc.sorted()));
    util::variant_match(desc.id(),
                        [&proto] (const StringId& str) { proto.set_str_id(str); },
                        [&proto] (const NumericId& n) { proto.set_num_id(n); });

    proto.mutable_fields()->Clear();
    for(const auto& field : desc.fields()) {
        auto new_field = proto.mutable_fields()->Add();
        new_field->set_name(std::string(field.name()));
        new_field->mutable_type_desc()->set_dimension(static_cast<uint32_t>(field.type().dimension()));
        set_data_type(field.type().data_type(), *new_field->mutable_type_desc());
    }
    return proto;
}

static StreamId id_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& proto) {
    if(proto.id_case() == arcticdb::proto::descriptors::StreamDescriptor::kNumId)
        return NumericId(proto.num_id());
    else
        return proto.str_id();
}

inline void set_id(arcticdb::proto::descriptors::StreamDescriptor& pb_desc, StreamId id) {
    std::visit([&pb_desc](const auto& arg) {
        using IdType = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<IdType, NumericId>)
            pb_desc.set_num_id(arg);
        else if constexpr (std::is_same_v<IdType, StringId>)
            pb_desc.set_str_id(arg);
        else
            util::raise_rte("Encoding unknown descriptor type");
    }, id);
}

} //namespace arcticdb