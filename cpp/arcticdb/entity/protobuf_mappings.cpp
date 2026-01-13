/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

namespace arcticdb {

using namespace arcticdb::entity;

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

// The type enum needs to be kept in sync with the protobuf one, which should not be changed
[[nodiscard]] arcticdb::proto::descriptors::IndexDescriptor index_descriptor_to_proto(
        const IndexDescriptorImpl& index_descriptor
) {
    arcticdb::proto::descriptors::IndexDescriptor proto;
    proto.set_kind(static_cast<arcticdb::proto::descriptors::IndexDescriptor_Type>(index_descriptor.type_));
    proto.set_field_count(index_descriptor.field_count_);
    return proto;
}

[[nodiscard]] IndexDescriptorImpl index_descriptor_from_proto(
        const arcticdb::proto::descriptors::IndexDescriptor& index_descriptor
) {
    IndexDescriptorImpl output;
    output.set_type(static_cast<IndexDescriptor::Type>(index_descriptor.kind()));
    output.set_field_count(index_descriptor.field_count());
    return output;
}

arcticdb::proto::descriptors::AtomKey key_to_proto(const AtomKey& key) {
    arcticdb::proto::descriptors::AtomKey output;
    util::variant_match(
            key.id(),
            [&](const StringId& id) { output.set_string_id(id); },
            [&](const NumericId& id) { output.set_numeric_id(id); }
    );

    output.set_version_id(key.version_id());
    output.set_creation_ts(key.creation_ts());
    output.set_content_hash(key.content_hash());

    util::check(
            std::holds_alternative<StringId>(key.start_index()) || !std::holds_alternative<StringId>(key.end_index()),
            "Start and end index mismatch"
    );

    util::variant_match(
            key.start_index(),
            [&](const StringId& id) { output.set_string_start(id); },
            [&](const NumericId& id) { output.set_numeric_start(id); }
    );

    util::variant_match(
            key.end_index(),
            [&](const StringId& id) { output.set_string_end(id); },
            [&](const NumericId& id) { output.set_numeric_end(id); }
    );

    output.set_key_type(arcticdb::proto::descriptors::KeyType(int(key.type())));
    return output;
}

AtomKey key_from_proto(const arcticdb::proto::descriptors::AtomKey& input) {
    StreamId stream_id =
            input.id_case() == input.kNumericId ? StreamId(input.numeric_id()) : StreamId(input.string_id());
    IndexValue index_start = input.index_start_case() == input.kNumericStart ? IndexValue(input.numeric_start())
                                                                             : IndexValue(input.string_start());
    IndexValue index_end = input.index_end_case() == input.kNumericEnd ? IndexValue(input.numeric_end())
                                                                       : IndexValue(input.string_end());

    return atom_key_builder()
            .version_id(input.version_id())
            .creation_ts(timestamp(input.creation_ts()))
            .content_hash(input.content_hash())
            .start_index(index_start)
            .end_index(index_end)
            .build(stream_id, KeyType(input.key_type()));
}

void copy_stream_descriptor_to_proto(
        const StreamDescriptor& desc, arcticdb::proto::descriptors::StreamDescriptor& proto
) {
    proto.set_in_bytes(desc.uncompressed_bytes());
    proto.set_out_bytes(desc.compressed_bytes());
    proto.set_sorted(sorted_value_to_proto(desc.sorted()));
    // The index descriptor enum must be kept in sync with the protobuf
    *proto.mutable_index() = index_descriptor_to_proto(desc.index());
    util::variant_match(
            desc.id(),
            [&proto](const StringId& str) { proto.set_str_id(str); },
            [&proto](const NumericId& n) { proto.set_num_id(n); }
    );

    proto.mutable_fields()->Clear();
    for (const auto& field : desc.fields()) {
        auto new_field = proto.mutable_fields()->Add();
        new_field->set_name(std::string(field.name()));
        new_field->mutable_type_desc()->set_dimension(static_cast<uint32_t>(field.type().dimension()));
        set_data_type(field.type().data_type(), *new_field->mutable_type_desc());
    }
}

arcticdb::proto::descriptors::TimeSeriesDescriptor copy_time_series_descriptor_to_proto(const TimeseriesDescriptor& tsd
) {
    arcticdb::proto::descriptors::TimeSeriesDescriptor output;

    output.set_total_rows(tsd.total_rows());
    if (tsd.column_groups())
        output.mutable_column_groups()->set_enabled(true);

    exchange_timeseries_proto(tsd.proto(), output);

    auto index_stream_descriptor = tsd.as_stream_descriptor();
    copy_stream_descriptor_to_proto(index_stream_descriptor, *output.mutable_stream_descriptor());
    return output;
}

} // namespace arcticdb
