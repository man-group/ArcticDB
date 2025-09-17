/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/preconditions.hpp>
#include "arcticdb/storage/memory_layout.hpp"
#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/codec/protobuf_mappings.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {

void block_from_proto(const arcticdb::proto::encoding::Block& input, EncodedBlock& output, bool is_shape) {
    output.set_in_bytes(input.in_bytes());
    output.set_out_bytes(input.out_bytes());
    output.set_hash(input.hash());
    output.set_encoder_version(static_cast<uint16_t>(input.encoder_version()));
    output.is_shape_ = is_shape;
    switch (input.codec().codec_case()) {
    case arcticdb::proto::encoding::VariantCodec::kZstd: {
        set_codec(input.codec().zstd(), *output.mutable_codec()->mutable_zstd());
        break;
    }
    case arcticdb::proto::encoding::VariantCodec::kLz4: {
        set_codec(input.codec().lz4(), *output.mutable_codec()->mutable_lz4());
        break;
    }
    case arcticdb::proto::encoding::VariantCodec::kPassthrough: {
        set_codec(input.codec().passthrough(), *output.mutable_codec()->mutable_passthrough());
        break;
    }
    default:
        util::raise_rte("Unrecognized_codec");
    }
}

void proto_from_block(const EncodedBlock& input, arcticdb::proto::encoding::Block& output) {
    output.set_in_bytes(input.in_bytes());
    output.set_out_bytes(input.out_bytes());
    output.set_hash(input.hash());
    output.set_encoder_version(input.encoder_version());

    switch (input.codec().codec_) {
    case Codec::ZSTD: {
        set_zstd(input.codec().zstd(), *output.mutable_codec()->mutable_zstd());
        break;
    }
    case Codec::LZ4: {
        set_lz4(input.codec().lz4(), *output.mutable_codec()->mutable_lz4());
        break;
    }
    case Codec::PASS: {
        set_passthrough(input.codec().passthrough(), *output.mutable_codec()->mutable_passthrough());
        break;
    }
    default:
        util::raise_rte("Unrecognized_codec");
    }
}

void encoded_field_from_proto(const arcticdb::proto::encoding::EncodedField& input, EncodedFieldImpl& output) {
    util::check(input.has_ndarray(), "Only ndarray fields supported for v1 encoding");
    const auto& input_ndarray = input.ndarray();
    auto* output_ndarray = output.mutable_ndarray();
    output_ndarray->set_items_count(input_ndarray.items_count());
    output_ndarray->set_sparse_map_bytes(input_ndarray.sparse_map_bytes());

    for (auto i = 0; i < input_ndarray.shapes_size(); ++i) {
        auto* shape_block = output_ndarray->add_shapes();
        block_from_proto(input_ndarray.shapes(i), *shape_block, true);
    }

    for (auto i = 0; i < input_ndarray.values_size(); ++i) {
        auto* value_block = output_ndarray->add_values(EncodingVersion::V1);
        block_from_proto(input_ndarray.values(i), *value_block, false);
    }

    output.set_statistics(create_from_proto(input.stats()));
}

void copy_encoded_field_to_proto(const EncodedFieldImpl& input, arcticdb::proto::encoding::EncodedField& output) {
    util::check(input.has_ndarray(), "Only ndarray fields supported for v1 encoding");
    ARCTICDB_TRACE(log::codec(), "Copying field to proto: {}", input);
    const auto& input_ndarray = input.ndarray();
    auto* output_ndarray = output.mutable_ndarray();
    output_ndarray->set_items_count(input_ndarray.items_count());
    output_ndarray->set_sparse_map_bytes(input_ndarray.sparse_map_bytes());

    for (auto i = 0; i < input_ndarray.shapes_size(); ++i) {
        auto* shape_block = output_ndarray->add_shapes();
        proto_from_block(input_ndarray.shapes(i), *shape_block);
    }

    for (auto i = 0; i < input_ndarray.values_size(); ++i) {
        auto* value_block = output_ndarray->add_values();
        proto_from_block(input_ndarray.values(i), *value_block);
    }

    field_stats_to_proto(input.get_statistics(), *output.mutable_stats());
}

size_t num_blocks(const arcticdb::proto::encoding::EncodedField& field) {
    util::check(field.has_ndarray(), "Expected ndarray in segment header");
    return field.ndarray().shapes_size() + field.ndarray().values_size();
}

SegmentHeader deserialize_segment_header_from_proto(const arcticdb::proto::encoding::SegmentHeader& header) {
    SegmentHeader output;
    output.set_encoding_version(EncodingVersion(header.encoding_version()));
    output.set_compacted(header.compacted());

    if (header.has_metadata_field())
        encoded_field_from_proto(
                header.metadata_field(), output.mutable_metadata_field(num_blocks(header.metadata_field()))
        );

    if (header.has_string_pool_field())
        encoded_field_from_proto(
                header.string_pool_field(), output.mutable_string_pool_field(num_blocks(header.string_pool_field()))
        );

    auto fields_from_proto = encoded_fields_from_proto(header);
    output.set_body_fields(std::move(fields_from_proto));
    return output;
}

size_t calc_proto_encoded_blocks_size(const arcticdb::proto::encoding::SegmentHeader& hdr) {
    size_t bytes{};
    for (const auto& field : hdr.fields()) {
        bytes += EncodedFieldImpl::Size;
        if (field.has_ndarray()) {
            const auto& ndarray = field.ndarray();
            const auto shapes_size = sizeof(EncodedBlock) * ndarray.shapes_size();
            const auto values_size = sizeof(EncodedBlock) * ndarray.values_size();
            bytes += shapes_size + values_size;
        }
    }
    return bytes;
}

EncodedFieldCollection encoded_fields_from_proto(const arcticdb::proto::encoding::SegmentHeader& hdr) {
    const auto encoded_buffer_size = calc_proto_encoded_blocks_size(hdr);
    EncodedFieldCollection encoded_fields;
    encoded_fields.reserve(encoded_buffer_size, hdr.fields_size());
    for (auto&& [index, in_field] : folly::enumerate(hdr.fields())) {
        auto* out_field = encoded_fields.add_field(num_blocks(in_field));
        encoded_field_from_proto(in_field, *out_field);
    }
    return encoded_fields;
}

void copy_encoded_fields_to_proto(const EncodedFieldCollection& fields, arcticdb::proto::encoding::SegmentHeader& hdr) {
    auto& proto_fields = *hdr.mutable_fields();
    auto field = fields.begin();
    for (auto i = 0U; i < fields.size(); ++i) {
        auto* proto_field = proto_fields.Add();
        copy_encoded_field_to_proto(field.current(), *proto_field);
        ++field;
    }
}

} // namespace arcticdb
