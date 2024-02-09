//
// Created by root on 2/16/24.
//
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/memory_layout.hpp>
#include <arcticdb/codec/segment_header.hpp>
#include <arcticdb/codec/protobuf_mappings.hpp>
#include <folly/container/Enumerate.h>

namespace arcticdb {

size_t calc_encoded_field_buffer_size(const arcticdb::proto::encoding::EncodedField& field) {
    size_t bytes = EncodedFieldImpl::Size;
    util::check(field.has_ndarray(), "Only ndarray translations supported");
    const auto& ndarray = field.ndarray();
    util::check(ndarray.shapes_size() < 2, "Unexpected number of shapes in proto translation: {}", ndarray.shapes_size());
    bytes += sizeof(EncodedBlock) * ndarray.shapes_size();
    bytes += sizeof(EncodedBlock) * ndarray.values_size();
    return bytes;
}

void block_from_proto(const arcticdb::proto::encoding::Block& input, EncodedBlock& output, bool is_shape) {
    output.set_in_bytes(input.in_bytes());
    output.set_out_bytes(input.out_bytes());
    output.set_hash(input.hash());
    output.set_encoder_version(static_cast<uint8_t>(input.encoder_version()));
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
    case arcticdb::proto::encoding::VariantCodec::kPassthrough : {
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
    util::check(input_ndarray.shapes_size() < 2, "Unexpected number of shapes in proto translation");
    if(input_ndarray.shapes_size() == 1) {
        auto* shape_block = output_ndarray->add_shapes();
        block_from_proto(input_ndarray.shapes(0), *shape_block, true);
    }

    for(auto i = 0; i < input_ndarray.values_size(); ++i) {
        auto* value_block = output_ndarray->add_values();
        block_from_proto(input_ndarray.values(i), *value_block, false);
    }
}

void proto_from_encoded_field(const EncodedFieldImpl& input, arcticdb::proto::encoding::EncodedField& output) {
    util::check(input.has_ndarray(), "Only ndarray fields supported for v1 encoding");
    const auto& input_ndarray = input.ndarray();
    auto* output_ndarray = output.mutable_ndarray();

    output_ndarray->set_items_count(input_ndarray.items_count());
    util::check(input_ndarray.shapes_size() < 2, "Unexpected number of shapes in proto translation");
    if(input_ndarray.shapes_size() == 1) {
        auto* shape_block = output_ndarray->add_shapes();
        proto_from_block(input_ndarray.shapes(0), *shape_block);
    }

    for(auto i = 0; i < input_ndarray.values_size(); ++i) {
        auto* value_block = output_ndarray->add_values();
        proto_from_block(input_ndarray.values(i), *value_block);
    }
}

void deserialize_proto_field(
    SegmentHeader& segment_header,
    FieldOffset field_offset,
    CursoredBuffer<Buffer>& buffer,
    const arcticdb::proto::encoding::EncodedField& field,
    size_t& pos) {
    segment_header.set_field(field_offset);
    segment_header.set_offset(field_offset, pos++);
    const auto field_size = calc_encoded_field_buffer_size(field);
    buffer.ensure<uint8_t>(field_size);
    auto* data = buffer.data();
    encoded_field_from_proto(field, *reinterpret_cast<EncodedFieldImpl*>(data));
}

SegmentHeader deserialize_segment_header_from_proto(const arcticdb::proto::encoding::SegmentHeader& header) {
    SegmentHeader output;
    output.set_encoding_version(EncodingVersion(header.encoding_version()));
    output.set_compacted(header.compacted());

    auto pos = 0UL;
    CursoredBuffer<Buffer> buffer;
    if(header.has_metadata_field())
        deserialize_proto_field(output, FieldOffset::METADATA, buffer, header.descriptor_field(), pos);

    if(header.has_string_pool_field())
        deserialize_proto_field(output, FieldOffset::STRING_POOL, buffer, header.string_pool_field(), pos);

    if(header.has_descriptor_field())
        deserialize_proto_field(output, FieldOffset::DESCRIPTOR, buffer, header.descriptor_field(), pos);

    if(header.has_index_descriptor_field())
        deserialize_proto_field(output, FieldOffset::INDEX, buffer, header.index_descriptor_field(), pos);

    if(header.has_column_fields())
        deserialize_proto_field(output, FieldOffset::COLUMN, buffer, header.column_fields(), pos);
}

void serialize_segment_header_to_proto(uint8_t* dst, const SegmentHeader& hdr) {
    arcticdb::proto::encoding::SegmentHeader segment_header;
    if(hdr.has_metadata_field())
        proto_from_encoded_field(hdr.metadata_field(), *segment_header.mutable_metadata_field());

    if(hdr.has_string_pool_field())
        proto_from_encoded_field(hdr.metadata_field(), *segment_header.mutable_metadata_field());

    if(hdr.has_descriptor_field())
        proto_from_encoded_field(hdr.metadata_field(), *segment_header.mutable_metadata_field());

    if(hdr.has_index_descriptor_field())
        proto_from_encoded_field(hdr.metadata_field(), *segment_header.mutable_metadata_field());

    if(hdr.has_column_fields())
        proto_from_encoded_field(hdr.metadata_field(), *segment_header.mutable_metadata_field());

    const auto hdr_size = segment_header.ByteSizeLong();
    google::protobuf::io::ArrayOutputStream aos(dst + FIXED_HEADER_SIZE, static_cast<int>(hdr_size));
    segment_header.SerializeToZeroCopyStream(&aos);
}

size_t calc_proto_encoded_blocks_size(const arcticdb::proto::encoding::SegmentHeader& hdr) {
    size_t bytes{};
    for(const auto& field : hdr.fields()) {
        bytes += EncodedFieldImpl::Size;
        if(field.has_ndarray()) {
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
    EncodedFieldCollection encoded_fields(encoded_buffer_size, hdr.fields_size());
    auto buffer = ChunkedBuffer::presized(encoded_buffer_size);
    auto pos = 0U;
    for(auto&& [index, in_field] : folly::enumerate(hdr.fields())) {
        auto* out_field = encoded_fields.add_field(pos, static_cast<shape_t>(index));
        encoded_field_from_proto(in_field, *out_field);
    }
    return encoded_fields;
}

} //namespace arcticdb
