#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/memory_layout.hpp>
#include <arcticdb/codec/segment_header.hpp>

namespace arcticdb {

template <typename T, typename U>
void copy_codec(T& out_codec, const U& in_codec) {
    out_codec.MergeFrom(in_codec);
}

void copy_codec(ZstdCodec& codec, const arcticdb::proto::encoding::VariantCodec::Zstd& zstd) {
    codec.level_ = zstd.level();
    codec.is_streaming_ = zstd.is_streaming();
}

void copy_codec(Lz4Codec& codec, const arcticdb::proto::encoding::VariantCodec::Lz4& lz4) {
    codec.acceleration_ = lz4.acceleration();
}

void copy_codec(PassthroughCodec&, const arcticdb::proto::encoding::VariantCodec::Passthrough&) {
    // No data in passthrough
}

[[nodiscard]] arcticdb::proto::encoding::VariantCodec::CodecCase codec_case(Codec codec) {
    switch (codec) {
    case Codec::ZSTD:return arcticdb::proto::encoding::VariantCodec::kZstd;
    case Codec::LZ4:return arcticdb::proto::encoding::VariantCodec::kLz4;
    case Codec::PFOR:return arcticdb::proto::encoding::VariantCodec::kTp4;
    case Codec::PASS:return arcticdb::proto::encoding::VariantCodec::kPassthrough;
    default:util::raise_rte("Unknown codec");
    }
}

template <typename Input, typename Output>
void set_codec(Input& in, Output& out) {
    copy_codec(out, in);
}

void block_from_proto(const arcticdb::proto::encoding::Block& input, EncodedBlock& output, bool is_shape);

void set_lz4(const Lz4Codec& lz4_in, arcticdb::proto::encoding::VariantCodec::Lz4& lz4_out) {
    lz4_out.set_acceleration(lz4_in.acceleration_);
}

void set_zstd(const ZstdCodec& zstd_in, arcticdb::proto::encoding::VariantCodec::Zstd& zstd_out) {
    zstd_out.set_is_streaming(zstd_in.is_streaming_);
    zstd_out.set_level(zstd_in.level_);
}

void set_passthrough(const PassthroughCodec& passthrough_in, arcticdb::proto::encoding::VariantCodec::Passthrough& passthrough_out) {
    passthrough_out.set_mark(passthrough_in.unused_);
}

size_t calc_encoded_field_buffer_size(const arcticdb::proto::encoding::EncodedField& field);

void proto_from_block(const EncodedBlock& input, arcticdb::proto::encoding::Block& output);

void encoded_field_from_proto(const arcticdb::proto::encoding::EncodedField& input, EncodedFieldImpl& output);

void proto_from_encoded_field(const EncodedFieldImpl& input, arcticdb::proto::encoding::EncodedField& output);

SegmentHeader deserialize_segment_header_from_proto(const arcticdb::proto::encoding::SegmentHeader& header);

void serialize_segment_header_to_proto(uint8_t* dst, const SegmentHeader& hdr);

size_t calc_proto_encoded_blocks_size(const arcticdb::proto::encoding::SegmentHeader& hdr);

EncodedFieldCollection encoded_fields_from_proto(const arcticdb::proto::encoding::SegmentHeader& hdr);

} //namespace arcticdb