/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/preconditions.hpp>
#include "arcticdb/storage/memory_layout.hpp"
#include <arcticdb/codec/segment_header.hpp>

namespace arcticdb {

template<typename T, typename U>
void copy_codec(T& out_codec, const U& in_codec) {
    out_codec.MergeFrom(in_codec);
}

inline void copy_codec(ZstdCodec& codec, const arcticdb::proto::encoding::VariantCodec::Zstd& zstd) {
    codec.level_ = zstd.level();
    codec.is_streaming_ = zstd.is_streaming();
}

inline void copy_codec(Lz4Codec& codec, const arcticdb::proto::encoding::VariantCodec::Lz4& lz4) {
    codec.acceleration_ = lz4.acceleration();
}

inline void copy_codec(PassthroughCodec&, const arcticdb::proto::encoding::VariantCodec::Passthrough&) {
    // No data in passthrough
}

[[nodiscard]] inline arcticdb::proto::encoding::VariantCodec::CodecCase codec_case(Codec codec) {
    switch (codec) {
    case Codec::ZSTD:
        return arcticdb::proto::encoding::VariantCodec::kZstd;
    case Codec::LZ4:
        return arcticdb::proto::encoding::VariantCodec::kLz4;
    case Codec::PFOR:
        return arcticdb::proto::encoding::VariantCodec::kTp4;
    case Codec::PASS:
        return arcticdb::proto::encoding::VariantCodec::kPassthrough;
    default:
        util::raise_rte("Unknown codec");
    }
}

template<typename Input, typename Output>
void set_codec(Input& in, Output& out) {
    copy_codec(out, in);
}

void block_from_proto(const arcticdb::proto::encoding::Block& input, EncodedBlock& output, bool is_shape);

inline void set_lz4(const Lz4Codec& lz4_in, arcticdb::proto::encoding::VariantCodec::Lz4& lz4_out) {
    lz4_out.set_acceleration(lz4_in.acceleration_);
}

inline void set_zstd(const ZstdCodec& zstd_in, arcticdb::proto::encoding::VariantCodec::Zstd& zstd_out) {
    zstd_out.set_is_streaming(zstd_in.is_streaming_);
    zstd_out.set_level(zstd_in.level_);
}

inline void set_passthrough(
        const PassthroughCodec& passthrough_in, arcticdb::proto::encoding::VariantCodec::Passthrough& passthrough_out
) {
    passthrough_out.set_mark(passthrough_in.unused_);
}

void proto_from_block(const EncodedBlock& input, arcticdb::proto::encoding::Block& output);

void encoded_field_from_proto(const arcticdb::proto::encoding::EncodedField& input, EncodedFieldImpl& output);

void copy_encoded_field_to_proto(const EncodedFieldImpl& input, arcticdb::proto::encoding::EncodedField& output);

SegmentHeader deserialize_segment_header_from_proto(const arcticdb::proto::encoding::SegmentHeader& header);

size_t calc_proto_encoded_blocks_size(const arcticdb::proto::encoding::SegmentHeader& hdr);

EncodedFieldCollection encoded_fields_from_proto(const arcticdb::proto::encoding::SegmentHeader& hdr);

void copy_encoded_fields_to_proto(const EncodedFieldCollection& fields, arcticdb::proto::encoding::SegmentHeader& hdr);

} // namespace arcticdb