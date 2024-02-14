#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/memory_layout.hpp>

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

size_t calc_encoded_field_buffer_size(const arcticdb::proto::encoding::EncodedField& field) {
    size_t bytes = EncodedFieldImpl::Size;
    util::check(field.has_ndarray(), "Only ndarray translations supported");
    const auto& ndarray = field.ndarray();
    util::check(ndarray.shapes_size() < 2, "Unexpected number of shapes in proto translation: {}", ndarray.shapes_size());
    bytes += sizeof(EncodedBlock) * ndarray.shapes_size();
    bytes += sizeof(EncodedBlock) * ndarray.values_size();
    return bytes;
}

template <typename Input, typename Output>
void set_codec(Input& in, Output& out) {
    copy_codec(out, in);
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
    Buffer buffer(encoded_buffer_size);
    auto pos = 0U;
    for(const auto& in_field : hdr.fields()) {
       auto* out_field = reinterpret_cast<EncodedFieldImpl*>(buffer.data() + pos);
        encoded_field_from_proto(in_field, *out_field);
    }
    return EncodedFieldCollection{std::move(buffer)};
}

} //namespace arcticdb