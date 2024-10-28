#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/passthrough.hpp>
#include <arcticdb/codec/zstd.hpp>
#include <arcticdb/codec/lz4.hpp>
#include <arcticdb/codec/adaptive_encoder.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/buffer.hpp>

#include <type_traits>

namespace arcticdb {
using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;

template<template<typename> class TypedBlock, class TD, EncodingVersion encoder_version>
struct TypedBlockEncoderImpl {

    static size_t max_compressed_size(
            const BlockCodecImpl& codec_opts,
            const TypedBlock<TD>& typed_block) {
        return visit_encoder(codec_opts, [&](auto encoder_tag) {
            return decltype(encoder_tag)::Encoder::max_compressed_size(typed_block);
        });
    }
    /**
     * Perform encoding of in memory field for storage
     * @param[in] codec_opts Option used to dispatch to the appropriate encoder and configure it
     * @param[in] typed_block The block to be encoded
     * @param[in, out] field description of the encoding operation
     * @param[out] out output buffer to write the encoded values to. Must be resized if pos becomes > size
     * @param[in, out] pos position in bytes in the buffer where to start writing.
     *  Modified to reflect the position after the last byte written
     */
    static void encode(
            const BlockCodecImpl& codec_opts,
            const TypedBlock<TD>& typed_block,
            EncodedFieldImpl& field,
            Buffer& out,
            std::ptrdiff_t& pos) {
        static_assert(encoder_version == EncodingVersion::V1, "Encoding of both shapes and values at the same time is allowed only in V1 encoding");
        visit_encoder(codec_opts, [&](auto encoder_tag) {
            decltype(encoder_tag)::Encoder::encode(
                get_opts(codec_opts, encoder_tag),
                typed_block,
                field,
                out,
                pos);
        });
    }

    template<typename EncodedFieldType>
    static void encode_values(
        const BlockCodecImpl& codec_opts,
        const TypedBlock<TD>& typed_block,
        EncodedFieldType& field,
        Buffer& out,
        std::ptrdiff_t& pos
    ) {
        static_assert(encoder_version == EncodingVersion::V2, "Encoding values separately from the shapes is allowed only in V2 encoding");
        auto* ndarray = field.mutable_ndarray();
        if(typed_block.nbytes() == 0) {
            ARCTICDB_TRACE(log::codec(), "Encoder got values of size 0. Noting to encode.");
            return;
        }

        auto* values_encoded_block = ndarray->add_values(encoder_version);
        visit_encoder(codec_opts, [&](auto encoder_tag) {
            decltype(encoder_tag)::Encoder::encode(
                get_opts(codec_opts, encoder_tag),
                typed_block,
                out,
                pos,
                values_encoded_block);
        });
        const auto existing_items_count = ndarray->items_count();
        ndarray->set_items_count(existing_items_count + typed_block.row_count());
    }

    template<typename EncodedFieldType>
    static void encode_shapes(
            const BlockCodecImpl& codec_opts,
            const TypedBlockData<ShapesBlockTDT>& typed_block,
            EncodedFieldType& field,
            Buffer& out,
            std::ptrdiff_t& pos) {
        static_assert(encoder_version == EncodingVersion::V2, "Encoding shapes separately from the values is allowed only in V2 encoding");

        if(typed_block.nbytes() == 0) {
            ARCTICDB_TRACE(log::codec(), "Encoder got shapes of size 0. Noting to encode.");
            return;
        }

        auto* ndarray = field.mutable_ndarray();
        auto* shapes_encoded_block = ndarray->add_shapes();
        visit_encoder(codec_opts, [&](auto encoder_tag) {
            decltype(encoder_tag)::Encoder::encode(
                get_opts(codec_opts, encoder_tag),
                typed_block,
                out,
                pos,
                shapes_encoded_block);
        });
    }

private:
    template<class EncoderType>
    using BlockEncoder = std::conditional_t<encoder_version == EncodingVersion::V1,
        arcticdb::detail::GenericBlockEncoder<TypedBlock<TD>, TD, EncoderType>,
        arcticdb::detail::GenericBlockEncoderV2<TypedBlock<TD>, TD, EncoderType>>;

    using ZstdEncoder = BlockEncoder<arcticdb::detail::ZstdBlockEncoder>;
    using Lz4Encoder = BlockEncoder<arcticdb::detail::Lz4BlockEncoder>;

    using PassthroughEncoder = std::conditional_t<encoder_version == EncodingVersion::V1,
        arcticdb::detail::PassthroughEncoderV1<TypedBlock, TD>,
        arcticdb::detail::PassthroughEncoderV2<TypedBlock, TD>>;

    using AdaptiveEncoder = std::conditional_t<encoder_version == EncodingVersion::V1,
                                                  arcticdb::detail::AdaptiveEncoderV1<TypedBlock, TD>,
                                                  arcticdb::detail::AdaptiveEncoder<TypedBlock, TD>>;


    template<typename EncoderT>
    struct EncoderTag {
        using Encoder = EncoderT;
    };

    template<typename FunctorT>
    static auto visit_encoder(const BlockCodecImpl& codec_opts, FunctorT&& f) {
        switch (codec_opts.codec_type()) {
            case Codec::ZSTD:
                return f(EncoderTag<ZstdEncoder>());
            case Codec::LZ4:
                return f(EncoderTag<Lz4Encoder>());
        case Codec::PASS :
                return f(EncoderTag<PassthroughEncoder>());
        case Codec::ADAPTIVE :
            return f(EncoderTag<AdaptiveEncoder>());
            default:
                return f(EncoderTag<PassthroughEncoder>());
        }
    }

    static auto get_opts(const BlockCodecImpl& codec_opts, EncoderTag<Lz4Encoder>) {
        return codec_opts.lz4();
    }

    static auto get_opts(const BlockCodecImpl& codec_opts, EncoderTag<ZstdEncoder>) {
        return codec_opts.zstd();
    }

    static auto get_opts(const BlockCodecImpl& codec_opts, EncoderTag<PassthroughEncoder>) {
        return codec_opts.passthrough();
    }

    static auto get_opts(const BlockCodecImpl& codec_opts, EncoderTag<AdaptiveEncoder>) {
        return codec_opts.adaptive();
    }
};
}
