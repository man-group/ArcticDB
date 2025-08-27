/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#ifndef ARCTICDB_SEGMENT_ENCODER_H_
#error "This should only be included by codec.hpp"
#endif

#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/passthrough.hpp>
#include <arcticdb/codec/zstd.hpp>
#include <arcticdb/codec/lz4.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/magic_words.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/sparse_utils.hpp>
#include <type_traits>

namespace arcticdb {

template<typename T, typename BlockType>
void decode_block(const BlockType &block, const std::uint8_t *input, T *output) {
    ARCTICDB_SUBSAMPLE_AGG(DecodeBlock)
    std::size_t size_to_decode = block.out_bytes();
    std::size_t decoded_size = block.in_bytes();

    if (!block.has_codec()) {
        arcticdb::detail::PassthroughDecoder::decode_block<T>(input, size_to_decode, output, decoded_size);
    } else {
        std::uint32_t encoder_version = block.encoder_version();
        switch (block.codec().codec_type()) {
        case arcticdb::Codec::ZSTD:
            arcticdb::detail::ZstdDecoder::decode_block<T>(encoder_version,
                 input,
                 size_to_decode,
                 output,
                 decoded_size);
            break;
        case arcticdb::Codec::LZ4:
            arcticdb::detail::Lz4Decoder::decode_block<T>(encoder_version,
                input,
                size_to_decode,
                output,
                decoded_size);
            break;
        default:
            util::raise_rte("Unsupported block codec {}", codec_type_to_string(block.codec().codec_type()));
        }
    }
}

template<typename FieldType, class DataSink>
inline void read_shapes(
    FieldType& encoded_field,
    DataSink& data_sink,
    uint8_t const *& data_in,
    int shapes_block,
    shape_t*& shapes_out
) {
    const auto& shape = encoded_field.shapes(shapes_block);
    decode_block<shape_t>(shape, data_in, shapes_out);
    data_in += shape.out_bytes();
    shapes_out += shape.in_bytes() / sizeof(shape_t);
    data_sink.advance_shapes(shape.in_bytes());
}

template<class DataSink, typename NDArrayEncodedFieldType>
std::size_t decode_ndarray(
    const TypeDescriptor& td,
    const NDArrayEncodedFieldType& field,
    const std::uint8_t* input,
    DataSink& data_sink,
    std::optional<util::BitMagic>& bv,
    EncodingVersion encoding_version
) {
    ARCTICDB_SUBSAMPLE_AGG(DecodeNdArray)

    std::size_t read_bytes = 0;
    td.visit_tag([&](auto type_desc_tag) {
        using TD = std::decay_t<decltype(type_desc_tag)>;
        using T = typename TD::DataTypeTag::raw_type;

        const auto data_size = encoding_sizes::data_uncompressed_size(field);
        const bool is_empty_array = (data_size == 0) && type_desc_tag.dimension() > Dimension::Dim0;
        ARCTICDB_TRACE(log::version(), "Decoding ndarray of size {}", data_size);
        // Empty array types will not contain actual data, however, its sparse map should be loaded
        // so that we can distinguish None from []
        if(data_size == 0 && !is_empty_array) {
            util::check(type_desc_tag.data_type() == DataType::EMPTYVAL,
                "NDArray of type {} should not be of size 0!",
                datatype_to_str(type_desc_tag.data_type()));
            read_bytes = encoding_sizes::data_compressed_size(field);
            return;
        }

        auto data_begin = is_empty_array ? nullptr : static_cast<uint8_t*>(data_sink.allocate_data(data_size));
        util::check(is_empty_array || data_begin != nullptr, "Failed to allocate data of size {}", data_size);
        auto data_out = data_begin;
        auto data_in = input;
        auto num_blocks = field.values_size();

        ARCTICDB_TRACE(log::codec(), "Decoding ndarray with type {}, uncompressing {} ({}) bytes in {} blocks",
            td, data_size, encoding_sizes::ndarray_field_compressed_size(field), num_blocks);
        shape_t *shapes_out = nullptr;
        if constexpr(TD::DimensionTag::value != Dimension::Dim0) {
            const auto shape_size = encoding_sizes::shape_uncompressed_size(field);
            if(shape_size > 0) {
                shapes_out = data_sink.allocate_shapes(shape_size);
                if(encoding_version == EncodingVersion::V2)
                    read_shapes(field, data_sink, data_in, 0, shapes_out);
            }
        }
        for (auto block_num = 0; block_num < num_blocks; ++block_num) {
            if constexpr(TD::DimensionTag::value != Dimension::Dim0) {
                // In V1 encoding each block of values is preceded by a block of shapes.
                // In V2 encoding all shapes are put in a single block placed at the beginning of the block chain.
                if(encoding_version == EncodingVersion::V1) {
                    read_shapes(field, data_sink, data_in, block_num, shapes_out);
                }
            }

            const auto& block_info = field.values(block_num);
            ARCTICDB_TRACE(log::codec(), "Decoding block {} at pos {}", block_num, data_in - input);
            size_t block_inflated_size;
            decode_block<T>(block_info, data_in, reinterpret_cast<T *>(data_out));
            block_inflated_size = block_info.in_bytes();
            data_out += block_inflated_size;
            data_sink.advance_data(block_inflated_size);
            data_in += block_info.out_bytes();
        }

        if(field.sparse_map_bytes()) {
            util::check(!is_empty_type(type_desc_tag.data_type()), "Empty typed columns should not have sparse map");
            util::check_magic<util::BitMagicStart>(data_in);
            const auto bitmap_size = field.sparse_map_bytes() - util::combined_bit_magic_delimiters_size();
            bv = util::deserialize_bytes_to_bitmap(data_in, bitmap_size);
            util::check_magic<util::BitMagicEnd>(data_in);
            data_sink.set_allow_sparse(Sparsity::PERMITTED);
        }

        read_bytes = encoding_sizes::ndarray_field_compressed_size(field);
        util::check(data_in - input == intptr_t(read_bytes),
            "Decoding compressed size mismatch, expected decode size {} to equal total size {}", data_in - input,
             read_bytes);

        util::check(data_out - data_begin == intptr_t(data_size),
            "Decoding uncompressed size mismatch, expected position {} to be equal to data size {}",
             data_out - data_begin, data_size);
    });
    return read_bytes;
}

template<class DataSink>
std::size_t decode_field(
    const TypeDescriptor &td,
    const EncodedFieldImpl &field,
    const std::uint8_t *input,
    DataSink &data_sink,
    std::optional<util::BitMagic>& bv,
    EncodingVersion encoding_version) {
    size_t magic_size = 0u;
    if (encoding_version != EncodingVersion::V1) {
        magic_size += sizeof(ColumnMagic);
        util::check_magic<ColumnMagic>(input);
    }

    switch (field.encoding_case()) {
        case EncodedFieldType::NDARRAY:
            return decode_ndarray(td, field.ndarray(), input, data_sink, bv, encoding_version) + magic_size;
        default:
            util::raise_rte("Unsupported encoding {}", field);
    }
}

} // namespace arcticdb
