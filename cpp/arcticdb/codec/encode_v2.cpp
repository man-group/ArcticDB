/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encode_common.hpp>
#include <arcticdb/codec/typed_block_encoder_impl.hpp>
#include <arcticdb/codec/magic_words.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/codec/segment_identifier.hpp>
#include <arcticdb/codec/adaptive.hpp>
#include <arcticdb/codec/scanner.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>
#include <arcticdb/codec/encode_v2.hpp>

namespace arcticdb {
void add_bitmagic_compressed_size(
    const ColumnData& column_data,
    size_t& max_compressed_bytes,
    size_t& uncompressed_bytes
);

void encode_sparse_map(
    ColumnData& column_data,
    EncodedFieldImpl& field,
    Buffer& out,
    std::ptrdiff_t& pos
);

template<typename MagicType>
void write_magic(Buffer& buffer, std::ptrdiff_t& pos) {
    new (buffer.data() + pos) MagicType{};
    pos += sizeof(MagicType);
}

void write_frame_data(Buffer& buffer, std::ptrdiff_t& pos, const FrameDescriptor& frame_desc) {
    auto ptr = new (buffer.data() + pos) FrameDescriptor{};
    *ptr = frame_desc;
    pos += sizeof(FrameDescriptor);
}

void write_segment_descriptor(Buffer& buffer, std::ptrdiff_t& pos, const SegmentDescriptorImpl& segment_desc) {
    auto ptr = new (buffer.data() + pos) SegmentDescriptorImpl{};
    *ptr = segment_desc;
    pos += sizeof(SegmentDescriptorImpl);
}

[[nodiscard]] static TypedBlockData<ShapesBlockTDT> create_shapes_typed_block(const ColumnData& column_data) {
    static_assert(sizeof(ssize_t) == sizeof(int64_t));
    const size_t row_count = column_data.shapes()->bytes() / sizeof(shape_t);
    return {reinterpret_cast<const typename ShapesBlockTDT::DataTypeTag::raw_type*>(column_data.shapes()->data()),
            nullptr,
            column_data.shapes()->bytes(),
            row_count,
            nullptr};
}

void ColumnEncoderV2::encode(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos,
        std::optional<EncodingScanResult> values_result,
        std::optional<EncodingScanResult> shapes_result) {
    encode_shapes(column_data, field, out, pos, std::move(shapes_result));
    encode_blocks(codec_opts, column_data, field, out, pos, std::move(values_result));
    encode_sparse_map(column_data, field, out, pos);
}

void ColumnEncoderV2::encode_shapes(
        const ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos_in_buffer,
        std::optional<EncodingScanResult> scan_result) {
    // There is no need to store the shapes for a column of empty type as they will be all 0. The type handler will
    // assign 0 for the shape upon reading. There is one edge case - when we have None in the column, as it should not
    // have shape at all (since it's not an array). This is handled by the sparse map.
    if(column_data.type().dimension() == Dimension::Dim0 || is_empty_type(column_data.type().data_type()))
        return;

    TypedBlockData<ShapesBlockTDT> shapes_block = create_shapes_typed_block(column_data);
    if(scan_result) {
        using AdaptiveShapesEncoder = AdaptiveEncoder<TypedBlockData, ShapesBlockTDT>;
        AdaptiveShapesEncoder::encode_shapes(shapes_block, out, pos_in_buffer, field.shapes(0), *scan_result);
    } else {
        using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingVersion::V2>;
        ShapesEncoder::encode_shapes(codec::default_shapes_codec(), shapes_block, field, out, pos_in_buffer);
    }
}

EncodedFieldImpl* ColumnEncoderV2::add_field(
    const BlockCodecImpl& codec_opts,
    ColumnData& column_data,
    EncodedFieldCollection& fields) {
    size_t num_blocks;
    if(codec_opts.codec_type() == Codec::ADAPTIVE) {
        if(column_data.num_blocks() > 0)
            num_blocks = column_data.type().dimension() == Dimension::Dim0 ? 1 : 2;
        else
            num_blocks = 0;
    } else {
        num_blocks = column_data.num_blocks();
    }

    return fields.add_field(num_blocks);
}

void ColumnEncoderV2::encode_blocks(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos,
        std::optional<EncodingScanResult> scan_result) {
    column_data.type().visit_tag([&codec_opts, &column_data, &field, &out, &pos, &scan_result](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        if(scan_result) {
            using AdaptiveBlockEncoder = AdaptiveEncoder<TypedBlockData, TDT>;
              try {
                  AdaptiveBlockEncoder::encode_data(column_data, out, pos, field, *scan_result);
              } catch(const CompressionOverflowException& ) {
                  using RawType = TDT::DataTypeTag::raw_type;
                  auto plain_result = create_plain_result<RawType>(column_data.row_count());
                  auto& block = field.mutable_ndarray()->values(0);
                  block.mutable_codec()->mutable_adaptive()->encoding_type_ = EncodingType::PLAIN;
                  const auto size = PlainCompressor<RawType>::compress(
                      column_data,
                      reinterpret_cast<RawType*>(out.data() + pos),
                      plain_result.estimated_size_);
                  block.set_out_bytes(size);
                  pos += size;
              }
        } else {
            using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V2>;
            while (auto block = column_data.next<TDT>()) {
                if constexpr (must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                    util::check(block->nbytes() > 0, "Zero-sized block");
                    Encoder::encode_values(codec_opts, *block, field, out, pos);
                } else {
                    if (block->nbytes() > 0) {
                        Encoder::encode_values(codec_opts, *block, field, out, pos);
                    }
                }
            }
        }
    });
}

template <typename EncodingPolicyType>
void encode_string_pool_v2(
    const SegmentInMemory &in_mem_seg,
    SegmentHeader &segment_header,
    const BlockCodecImpl& codec_opts,
    Buffer &out_buffer,
    std::ptrdiff_t &pos
) {
    if (in_mem_seg.has_string_pool()) {
        ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
        auto col = in_mem_seg.string_pool_data();
        auto& encoded_field = segment_header.mutable_string_pool_field(calc_num_blocks<EncodingPolicyType>(col));
        // String pool uses default encoding for the time being
        EncodingPolicyType::ColumnEncoder::encode(codec_opts, col, encoded_field, out_buffer, pos, std::nullopt, std::nullopt);
        ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
    }
}

std::pair<size_t, size_t> ColumnEncoderV2::max_compressed_size(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data) {
    return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
        size_t max_compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
        using TDT = decltype(type_desc_tag);
        using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V2>;
        using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingVersion::V2>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        const size_t shapes_byte_count = column_data.shapes()->bytes();
        const TypedBlockData<ShapesBlockTDT> shapes_block = create_shapes_typed_block(column_data);
        max_compressed_bytes += ShapesEncoder::max_compressed_size(codec::default_shapes_codec(), shapes_block);
        uncompressed_bytes += shapes_byte_count;
        while (auto block = column_data.next<TDT>()) {
            const auto nbytes = block->nbytes();
            if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(nbytes > 0, "Zero-sized block");
                uncompressed_bytes += nbytes;
                max_compressed_bytes += Encoder::max_compressed_size(codec_opts, *block);
            } else if(nbytes > 0) {
                uncompressed_bytes += nbytes;
                max_compressed_bytes += Encoder::max_compressed_size(codec_opts, *block);
            }
        }
        add_bitmagic_compressed_size(column_data, max_compressed_bytes, uncompressed_bytes);
        return std::make_pair(uncompressed_bytes, max_compressed_bytes);
    });
}

using EncodingPolicyV2 = EncodingPolicyType<EncodingVersion::V2, ColumnEncoderV2>;

static void encode_field_descriptors(
        const SegmentInMemory& in_mem_seg,
        SegmentHeader& segment_header,
        const BlockCodecImpl& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos) {
    ARCTICDB_TRACE(log::codec(), "Encoding field descriptors to position {}", pos);
    if(!in_mem_seg.fields().empty()) {
        auto col = in_mem_seg.descriptor().fields().column_data();
        auto &encoded_field = segment_header.mutable_descriptor_field(calc_num_blocks<EncodingPolicyV2>(col));
        ColumnEncoderV2::encode(codec_opts, col, encoded_field, out_buffer, pos, std::nullopt, std::nullopt);
        ARCTICDB_TRACE(log::codec(), "Encoded field descriptors to position {}", pos);
    }
}

static void encode_index_descriptors(
    const SegmentInMemory& in_mem_seg,
    SegmentHeader& segment_header,
    const BlockCodecImpl& codec_opts,
    Buffer& out_buffer,
    std::ptrdiff_t& pos) {
    ARCTICDB_TRACE(log::codec(), "Encoding index descriptors to position {}", pos);

    if (in_mem_seg.has_index_descriptor()) {
        const auto& tsd = in_mem_seg.index_descriptor();
        write_frame_data(out_buffer, pos, *tsd.frame_data_);
        write_magic<SegmentDescriptorMagic>(out_buffer, pos);
        write_segment_descriptor(out_buffer, pos, *tsd.segment_desc_);
        write_identifier(out_buffer, pos, tsd.stream_id_);

        ARCTICDB_TRACE(log::codec(), "Encoding index fields descriptors to position {}", pos);
        auto index_field_data = tsd.fields().column_data();
        auto& index_field = segment_header.mutable_index_descriptor_field(calc_num_blocks<EncodingPolicyV2>(index_field_data));

        ColumnEncoderV2::encode(codec_opts, index_field_data, index_field, out_buffer, pos, std::nullopt, std::nullopt);
        ARCTICDB_TRACE(log::codec(), "Encoded index field descriptors to position {}", pos);
    }
}

[[nodiscard]] size_t num_output_blocks(Codec codec_type, const Column& col) {
    return codec_type == Codec::ADAPTIVE ? 1 : col.num_blocks();
}

[[nodiscard]] size_t calc_column_blocks_size(
        const Column& col,
        const std::optional<BlockCodecImpl>& codec) {
    size_t bytes = EncodedFieldImpl::Size;
    if(col.type().dimension() != entity::Dimension::Dim0)
        bytes += sizeof(EncodedBlock);

    bytes += sizeof(EncodedBlock) * num_output_blocks(codec->codec_type(), col);
    ARCTICDB_TRACE(log::version(), "Encoded block size: {} + shapes({}) + {} * {} = {}",
        EncodedFieldImpl::Size,
        col.type().dimension() != entity::Dimension::Dim0 ? sizeof(EncodedBlock) : 0u,
        sizeof(EncodedBlock),
        col.num_blocks(),
        bytes);

    return bytes;
}

[[nodiscard]] static size_t encoded_blocks_size(
        const SegmentInMemory& in_mem_seg,
        const BlockCodecImpl& codec) {
    size_t bytes = 0;
    for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
        const auto& col = in_mem_seg.column(position_t(c));
        bytes += calc_column_blocks_size(col, codec);
    }
    bytes += sizeof(EncodedBlock);
    return bytes;
}

static void calc_encoded_blocks_size(
        const SegmentInMemory& in_mem_seg,
        const BlockCodecImpl column_codec_opts, // Codec compressing the columns
        const BlockCodecImpl& meta_codec_opts,  // Codec compressing the encoding blocks buffer
        SizeResult& result) {
    result.encoded_blocks_bytes_ = static_cast<shape_t>(encoded_blocks_size(in_mem_seg, column_codec_opts));
    result.uncompressed_bytes_ += result.encoded_blocks_bytes_;
    result.max_compressed_bytes_ += BytesEncoder<EncodingPolicyV2>::max_compressed_size(meta_codec_opts, result.encoded_blocks_bytes_);
}

static void add_stream_descriptor_data_size(SizeResult& result, const StreamId& stream_id) {
    result.max_compressed_bytes_ += sizeof(FrameDescriptor);
    result.uncompressed_bytes_ += sizeof(FrameDescriptor);
    const auto identifier_size = identifier_bytes(stream_id);
    result.max_compressed_bytes_ += identifier_size;
    result.uncompressed_bytes_ += identifier_size;
}

static void calc_stream_descriptor_fields_size(
        const SegmentInMemory& in_mem_seg,
        const BlockCodecImpl& codec_opts,
        SizeResult& result) {
    auto segment_fields = in_mem_seg.descriptor().fields().column_data();
    const auto [uncompressed, required] = ColumnEncoderV2::max_compressed_size(codec_opts, segment_fields);
    result.uncompressed_bytes_ += uncompressed;
    result.max_compressed_bytes_ += required;
    add_stream_descriptor_data_size(result, in_mem_seg.descriptor().id());

    if(in_mem_seg.has_index_descriptor()) {
        const auto& tsd = in_mem_seg.index_descriptor();
        auto index_field_data = tsd.fields().column_data();
        const auto [idx_uncompressed, idx_required] = ColumnEncoderV2::max_compressed_size(codec_opts, index_field_data);
        result.uncompressed_bytes_ += idx_uncompressed;
        result.max_compressed_bytes_ += idx_required;
        add_stream_descriptor_data_size(result, tsd.stream_id_);
    }
}

[[nodiscard]] SizeResult max_compressed_size_v2(
        const SegmentInMemory& in_mem_seg,
        const BlockCodecImpl& column_codec_opts,
        SegmentScanResults& adaptive_encodings) {
    ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
    SizeResult result{};
    auto non_data_codec_opts = codec::default_lz4_codec();
    result.max_compressed_bytes_ += sizeof(MetadataMagic);
    calc_metadata_size<EncodingPolicyV2>(in_mem_seg, non_data_codec_opts, result);
    result.max_compressed_bytes_ += sizeof(DescriptorFieldsMagic);
    result.max_compressed_bytes_ += sizeof(IndexMagic);
    calc_stream_descriptor_fields_size(in_mem_seg, non_data_codec_opts, result);
    result.max_compressed_bytes_ += sizeof(EncodedMagic);
    calc_encoded_blocks_size(in_mem_seg, column_codec_opts, non_data_codec_opts, result);

    if(in_mem_seg.row_count() > 0) {
        result.max_compressed_bytes_ += sizeof(ColumnMagic) * in_mem_seg.descriptor().field_count();
        if(column_codec_opts.codec_type() == Codec::ADAPTIVE) {
            util::check(!adaptive_encodings.empty(), "Expected adaptive encodings for codec");
            resolve_adaptive_encodings_size(in_mem_seg, adaptive_encodings, result);
        } else {
            calc_columns_size<EncodingPolicyV2>(in_mem_seg, column_codec_opts, result);
        }

        result.max_compressed_bytes_ += sizeof(StringPoolMagic);
        calc_string_pool_size<EncodingPolicyV2>(in_mem_seg, non_data_codec_opts, result);
    }

    ARCTICDB_TRACE(log::codec(), "Max compressed size {}", result.max_compressed_bytes_);
    return result;
}

static void encode_encoded_fields(
        SegmentHeader& segment_header,
        const BlockCodecImpl& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos,
        EncodedFieldCollection&& encoded_fields) {
    ARCTICDB_DEBUG(log::codec(), "Encoding encoded blocks to position {}", pos);

    segment_header.set_footer_offset(pos);
    write_magic<EncodedMagic>(out_buffer, pos);
    Column encoded_fields_column(encoded_fields_type_desc(), Sparsity::NOT_PERMITTED, encoded_fields.release_data());
    auto data = encoded_fields_column.data();
    auto& encoded_field = segment_header.mutable_column_fields(calc_num_blocks<EncodingPolicyV2>(data));
    ColumnEncoderV2::encode(codec_opts, data, encoded_field, out_buffer, pos, std::nullopt, std::nullopt);
    ARCTICDB_DEBUG(log::codec(), "Encoded encoded blocks to position {}", pos);
}

[[nodiscard]] Segment encode_v2(
        SegmentInMemory&& s,
        const BlockCodecImpl& codec_opts) {
    ARCTICDB_SAMPLE(EncodeSegment, 0)
    auto in_mem_seg = std::move(s);

    if(ConfigsMap().instance()->get_int("Statistics.GenerateOnWrite", 0) == 1 || codec_opts.type_ == Codec::ADAPTIVE)
        in_mem_seg.calculate_statistics();

    if(in_mem_seg.has_index_descriptor()) {
        google::protobuf::Any any;
        util::pack_to_any(in_mem_seg.index_descriptor().proto(), any);
        in_mem_seg.set_metadata(std::move(any));
    }

    auto non_data_codec_opts = codec::default_lz4_codec();
    SegmentHeader segment_header{EncodingVersion::V2};
    segment_header.set_compacted(in_mem_seg.compacted());

    SegmentScanResults scan_results;
    if(codec_opts.codec_type() == Codec::ADAPTIVE)
        scan_results = get_encodings(in_mem_seg);

    std::ptrdiff_t pos = 0;
    auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(in_mem_seg, codec_opts, scan_results);
    ARCTICDB_TRACE(log::codec(), "Estimated max buffer requirement: {}", max_compressed_size);
    const auto preamble = SegmentHeader::required_bytes(in_mem_seg);
    auto out_buffer = std::make_shared<Buffer>(max_compressed_size + encoded_buffer_size, preamble);
    ARCTICDB_TRACE(log::codec(), "Encoding descriptor: {}", in_mem_seg.descriptor());

    const auto& descriptor = in_mem_seg.descriptor();
    auto descriptor_data = descriptor.data_ptr();
    descriptor_data->uncompressed_bytes_ = uncompressed_size;

    write_magic<MetadataMagic>(*out_buffer, pos);
    encode_metadata<EncodingPolicyV2>(in_mem_seg, segment_header, non_data_codec_opts, *out_buffer, pos);

    write_magic<SegmentDescriptorMagic>(*out_buffer, pos);
    write_segment_descriptor(*out_buffer, pos, descriptor.data());
    write_identifier(*out_buffer, pos, descriptor.id());
    write_magic<DescriptorFieldsMagic>(*out_buffer, pos);
    encode_field_descriptors(in_mem_seg, segment_header, non_data_codec_opts, *out_buffer, pos);

    write_magic<IndexMagic>(*out_buffer, pos);
    encode_index_descriptors(in_mem_seg, segment_header, non_data_codec_opts, *out_buffer, pos);

    EncodedFieldCollection encoded_fields;
    ColumnEncoderV2 encoder;
    if(in_mem_seg.row_count() > 0) {
        encoded_fields.reserve(encoded_buffer_size, in_mem_seg.num_columns());
        ARCTICDB_TRACE(log::codec(), "Encoding fields");
        for (std::size_t column_index = 0; column_index < in_mem_seg.num_columns(); ++column_index) {
            write_magic<ColumnMagic>(*out_buffer, pos);
            const auto& column = in_mem_seg.column(position_t(column_index));
            auto column_data = column.data();
            auto* column_field = encoder.add_field(codec_opts, column_data, encoded_fields);
            ARCTICDB_TRACE(log::codec(),"Beginning encoding of column {}: ({}) at position {}", column_index, in_mem_seg.descriptor().field(column_index).name(), pos);

            if(column_data.num_blocks() > 0) {
                auto value_scan_result = codec_opts.codec_type() == Codec::ADAPTIVE ? std::make_optional(scan_results.value(position_t(column_index)).first()) : std::nullopt;
                auto shapes_scan_result = column_data.type().dimension() != Dimension::Dim0 ? std::make_optional(scan_results.shape(column_index).first()) : std::nullopt;
                if(column.has_statistics())
                    column_field->set_statistics(column.get_statistics());

                encoder.encode(codec_opts, column_data, *column_field, *out_buffer, pos, value_scan_result, shapes_scan_result);
                ARCTICDB_TRACE(log::codec(), "Encoded column {}: ({}) to position {}", column_index, in_mem_seg.descriptor().field(column_index).name(), pos);
            } else {
                util::check(!must_contain_data(column_data.type()), "Column {} of type {} contains no blocks", column_index, column_data.type());
                auto* ndarray = column_field->mutable_ndarray();
                ndarray->set_items_count(0);
            }
            util::check(pos <= static_cast<ptrdiff_t>(max_compressed_size), "Size overflow: {} > {}", pos, max_compressed_size);
        }
        write_magic<StringPoolMagic>(*out_buffer, pos);
        encode_string_pool_v2<EncodingPolicyV2>(in_mem_seg, segment_header, non_data_codec_opts, *out_buffer, pos);
        encode_encoded_fields(segment_header, non_data_codec_opts, *out_buffer, pos, std::move(encoded_fields));
    } else {
        segment_header.set_footer_offset(pos);
    }

    out_buffer->set_bytes(pos);
    descriptor_data->compressed_bytes_ = pos;
    descriptor_data->row_count_ = in_mem_seg.row_count();

#ifdef DEBUG_BUILD
    segment_header.validate();
#endif

    ARCTICDB_TRACE(log::codec(), "Encoded header: {}", segment_header);
    const auto& desc = in_mem_seg.descriptor();
    return Segment::initialize(std::move(segment_header), std::move(out_buffer), descriptor_data, desc.fields_ptr(), desc.id());
}

}  //namespace arcticdb
