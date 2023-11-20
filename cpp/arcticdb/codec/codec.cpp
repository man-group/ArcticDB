/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb//codec/typed_block_encoder_impl.hpp>


#include <string>
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace arcticdb {

/// @brief Write the sparse map to the out buffer
/// Bitmagic achieves the theoretical best compression for booleans. Adding additional encoding (lz4, zstd, etc...)
/// will not improve anything and in fact it might worsen the encoding.
[[nodiscard]] static size_t encode_bitmap(const util::BitMagic& sparse_map, Buffer& out, std::ptrdiff_t& pos) {
    ARCTICDB_DEBUG(log::version(), "Encoding sparse map of count: {}", sparse_map.count());
    bm::serializer<bm::bvector<> > bvs;
    bm::serializer<bm::bvector<> >::buffer sbuf;
    bvs.serialize(sparse_map, sbuf);
    auto sz = sbuf.size();
    auto total_sz = sz + sizeof(util::BitMagicStart) + sizeof(util::BitMagicEnd);
    out.assert_size(pos + total_sz);

    uint8_t* target = out.data() + pos;
    util::write_magic<util::BitMagicStart>(target);
    std::memcpy(target, sbuf.data(), sz);
    target += sz;
    util::write_magic<util::BitMagicEnd>(target);
    pos = pos + static_cast<ptrdiff_t>(total_sz);
    return total_sz;
}

static ShapesBlock create_shapes_typed_block(const ColumnData& column_data) {
    static_assert(std::is_same_v<ShapesBlockTDT::DataTypeTag::raw_type, shape_t>,
        "Shape block type is not compatible");
    const size_t row_count = column_data.shapes()->bytes() / sizeof(shape_t);
    return {reinterpret_cast<const typename ShapesBlockTDT::DataTypeTag::raw_type*>(column_data.shapes()->data()),
            nullptr,
            column_data.shapes()->bytes(),
            row_count,
            nullptr};
}

static void encode_sparse_map_if_available(
	ColumnData& column_data,
	MutableVariantField variant_field,
	Buffer& out,
	std::ptrdiff_t& pos
) {
	if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0)   {
		ARCTICDB_DEBUG(log::codec(), "Sparse map count = {} pos = {}", column_data.bit_vector()->count(), pos);
		const size_t sparse_bm_bytes = encode_bitmap(*column_data.bit_vector(), out, pos);
		util::variant_match(variant_field, [&](auto field) {
			field->mutable_ndarray()->set_sparse_map_bytes(static_cast<int>(sparse_bm_bytes));
		});
	}
}

static void add_bitmagic_compressed_size_if_any(
    const ColumnData& column_data,
    size_t& max_compressed_bytes,
    size_t& uncompressed_bytes
) {
    if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0)   {
        bm::serializer<util::BitMagic>::statistics_type stat{};
        column_data.bit_vector()->calc_stat(&stat);
        uncompressed_bytes += stat.memory_used;
        max_compressed_bytes += stat.max_serialize_mem;
    }
}

arcticdb::proto::encoding::VariantCodec shapes_encoding_opts() {
    return codec::default_lz4_codec();
}

std::pair<size_t, size_t> ColumnEncoder_v1::max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data) {
    return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
        size_t max_compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
        using TDT = decltype(type_desc_tag);
        using Encoder = BlockEncoder<TDT, EncodingVersion::V1>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        while (auto block = column_data.next<TDT>()) {
            const auto nbytes = block.value().nbytes();
            if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(nbytes > 0, "Zero-sized block");
            }
	        uncompressed_bytes += nbytes;
	        // For the empty type the column will contain 0 size of user data however the encoder might need add some
            // encoder specific data to the buffer, thus the uncompressed size will be 0 but the max_compressed_bytes
            // might be non-zero.
            max_compressed_bytes += Encoder::max_compressed_size(codec_opts, *block);
        }
        add_bitmagic_compressed_size_if_any(column_data, uncompressed_bytes, max_compressed_bytes);
        return std::make_pair(uncompressed_bytes, max_compressed_bytes);
    });
}

void ColumnEncoder_v1::encode(
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    ColumnData& column_data,
    MutableVariantField variant_field,
    Buffer& out,
    std::ptrdiff_t& pos
) {
    column_data.type().visit_tag([&](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        using Encoder = BlockEncoder<TDT, EncodingVersion::V1>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        while (auto block = column_data.next<TDT>()) {
            if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(block.value().nbytes() > 0, "Zero-sized block");
            }
            std::visit([&](auto field){
                Encoder::encode(codec_opts, block.value(), *field, out, pos);
            }, variant_field);
        }
    });
    encode_sparse_map_if_available(column_data, variant_field, out, pos);
}

void ColumnEncoder_v2::encode(
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    ColumnData& column_data,
    MutableVariantField variant_field,
    Buffer& out,
    std::ptrdiff_t& pos
) {
    encode_shapes(column_data, variant_field, out, pos);
    encode_blocks(codec_opts, column_data, variant_field, out, pos);
    encode_sparse_map_if_available(column_data, variant_field, out, pos);
}

void ColumnEncoder_v2::encode_shapes(
    const ColumnData& column_data,
    MutableVariantField variant_field,
    Buffer& out,
    std::ptrdiff_t& pos_in_buffer
) {
    // There is no need to store the shapes for a column of empty type as they will be all 0. The type handler will
    // assign 0 for the shape upon reading. There is one edge case - when we have None in the column, as it should not
    // have shape at all (since it's not an array). This is handled by the sparse map.
    if(column_data.type().dimension() != Dimension::Dim0 && !is_empty_type(column_data.type().data_type())) {
        ShapesBlock shapes_block = create_shapes_typed_block(column_data);
        util::variant_match(variant_field, [&](auto field){
            ShapesEncoder::encode_shapes(shapes_encoding_opts(), shapes_block, *field, out, pos_in_buffer);
        });
    }
}

void ColumnEncoder_v2::encode_blocks(
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    ColumnData& column_data,
    MutableVariantField variant_field,
    Buffer& out,
    std::ptrdiff_t& pos
) {
    column_data.type().visit_tag([&](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        while (auto block = column_data.next<TDT>()) {
            if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(block.value().nbytes() > 0, "Zero-sized block");
            }
            util::variant_match(variant_field, [&](auto field) {
                Encoder<TDT>::encode_values(codec_opts, block.value(), *field, out, pos);
            });
        }
    });
}

std::pair<size_t, size_t> ColumnEncoder_v2::max_compressed_size(
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    ColumnData& column_data
) {
    return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
        size_t max_compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
        using TDT = decltype(type_desc_tag);
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        const size_t shapes_byte_count = column_data.shapes()->bytes();
        const ShapesBlock shapes_block = create_shapes_typed_block(column_data);
        max_compressed_bytes += ShapesEncoder::max_compressed_size(shapes_encoding_opts(), shapes_block);
        uncompressed_bytes += shapes_byte_count;
        while (auto block = column_data.next<TDT>()) {
            const auto nbytes = block.value().nbytes();
            if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(nbytes > 0, "Zero-sized block");
            }
            uncompressed_bytes += nbytes;
            // For the empty type the column will contain 0 size of user data however the encoder might need add some
            // encoder specific data to the buffer, thus the uncompressed size will be 0 but the max_compressed_bytes
            // might be non-zero.
            max_compressed_bytes += Encoder<TDT>::max_compressed_size(codec_opts, block.value());
        }
        add_bitmagic_compressed_size_if_any(column_data, uncompressed_bytes, max_compressed_bytes);
        return std::make_pair(uncompressed_bytes, max_compressed_bytes);
    });
}

template<EncodingVersion v>
struct BytesEncoder {
    using BytesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;
    using Encoder = BlockEncoder<BytesTypeDescriptorTag, v>;
    using BytesBlock = TypedBlockData<BytesTypeDescriptorTag>;
    using ShapesEncoder = BlockEncoder<ShapesBlockTDT, v>;

    template<typename EncodedFieldType>
    static void encode(
        const ChunkedBuffer& data,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos,
        EncodedFieldType* encoded_field
    ) {
        const shape_t bytes_count = static_cast<shape_t>(data.bytes());
        if constexpr(v == EncodingVersion::V1) {
            auto typed_block = BytesBlock(
                data.data(),
                &bytes_count,
                bytes_count,
                1u,
                data.block_and_offset(0).block_);
            Encoder::encode(codec_opts, typed_block, *encoded_field, out_buffer, pos);
        } else if constexpr(v == EncodingVersion::V2) {
            const size_t row_count = 1;
            auto shapes_block = ShapesBlock(&bytes_count,
                nullptr,
                sizeof(shape_t),
                row_count,
                data.block_and_offset(0).block_);
            auto data_block = BytesBlock(data.data(),
                &bytes_count,
                bytes_count,
                row_count,
                data.block_and_offset(0).block_);
            ShapesEncoder::encode_shapes(shapes_encoding_opts(), shapes_block, *encoded_field, out_buffer, pos);
            Encoder::encode_values(codec_opts, data_block, *encoded_field, out_buffer, pos);
            auto* field_nd_array = encoded_field->mutable_ndarray();
            const auto total_items_count = field_nd_array->items_count() + row_count;
            field_nd_array->set_items_count(total_items_count);
        } else {
            static_assert(std::is_same_v<decltype(v), void>, "Unknown encoding version");
        }
    }

    static size_t max_compressed_size(const arcticdb::proto::encoding::VariantCodec& codec_opts, shape_t data_size) {
        const shape_t shapes_bytes = sizeof(shape_t);
        const auto values_block = BytesBlock(data_size, &data_size);
        if constexpr(v == EncodingVersion::V1) {
            const auto shapes_block = BytesBlock(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                   Encoder::max_compressed_size(codec_opts, shapes_block);
        } else if constexpr(v == EncodingVersion::V2) {
            const auto shapes_block = ShapesBlock(shapes_bytes, &shapes_bytes);
            return Encoder::max_compressed_size(codec_opts, values_block) +
                   ShapesEncoder::max_compressed_size(shapes_encoding_opts(), shapes_block);
        } else {
            static_assert(std::is_same_v<decltype(v), void>, "Unknown encoding version");
        }
    }
};

constexpr TypeDescriptor metadata_type_desc() {
    return TypeDescriptor{
        DataType::UINT8, Dimension::Dim1
    };
}

constexpr TypeDescriptor encoded_blocks_type_desc() {
    return TypeDescriptor{
        DataType::UINT8, Dimension::Dim1
    };
}

size_t calc_column_blocks_size(const Column& col) {
    size_t bytes = EncodedField::Size;
    if(col.type().dimension() != entity::Dimension::Dim0)
        bytes += sizeof(EncodedBlock);

    bytes += sizeof(EncodedBlock) * col.num_blocks();
    ARCTICDB_DEBUG(log::version(), "Encoded block size: {} + shapes({}) + {} * {} = {}",
                 EncodedField::Size,
                 col.type().dimension() != entity::Dimension::Dim0 ? sizeof(EncodedBlock) : 0u,
                 sizeof(EncodedBlock),
                 col.num_blocks(),
                 bytes);

    return bytes;
}

size_t encoded_blocks_size(const SegmentInMemory& in_mem_seg) {
    size_t bytes = 0;
    for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
        const auto& col = in_mem_seg.column(position_t(c));
        bytes += calc_column_blocks_size(col);
    }
    return bytes;
}

struct SizeResult {
    size_t max_compressed_bytes_;
    size_t uncompressed_bytes_;
    shape_t encoded_blocks_bytes_;
};

template<EncodingVersion v>
void calc_metadata_size(
    const SegmentInMemory& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    SizeResult& result
    ) {
    if (in_mem_seg.metadata()) {
        const auto metadata_bytes = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        result.uncompressed_bytes_ += metadata_bytes + sizeof(shape_t);
        result.max_compressed_bytes_ += BytesEncoder<v>::max_compressed_size(codec_opts, metadata_bytes);
        ARCTICDB_TRACE(log::codec(), "Metadata requires {} max_compressed_bytes", result.max_compressed_bytes_);
    }
}

template<EncodingVersion v>
void calc_columns_size(
    const SegmentInMemory& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    SizeResult& result
    ) {
    ColumnEncoder<v> encoder;
    for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
        auto column_data = in_mem_seg.column_data(c);
        const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, column_data);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(log::codec(), "Column {} requires {} max_compressed_bytes, total {}", c, required, result.max_compressed_bytes_);
    }
}

template<EncodingVersion v>
void calc_string_pool_size(
    const SegmentInMemory& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    SizeResult& result
    ) {
    if (in_mem_seg.has_string_pool()) {
        ColumnEncoder<v> encoder;
        auto string_col = in_mem_seg.string_pool_data();
        const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, string_col);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;
        ARCTICDB_TRACE(log::codec(), "String pool requires {} max_compressed_bytes, total {}", required, result.max_compressed_bytes_);
    }
}

SizeResult max_compressed_size_v1(const SegmentInMemory &in_mem_seg, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
    SizeResult result{};
    calc_metadata_size<EncodingVersion::V1>(in_mem_seg, codec_opts, result);

    if(in_mem_seg.row_count() > 0) {
        calc_columns_size<EncodingVersion::V1>(in_mem_seg, codec_opts, result);
        calc_string_pool_size<EncodingVersion::V1>(in_mem_seg, codec_opts, result);
    }
    ARCTICDB_TRACE(log::codec(), "Max compressed size {}", result.max_compressed_bytes_);
    return result;
}

void calc_encoded_blocks_size(
    const SegmentInMemory& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    SizeResult& result
    ) {
    result.encoded_blocks_bytes_ = static_cast<shape_t>(encoded_blocks_size(in_mem_seg));
    result.uncompressed_bytes_ += result.encoded_blocks_bytes_;
    result.max_compressed_bytes_ += BytesEncoder<EncodingVersion::V2>::max_compressed_size(codec_opts, result.encoded_blocks_bytes_);
}

template<EncodingVersion v, typename = std::enable_if_t<v == EncodingVersion::V2>>
void calc_stream_descriptor_fields_size(
    const SegmentInMemory& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    SizeResult& result
    ) {
    ColumnEncoder<v> encoder;
    auto segment_fields = in_mem_seg.descriptor().fields().column_data();
    const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, segment_fields);
    result.uncompressed_bytes_ += uncompressed;
    result.max_compressed_bytes_ += required;

    // Calculate index fields size
    if(in_mem_seg.index_fields()) {
        auto index_field_data = in_mem_seg.index_fields()->column_data();
        const auto [idx_uncompressed, idx_required] = encoder.max_compressed_size(codec_opts, index_field_data);
        result.uncompressed_bytes_ += idx_uncompressed;
        result.max_compressed_bytes_ += idx_required;
    }
}

SizeResult max_compressed_size_v2(const SegmentInMemory &in_mem_seg, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
    SizeResult result{};
    result.max_compressed_bytes_ += sizeof(MetadataMagic);
    calc_metadata_size<EncodingVersion::V2>(in_mem_seg, codec_opts, result);
    result.max_compressed_bytes_ += sizeof(DescriptorMagic);
    result.max_compressed_bytes_ += sizeof(IndexMagic);
    calc_stream_descriptor_fields_size<EncodingVersion::V2>(in_mem_seg, codec_opts, result);
    result.max_compressed_bytes_ += sizeof(EncodedMagic);
    calc_encoded_blocks_size(in_mem_seg, codec_opts, result);

    // Calculate fields collection size
    if(in_mem_seg.row_count() > 0) {
        result.max_compressed_bytes_ += sizeof(ColumnMagic) * in_mem_seg.descriptor().field_count();
        calc_columns_size<EncodingVersion::V2>(in_mem_seg, codec_opts, result);
        result.max_compressed_bytes_ += sizeof(StringPoolMagic);
        calc_string_pool_size<EncodingVersion::V2>(in_mem_seg, codec_opts, result);
    }
    ARCTICDB_TRACE(log::codec(), "Max compressed size {}", result.max_compressed_bytes_);
    return result;
}

template<EncodingVersion v>
void encode_metadata(
    const SegmentInMemory& in_mem_seg,
    arcticdb::proto::encoding::SegmentHeader& segment_header,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    Buffer& out_buffer,
    std::ptrdiff_t& pos
    ) {
    if (in_mem_seg.metadata()) {
        const auto bytes_count = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        ARCTICDB_TRACE(log::codec(), "Encoding {} bytes of metadata", bytes_count);
        auto encoded_field = segment_header.mutable_metadata_field();

        constexpr int max_stack_alloc = 1 << 11;
        bool malloced{false};
        uint8_t* meta_ptr{nullptr};
        if(bytes_count > max_stack_alloc) {
            meta_ptr = reinterpret_cast<uint8_t*>(malloc(bytes_count));
            malloced = true;
        } else {
            meta_ptr = reinterpret_cast<uint8_t*>(alloca(bytes_count));
        }
        ChunkedBuffer meta_buffer;
        meta_buffer.add_external_block(meta_ptr, bytes_count, 0u);
        google::protobuf::io::ArrayOutputStream aos(&meta_buffer[0], static_cast<int>(bytes_count));
        in_mem_seg.metadata()->SerializeToZeroCopyStream(&aos);
        BytesEncoder<v>::encode(meta_buffer, codec_opts, out_buffer, pos, encoded_field);
        ARCTICDB_DEBUG(log::codec(), "Encoded metadata to position {}", pos);
        if(malloced)
            free(meta_ptr);
    } else {
        ARCTICDB_DEBUG(log::codec(), "Not encoding any metadata");
    }
}

template<EncodingVersion v>
void encode_string_pool(
    const SegmentInMemory& in_mem_seg,
    arcticdb::proto::encoding::SegmentHeader& segment_header,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    Buffer& out_buffer,
    std::ptrdiff_t& pos
    ) {
    if (in_mem_seg.has_string_pool()) {
        ColumnEncoder<v> encoder;
        ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
        auto *encoded_field = segment_header.mutable_string_pool_field();
        auto col = in_mem_seg.string_pool_data();
        encoder.encode(codec_opts, col, encoded_field, out_buffer, pos);
        ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
    }
}

template<EncodingVersion v, typename = std::enable_if_t<v == EncodingVersion::V2>>
void encode_field_descriptors(
    const SegmentInMemory& in_mem_seg,
    arcticdb::proto::encoding::SegmentHeader& segment_header,
    const arcticdb::proto::encoding::VariantCodec& codec_opts,
    Buffer& out_buffer,
    std::ptrdiff_t& pos
    ) {
    ColumnEncoder<v> encoder;
    ARCTICDB_TRACE(log::codec(), "Encoding field descriptors to position {}", pos);
    auto *encoded_field = segment_header.mutable_descriptor_field();
    auto col = in_mem_seg.descriptor().fields().column_data();
    encoder.encode(codec_opts, col, encoded_field, out_buffer, pos);
    ARCTICDB_TRACE(log::codec(), "Encoded field descriptors to position {}", pos);

    write_magic<IndexMagic>(out_buffer, pos);
    if (in_mem_seg.index_fields()) {
        ARCTICDB_TRACE(log::codec(), "Encoding index fields descriptors to position {}", pos);
        auto *index_field = segment_header.mutable_index_descriptor_field();
        auto index_col = in_mem_seg.index_fields()->column_data();
        encoder.encode(codec_opts, index_col, index_field, out_buffer, pos);
        ARCTICDB_TRACE(log::codec(), "Encoded index field descriptors to position {}", pos);
    }
}

void encode_encoded_fields(
        arcticdb::proto::encoding::SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos,
        const ChunkedBuffer& encoded_blocks_buffer
        ) {

    ARCTICDB_TRACE(log::codec(), "Encoding encoded blocks to position {}", pos);
    auto encoded_field = segment_header.mutable_column_fields();
    encoded_field->set_offset(static_cast<uint32_t>(pos));
    write_magic<EncodedMagic>(out_buffer, pos);
	BytesEncoder<EncodingVersion::V2>::encode(encoded_blocks_buffer, codec_opts, out_buffer, pos, encoded_field);
    ARCTICDB_TRACE(log::codec(), "Encoded encoded blocks to position {}", pos);
}

Segment encode_v2(SegmentInMemory&& s, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    ARCTICDB_SAMPLE(EncodeSegment, 0)

    auto in_mem_seg = std::move(s);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto segment_header = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    auto& seg_descriptor = in_mem_seg.descriptor();
    *segment_header->mutable_stream_descriptor() = std::move(seg_descriptor.mutable_proto());
    segment_header->set_compacted(in_mem_seg.compacted());
    segment_header->set_encoding_version(static_cast<uint16_t>(EncodingVersion::V2));

    std::ptrdiff_t pos = 0;
    static auto block_to_header_ratio = ConfigsMap::instance()->get_int("Codec.EstimatedHeaderRatio", 75);
    const auto preamble = in_mem_seg.num_blocks() * block_to_header_ratio;
    auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v2(in_mem_seg, codec_opts);
    ARCTICDB_TRACE(log::codec(), "Estimated max buffer requirement: {}", max_compressed_size);
    auto out_buffer = std::make_shared<Buffer>(max_compressed_size + encoded_buffer_size, preamble);
    ARCTICDB_TRACE(log::codec(), "Encoding descriptor: {}", segment_header->stream_descriptor().DebugString());
    auto *tsd = segment_header->mutable_stream_descriptor();
    tsd->set_in_bytes(uncompressed_size);

    write_magic<MetadataMagic>(*out_buffer, pos);
    encode_metadata<EncodingVersion::V2>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);
    write_magic<DescriptorMagic>(*out_buffer, pos);
    encode_field_descriptors<EncodingVersion::V2>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);

    auto encoded_fields_buffer = ChunkedBuffer::presized(static_cast<size_t>(encoded_buffer_size));
    auto encoded_field_pos = 0u;
    ColumnEncoder<EncodingVersion::V2> encoder;
    if(in_mem_seg.row_count() > 0) {
        ARCTICDB_TRACE(log::codec(), "Encoding fields");
        for (std::size_t column_index = 0; column_index < in_mem_seg.num_columns(); ++column_index) {
            write_magic<ColumnMagic>(*out_buffer, pos);
            auto column_field = new(encoded_fields_buffer.data() + encoded_field_pos) EncodedField;
            ARCTICDB_TRACE(log::codec(),"Beginning encoding of column {}: ({}) to position {}", column_index, in_mem_seg.descriptor().field(column_index).name(), pos);
            auto column_data = in_mem_seg.column_data(column_index);
            encoder.encode(codec_opts, column_data, column_field, *out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded column {}: ({}) to position {}", column_index, in_mem_seg.descriptor().field(column_index).name(), pos);
            encoded_field_pos += encoded_field_bytes(*column_field);
            util::check(encoded_field_pos <= encoded_fields_buffer.bytes(),
                "Encoded field buffer overflow {} > {}",
                 encoded_field_pos,
                 encoded_fields_buffer.bytes());
        }
        auto field_here ARCTICDB_UNUSED = reinterpret_cast<EncodedField*>(encoded_fields_buffer.data());
        write_magic<StringPoolMagic>(*out_buffer, pos);
        encode_string_pool<EncodingVersion::V2>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);
    }

    auto field_before ARCTICDB_UNUSED = reinterpret_cast<EncodedField*>(encoded_fields_buffer.data());
    encode_encoded_fields(*segment_header, codec_opts, *out_buffer, pos, encoded_fields_buffer);
    auto field ARCTICDB_UNUSED = reinterpret_cast<EncodedField*>(encoded_fields_buffer.data());
    out_buffer->set_bytes(pos);
    tsd->set_out_bytes(pos);

    ARCTICDB_DEBUG(log::codec(), "Encoded header: {}", tsd->DebugString());
    ARCTICDB_DEBUG(log::codec(), "Block count {} header size {} ratio {}",
        in_mem_seg.num_blocks(), segment_header->ByteSizeLong(),
        in_mem_seg.num_blocks() ? segment_header->ByteSizeLong() / in_mem_seg.num_blocks() : 0);
    return {std::move(arena), segment_header, std::move(out_buffer), seg_descriptor.fields_ptr()};
}

Segment encode_v1(SegmentInMemory&& s, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    /*
     * This takes an in memory segment with all the metadata, column tensors etc., loops through each column
     * and based on the type of the column, calls the typed block encoder for that column.
     */
    ARCTICDB_SAMPLE(EncodeSegment, 0)
    auto in_mem_seg = std::move(s);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto segment_header = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    *segment_header->mutable_stream_descriptor() = in_mem_seg.descriptor().copy_to_proto();
    segment_header->set_compacted(in_mem_seg.compacted());
    std::ptrdiff_t pos = 0;
    static auto block_to_header_ratio = ConfigsMap::instance()->get_int("Codec.EstimatedHeaderRatio", 75);
    const auto preamble = in_mem_seg.num_blocks() * block_to_header_ratio;
    auto [max_compressed_size, uncompressed_size, encoded_blocks_bytes] = max_compressed_size_v1(in_mem_seg, codec_opts);
    ARCTICDB_TRACE(log::codec(), "Estimated max buffer requirement: {}", max_compressed_size);
    auto out_buffer = std::make_shared<Buffer>(max_compressed_size, preamble);
    ColumnEncoder<EncodingVersion::V1> encoder;

    ARCTICDB_TRACE(log::codec(), "Encoding descriptor: {}", segment_header->stream_descriptor().DebugString());
    auto *tsd = segment_header->mutable_stream_descriptor();
    tsd->set_in_bytes(uncompressed_size);

    encode_metadata<EncodingVersion::V1>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);

    if(in_mem_seg.row_count() > 0) {
        ARCTICDB_TRACE(log::codec(), "Encoding fields");
        for (std::size_t column_index = 0; column_index < in_mem_seg.num_columns(); ++column_index) {
            auto column_data = in_mem_seg.column_data(column_index);
            auto *encoded_field = segment_header->mutable_fields()->Add();
            encoder.encode(codec_opts, column_data, encoded_field, *out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded column {}: ({}) to position {}", column_index, in_mem_seg.descriptor().fields(column_index).name(), pos);
        }
        encode_string_pool<EncodingVersion::V1>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);
    }
    ARCTICDB_DEBUG(log::codec(), "Setting buffer bytes to {}", pos);
    out_buffer->set_bytes(pos);
    tsd->set_out_bytes(pos);
    ARCTICDB_DEBUG(log::codec(), "Encoded header: {}", tsd->DebugString());
    if(!segment_header->has_metadata_field())
        ARCTICDB_DEBUG(log::codec(), "No metadata field");
    ARCTICDB_DEBUG(log::codec(), "Block count {} header size {} ratio {}",
                  in_mem_seg.num_blocks(), segment_header->ByteSizeLong(),
                  in_mem_seg.num_blocks() ? segment_header->ByteSizeLong() / in_mem_seg.num_blocks() : 0);
    return {std::move(arena), segment_header, std::move(out_buffer), in_mem_seg.descriptor().fields_ptr()};
}

namespace {
class MetaBuffer {
  public:
    MetaBuffer() = default;

    shape_t *allocate_shapes(std::size_t bytes) {
        util::check_arg(bytes == 8, "expected exactly one shape, actual {}", bytes / sizeof(shape_t));
        return &shape_;
    }

    uint8_t *allocate_data(std::size_t bytes) {
        buff_.ensure(bytes);
        return buff_.data();
    }

    void advance_data(std::size_t) const {
        // Not used
    }

    void advance_shapes(std::size_t) const {
        // Not used
    }

    void set_allow_sparse(bool) const {
        // Not used
    }

    [[nodiscard]] const Buffer& buffer() const { return buff_; }

    Buffer&& detach_buffer() {
        return std::move(buff_);
    }

  private:
    Buffer buff_;
    shape_t shape_ = 0;
};
}

std::optional<google::protobuf::Any> decode_metadata(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED
    ) {
    if (hdr.has_metadata_field()) {
        auto meta_type_desc = metadata_type_desc();
        MetaBuffer meta_buf;
        std::optional<util::BitMagic> bv;
        data += decode_field(meta_type_desc, hdr.metadata_field(), data, meta_buf, bv, static_cast<EncodingVersion>(hdr.encoding_version()));
        ARCTICDB_TRACE(log::codec(), "Decoded metadata to position {}", data - begin);
        google::protobuf::io::ArrayInputStream ais(meta_buf.buffer().data(),
                                                   static_cast<int>(meta_buf.buffer().bytes()));
        google::protobuf::Any any;
        auto success = any.ParseFromZeroCopyStream(&ais);
        util::check(success, "Failed to parse metadata field in decode_metadata");
        return std::make_optional(std::move(any));
    } else {
        return std::nullopt;
    }
}

void decode_metadata(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED,
    SegmentInMemory& res) {
    auto maybe_any = decode_metadata(hdr, data, begin);
    if(maybe_any)
        res.set_metadata(std::move(*maybe_any));
}

std::optional<google::protobuf::Any> decode_metadata_from_segment(const Segment &segment) {
    auto &hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    const auto begin = data;
    const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;
    if(has_magic_numbers)
        check_magic<MetadataMagic>(data);

    return decode_metadata(hdr, data, begin);
}

Buffer decode_encoded_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin ARCTICDB_UNUSED) {
        ARCTICDB_TRACE(log::codec(), "Decoding encoded fields");
        MetaBuffer meta_buffer;
        std::optional<util::BitMagic> bv;
        if(hdr.has_column_fields()) {
            const auto encoding_version = static_cast<EncodingVersion>(hdr.encoding_version());
            constexpr auto type_desc = encoded_blocks_type_desc();
            decode_field(type_desc, hdr.column_fields(), data, meta_buffer, bv, encoding_version);
        }
        ARCTICDB_TRACE(log::codec(), "Decoded encoded fields at position {}", data-begin);
        return meta_buffer.detach_buffer();
}

std::optional<FieldCollection> decode_index_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED,
    const uint8_t* end) {
    //TODO append map sets the field but it's empty
    if(hdr.has_index_descriptor_field() && hdr.index_descriptor_field().has_ndarray()) {
        ARCTICDB_TRACE(log::codec(), "Decoding index fields");
        util::check(data!=end, "Reached end of input block with index descriptor fields to decode");
        std::optional<util::BitMagic> bv;
        FieldCollection fields;
        data += decode_field(FieldCollection::type(),
                       hdr.index_descriptor_field(),
                       data,
                       fields,
                       bv,
                       static_cast<EncodingVersion>(hdr.encoding_version()));

        ARCTICDB_TRACE(log::codec(), "Decoded index descriptor to position {}", data-begin);
        return std::make_optional<FieldCollection>(std::move(fields));
    } else {
        return std::nullopt;
    }
}

namespace {
inline arcticdb::proto::descriptors::TimeSeriesDescriptor timeseries_descriptor_from_any(const google::protobuf::Any& any) {
    arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
    any.UnpackTo(&tsd);
    return tsd;
}
}

std::optional<FieldCollection> decode_descriptor_fields(
    const arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED,
    const uint8_t* end) {
    if(hdr.has_descriptor_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding index fields");
        util::check(data!=end, "Reached end of input block with descriptor fields to decode");
        std::optional<util::BitMagic> bv;
        FieldCollection fields;
        data += decode_field(FieldCollection::type(),
                       hdr.descriptor_field(),
                       data,
                       fields,
                       bv,
                       static_cast<EncodingVersion>(hdr.encoding_version()));

        ARCTICDB_TRACE(log::codec(), "Decoded descriptor fields to position {}", data-begin);
        return std::make_optional<FieldCollection>(std::move(fields));
    } else {
        return std::nullopt;
    }
}

std::optional<std::tuple<google::protobuf::Any, arcticdb::proto::descriptors::TimeSeriesDescriptor, FieldCollection>> decode_timeseries_descriptor(
    arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin,
    const uint8_t* end) {
    util::check(data != nullptr, "Got null data ptr from segment");
    const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;
    if(has_magic_numbers)
        check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if(!maybe_any)
        return std::nullopt;

    auto tsd = timeseries_descriptor_from_any(*maybe_any);

    if(has_magic_numbers)
        check_magic<DescriptorMagic>(data);

    if(hdr.has_descriptor_field() && hdr.descriptor_field().has_ndarray())
        data += encoding_sizes::ndarray_field_compressed_size(hdr.descriptor_field().ndarray());

    if(has_magic_numbers)
        check_magic<IndexMagic>(data);

    auto maybe_fields = decode_index_fields(hdr, data, begin, end);
    if(!maybe_fields) {
        auto old_fields = fields_from_proto(tsd.stream_descriptor());
        return std::make_optional(std::make_tuple(std::move(*maybe_any), std::move(tsd), std::move(old_fields)));
    }

    maybe_fields->regenerate_offsets();
    return std::make_tuple(std::move(*maybe_any), std::move(tsd), std::move(*maybe_fields));
}

std::optional<std::tuple<google::protobuf::Any, arcticdb::proto::descriptors::TimeSeriesDescriptor, FieldCollection>> decode_timeseries_descriptor(
    Segment& segment) {
    auto &hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = data + segment.buffer().bytes();

    return decode_timeseries_descriptor(hdr, data, begin, end);
}

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(
    arcticdb::proto::encoding::SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin,
    const uint8_t* end) {
    util::check(data != nullptr, "Got null data ptr from segment");
    if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        check_magic<DescriptorMagic>(data);

    auto maybe_fields = decode_descriptor_fields(hdr, data, begin, end);
    if(!maybe_fields) {
        auto old_fields = std::make_shared<FieldCollection>(fields_from_proto(hdr.stream_descriptor()));
        return std::make_pair(std::move(maybe_any),StreamDescriptor{std::make_shared<StreamDescriptor::Proto>(std::move(*hdr.mutable_stream_descriptor())), old_fields});
    }
    return std::make_pair(std::move(maybe_any),StreamDescriptor{std::make_shared<StreamDescriptor::Proto>(std::move(*hdr.mutable_stream_descriptor())), std::make_shared<FieldCollection>(std::move(*maybe_fields))});
}

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(
    Segment& segment) {
    auto &hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = data + segment.buffer().bytes();

    return decode_metadata_and_descriptor_fields(hdr, data, begin, end);
}

void decode_string_pool( const arcticdb::proto::encoding::SegmentHeader& hdr,
                         const uint8_t*& data,
                         const uint8_t* begin ARCTICDB_UNUSED,
                         const uint8_t* end,
                         SegmentInMemory& res) {
    if (hdr.has_string_pool_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding string pool");
        util::check(data!=end, "Reached end of input block with string pool fields to decode");
        std::optional<util::BitMagic> bv;
        data += decode_field(string_pool_descriptor().type(),
                       hdr.string_pool_field(),
                       data,
                       res.string_pool(),
                       bv,
                       static_cast<EncodingVersion>(hdr.encoding_version()));

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
    }
}

void decode_v2(const Segment& segment,
           arcticdb::proto::encoding::SegmentHeader& hdr,
           SegmentInMemory& res,
           const StreamDescriptor& desc)
           {
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const auto [begin, end] = get_segment_begin_end(segment, hdr);
    auto encoded_fields_ptr = end;
    auto data = begin;
    check_magic<MetadataMagic>(data);
    decode_metadata(hdr, data, begin, res);
    util::check(hdr.has_descriptor_field(), "Expected descriptor field in v2 encoding");
    check_magic<DescriptorMagic>(data);
    if(hdr.has_descriptor_field() && hdr.descriptor_field().has_ndarray())
        data += encoding_sizes::field_compressed_size(hdr.descriptor_field());

    check_magic<IndexMagic>(data);
    auto index_fields = decode_index_fields(hdr, data, begin, end);
    if(index_fields)
        res.set_index_fields(std::make_shared<FieldCollection>(std::move(*index_fields)));

    util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
    check_magic<EncodedMagic>(encoded_fields_ptr);
    if (data!=end) {
        auto encoded_fields_buffer = decode_encoded_fields(hdr, encoded_fields_ptr, begin);
        const auto fields_size = desc.fields().size();
        //util::check(fields_size == static_cast<size_t>(hdr.fields_size()), "Mismatch between descriptor and header field size: {} != {}", fields_size, hdr.fields_size());
        const auto start_row = res.row_count();
        EncodedFieldCollection encoded_fields(std::move(encoded_fields_buffer));
        const auto seg_row_count = fields_size ? ssize_t(encoded_fields.at(0).ndarray().items_count()) : 0L;
        res.init_column_map();

        for (std::size_t i = 0; i < static_cast<size_t>(fields_size); ++i) {
            const auto& encoded_field = encoded_fields.at(i);
            //log::version().debug("{}", dump_bytes(begin, (data - begin) + encoding_sizes::field_compressed_size(*encoded_field), 100u));
            const auto& field_name = desc.fields(i).name();
            util::check(data!=end, "Reached end of input block with {} fields to decode", fields_size-i);
            if(auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode_field(res.field(*col_index).type(), encoded_field, data, col, col.opt_sparse_map(), static_cast<EncodingVersion>(hdr.encoding_version()));
            } else {
                data += encoding_sizes::field_compressed_size(encoded_field) + sizeof(ColumnMagic);
            }

            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", i, data-begin);
        }

        check_magic<StringPoolMagic>(data);
        decode_string_pool(hdr, data, begin, end, res);

        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count-1));
        res.set_compacted(segment.header().compacted());
    }}

void decode_v1(const Segment& segment,
            const arcticdb::proto::encoding::SegmentHeader& hdr,
            SegmentInMemory& res,
            StreamDescriptor::Proto& desc)
{
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const uint8_t* data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = begin + segment.buffer().bytes();
    decode_metadata(hdr, data, begin, res);

    if (data!=end) {
        const auto fields_size = desc.fields().size();
        util::check(fields_size == hdr.fields_size(), "Mismatch between descriptor and header field size: {} != {}", fields_size, hdr.fields_size());
        const auto start_row = res.row_count();

        const auto seg_row_count = fields_size ? ssize_t(hdr.fields(0).ndarray().items_count()) : 0LL;
        res.init_column_map();

        for (std::size_t i = 0; i < static_cast<size_t>(fields_size); ++i) {
            const auto& field = hdr.fields(static_cast<int>(i));
            const auto& field_name = desc.fields(static_cast<int>(i)).name();
            util::check(data!=end, "Reached end of input block with {} fields to decode", fields_size-i);
            if(auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode_field(res.field(*col_index).type(), field, data, col, col.opt_sparse_map(), static_cast<EncodingVersion>(hdr.encoding_version()));
            } else
                data += encoding_sizes::field_compressed_size(field);

            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", i, data-begin);
        }

        decode_string_pool(hdr, data, begin, end, res);
        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count-1));
        res.set_compacted(segment.header().compacted());
    }
}

void decode_into_memory_segment(
    const Segment& segment,
    arcticdb::proto::encoding::SegmentHeader& hdr,
    SegmentInMemory& res,
    StreamDescriptor& desc)
{
    if(EncodingVersion(segment.header().encoding_version()) == EncodingVersion::V2)
        decode_v2(segment, hdr, res, desc);
    else
        decode_v1(segment, hdr, res, desc.mutable_proto());
}

SegmentInMemory decode_segment(Segment&& s) {
    auto segment = std::move(s);
    auto &hdr = segment.header();
    ARCTICDB_TRACE(log::codec(), "Decoding descriptor: {}", segment.header().stream_descriptor().DebugString());
    StreamDescriptor descriptor(std::make_shared<StreamDescriptor::Proto>(std::move(*segment.header().mutable_stream_descriptor())), segment.fields_ptr());

    if(EncodingVersion(segment.header().encoding_version()) != EncodingVersion::V2)
        descriptor.fields() = field_collection_from_proto(std::move(*descriptor.mutable_proto().mutable_fields()));

    descriptor.fields().regenerate_offsets();
    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory res(std::move(descriptor));
    ARCTICDB_TRACE(log::codec(), "Decoding segment");
    decode_into_memory_segment(segment, hdr, res, res.descriptor());
    ARCTICDB_TRACE(log::codec(), "Returning segment");
    return res;
}

} // namespace arcticdb
