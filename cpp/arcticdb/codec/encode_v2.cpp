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
#include <arcticdb/util/configs_map.hpp>


namespace arcticdb {
    void add_bitmagic_compressed_size(
        const ColumnData& column_data,
        size_t& max_compressed_bytes,
        size_t& uncompressed_bytes
    );

    void encode_sparse_map(
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    );

    template<typename MagicType>
    static void write_magic(Buffer& buffer, std::ptrdiff_t& pos) {
        new (buffer.data() + pos) MagicType{};
        pos += sizeof(MagicType);
    }

    /// @brief Utility class used to encode and compute the max encoding size for regular data columns for V2 encoding
    /// What differs this from the already existing ColumnEncoder is that ColumnEncoder encodes the shapes of
    /// multidimensional data as part of each block. ColumnEncoder2 uses a better strategy and encodes the shapes for the
    /// whole column upfront (before all blocks).
    /// @note Although ArcticDB did not support multidimensional user data prior to creating ColumnEncoder2 some of the
    /// internal data was multidimensional and used ColumnEncoder. More specifically: string pool and metadata.
    /// @note This should be used for V2 encoding. V1 encoding can't use it as there is already data written the other
    ///	way and it will be hard to distinguish both.
    struct ColumnEncoderV2 {
    public:
        static void encode(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
        static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts,
            ColumnData& column_data);
    private:
        static void encode_shapes(
            const ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos_in_buffer);
        static void encode_blocks(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
    };


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
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    ) {
        ARCTICDB_DEBUG(log::codec(), "Encoding field with codec {}", codec_opts.DebugString());
        encode_shapes(column_data, variant_field, out, pos);
        encode_blocks(codec_opts, column_data, variant_field, out, pos);
        encode_sparse_map(column_data, variant_field, out, pos);
    }

    void ColumnEncoderV2::encode_shapes(
        const ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos_in_buffer
    ) {
        // There is no need to store the shapes for a column of empty type as they will be all 0. The type handler will
        // assign 0 for the shape upon reading. There is one edge case - when we have None in the column, as it should not
        // have shape at all (since it's not an array). This is handled by the sparse map.
        if(column_data.type().dimension() != Dimension::Dim0 && !is_empty_type(column_data.type().data_type())) {
            TypedBlockData<ShapesBlockTDT> shapes_block = create_shapes_typed_block(column_data);
            util::variant_match(variant_field, [&](auto field){
                using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingVersion::V2>;
                ShapesEncoder::encode_shapes(codec::default_shapes_codec(), shapes_block, *field, out, pos_in_buffer);
            });
        }
    }

    void ColumnEncoderV2::encode_blocks(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    ) {
        column_data.type().visit_tag([&](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V2>;
            ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
            while (auto block = column_data.next<TDT>()) {
                if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                    util::check(block.value().nbytes() > 0, "Zero-sized block");
                }
                util::variant_match(variant_field, [&](auto field) {
                    Encoder::encode_values(codec_opts, block.value(), *field, out, pos);
                });
            }
        });
    }

    std::pair<size_t, size_t> ColumnEncoderV2::max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data
    ) {
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
                const auto nbytes = block.value().nbytes();
                if constexpr(must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                    util::check(nbytes > 0, "Zero-sized block");
                }
                uncompressed_bytes += nbytes;
                // For the empty type the column will contain 0 size of user data however the encoder might need add some
                // encoder specific data to the buffer, thus the uncompressed size will be 0 but the max_compressed_bytes
                // might be non-zero.
                max_compressed_bytes += Encoder::max_compressed_size(codec_opts, block.value());
            }
            add_bitmagic_compressed_size(column_data, uncompressed_bytes, max_compressed_bytes);
            return std::make_pair(uncompressed_bytes, max_compressed_bytes);
        });
    }

    using EncodingPolicyV2 = EncodingPolicyType<EncodingVersion::V2, ColumnEncoderV2>;

    static void encode_field_descriptors(
        const SegmentInMemory& in_mem_seg,
        arcticdb::proto::encoding::SegmentHeader& segment_header,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        Buffer& out_buffer,
        std::ptrdiff_t& pos
    ) {
        ARCTICDB_TRACE(log::codec(), "Encoding field descriptors to position {}", pos);
        auto *encoded_field = segment_header.mutable_descriptor_field();
        auto col = in_mem_seg.descriptor().fields().column_data();
        ColumnEncoderV2::encode(codec_opts, col, encoded_field, out_buffer, pos);
        ARCTICDB_TRACE(log::codec(), "Encoded field descriptors to position {}", pos);

        write_magic<IndexMagic>(out_buffer, pos);
        if (in_mem_seg.index_fields()) {
            ARCTICDB_TRACE(log::codec(), "Encoding index fields descriptors to position {}", pos);
            auto *index_field = segment_header.mutable_index_descriptor_field();
            auto index_col = in_mem_seg.index_fields()->column_data();
            ColumnEncoderV2::encode(codec_opts, index_col, index_field, out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded index field descriptors to position {}", pos);
        }
    }

    [[nodiscard]] size_t calc_column_blocks_size(const Column& col) {
        size_t bytes = EncodedField::Size;
        if(col.type().dimension() != entity::Dimension::Dim0)
            bytes += sizeof(EncodedBlock);

        bytes += sizeof(EncodedBlock) * col.num_blocks();
        ARCTICDB_TRACE(log::version(), "Encoded block size: {} + shapes({}) + {} * {} = {}",
            EncodedField::Size,
            col.type().dimension() != entity::Dimension::Dim0 ? sizeof(EncodedBlock) : 0u,
            sizeof(EncodedBlock),
            col.num_blocks(),
            bytes);

        return bytes;
    }

    [[nodiscard]] static size_t encoded_blocks_size(const SegmentInMemory& in_mem_seg) {
        size_t bytes = 0;
        for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
            const auto& col = in_mem_seg.column(position_t(c));
            bytes += calc_column_blocks_size(col);
        }
        return bytes;
    }

    static void calc_encoded_blocks_size(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        SizeResult& result
    ) {
        result.encoded_blocks_bytes_ = static_cast<shape_t>(encoded_blocks_size(in_mem_seg));
        result.uncompressed_bytes_ += result.encoded_blocks_bytes_;
        result.max_compressed_bytes_ += BytesEncoder<EncodingPolicyV2>::max_compressed_size(codec_opts, result.encoded_blocks_bytes_);
    }

    static void calc_stream_descriptor_fields_size(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        SizeResult& result
    ) {
        auto segment_fields = in_mem_seg.descriptor().fields().column_data();
        const auto [uncompressed, required] = ColumnEncoderV2::max_compressed_size(codec_opts, segment_fields);
        result.uncompressed_bytes_ += uncompressed;
        result.max_compressed_bytes_ += required;

        // Calculate index fields size
        if(in_mem_seg.index_fields()) {
            auto index_field_data = in_mem_seg.index_fields()->column_data();
            const auto [idx_uncompressed, idx_required] = ColumnEncoderV2::max_compressed_size(codec_opts, index_field_data);
            result.uncompressed_bytes_ += idx_uncompressed;
            result.max_compressed_bytes_ += idx_required;
        }
    }

    [[nodiscard]] SizeResult max_compressed_size_v2(
        const SegmentInMemory& in_mem_seg,
        const arcticdb::proto::encoding::VariantCodec& codec_opts
    ) {
        ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
        SizeResult result{};
        result.max_compressed_bytes_ += sizeof(MetadataMagic);
        calc_metadata_size<EncodingPolicyV2>(in_mem_seg, codec_opts, result);
        result.max_compressed_bytes_ += sizeof(DescriptorMagic);
        result.max_compressed_bytes_ += sizeof(IndexMagic);
        calc_stream_descriptor_fields_size(in_mem_seg, codec_opts, result);
        result.max_compressed_bytes_ += sizeof(EncodedMagic);
        calc_encoded_blocks_size(in_mem_seg, codec_opts, result);

        // Calculate fields collection size
        if(in_mem_seg.row_count() > 0) {
            result.max_compressed_bytes_ += sizeof(ColumnMagic) * in_mem_seg.descriptor().field_count();
            calc_columns_size<EncodingPolicyV2>(in_mem_seg, codec_opts, result);
            result.max_compressed_bytes_ += sizeof(StringPoolMagic);
            calc_string_pool_size<EncodingPolicyV2>(in_mem_seg, codec_opts, result);
        }
        ARCTICDB_TRACE(log::codec(), "Max compressed size {}", result.max_compressed_bytes_);
        return result;
    }

    static void encode_encoded_fields(
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
        if(!encoded_blocks_buffer.empty())
            BytesEncoder<EncodingPolicyV2>::encode(encoded_blocks_buffer, codec_opts, out_buffer, pos, encoded_field);

        ARCTICDB_TRACE(log::codec(), "Encoded encoded blocks to position {}", pos);
    }

    [[nodiscard]] Segment encode_v2(SegmentInMemory&& s, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
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
        encode_metadata<EncodingPolicyV2>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);
        write_magic<DescriptorMagic>(*out_buffer, pos);
        encode_field_descriptors(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);

        auto encoded_fields_buffer = ChunkedBuffer::presized(static_cast<size_t>(encoded_buffer_size));
        auto encoded_field_pos = 0u;
        ColumnEncoderV2 encoder;
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
            write_magic<StringPoolMagic>(*out_buffer, pos);
            encode_string_pool<EncodingPolicyV2>(in_mem_seg, *segment_header, codec_opts, *out_buffer, pos);
        }

        encode_encoded_fields(*segment_header, codec_opts, *out_buffer, pos, encoded_fields_buffer);
        out_buffer->set_bytes(pos);
        tsd->set_out_bytes(pos);

        ARCTICDB_TRACE(log::codec(), "Encoded header: {}", tsd->DebugString());
        ARCTICDB_DEBUG(log::codec(), "Block count {} header size {} ratio {}",
            in_mem_seg.num_blocks(), segment_header->ByteSizeLong(),
            in_mem_seg.num_blocks() ? segment_header->ByteSizeLong() / in_mem_seg.num_blocks() : 0);
        return {std::move(arena), segment_header, std::move(out_buffer), seg_descriptor.fields_ptr()};
    }
}
