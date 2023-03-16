/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/util/configs_map.hpp>

#include <string>
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace arcticdb {

std::pair<size_t, size_t> ColumnEncoder::max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec &codec_opts,
        ColumnData &column_data) {
    return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
        size_t max_compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
        using TDT = decltype(type_desc_tag);
        using Encoder = BlockEncoder<TDT>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());

        while (auto block = column_data.next<TDT>()) {
            const auto nbytes = block.value().nbytes();
            util::check(nbytes, "Zero-sized block");
            uncompressed_bytes += nbytes;
            max_compressed_bytes += Encoder::max_compressed_size(codec_opts, block.value());
        }

        if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0)   {
            bm::serializer<util::BitMagic>::statistics_type stat{};
            column_data.bit_vector()->calc_stat(&stat);
            uncompressed_bytes += stat.memory_used;
            max_compressed_bytes += stat.max_serialize_mem;
        }
        return std::make_pair(uncompressed_bytes, max_compressed_bytes);
    });
}

void ColumnEncoder::encode(
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    ColumnData &column_data,
    arcticdb::proto::encoding::EncodedField &field,
    Buffer &out,
    std::ptrdiff_t &pos) {
    column_data.type().visit_tag([&codec_opts, &column_data, &field, &out, &pos](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        using Encoder = BlockEncoder<TDT>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());

        while (auto block = column_data.next<TDT>()) {
            util::check(block.value().nbytes(), "Zero-sized block");
            Encoder::encode(codec_opts, block.value(), field, out, pos);
        }

        if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0)   {
            ARCTICDB_DEBUG(log::codec(), "Sparse map count = {} pos = {}", column_data.bit_vector()->count(), pos);
            auto sparse_bm_bytes = encode_bitmap(*column_data.bit_vector(), out, pos);
            field.mutable_ndarray()->set_sparse_map_bytes(static_cast<int>(sparse_bm_bytes));
        }
    });
}

constexpr TypeDescriptor metadata_type_desc() {
    return TypeDescriptor{
        DataType::UINT8, Dimension::Dim1
    };
}

std::pair<size_t, size_t> max_compressed_size(const SegmentInMemory &in_mem_seg, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    /*
     * This takes an in memory segment with all the metadata, column tensors etc, loops through each column
     * and based on the type of the column, calls the typed block encoder for that column.
     */
    ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
    size_t max_compressed_bytes = 0;
    size_t uncompressed_bytes = 0;

    using BytesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;
    using BytesEncoder = BlockEncoder<BytesTypeDescriptorTag>;
    if (in_mem_seg.metadata()) {
        auto metadata_bytes = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        uncompressed_bytes += metadata_bytes;
        max_compressed_bytes += BytesEncoder::max_compressed_size(codec_opts, TypedBlockData<BytesTypeDescriptorTag>(metadata_bytes, &metadata_bytes));
        shape_t shapes_bytes = sizeof(shape_t);
        uncompressed_bytes += shapes_bytes;
        max_compressed_bytes += BytesEncoder::max_compressed_size(codec_opts, TypedBlockData<BytesTypeDescriptorTag>(shapes_bytes, &shapes_bytes));
        ARCTICDB_TRACE(log::codec(), "Metadata requires {} max_compressed_bytes", max_compressed_bytes);
    }

    ColumnEncoder encoder;
    if(in_mem_seg.row_count() > 0) {
        for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
            auto col = in_mem_seg.column_data(c);
            arcticdb::proto::encoding::VariantCodec codec;
            codec.CopyFrom(codec_opts);
            const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, col);
            uncompressed_bytes += uncompressed;
            max_compressed_bytes += required;
            ARCTICDB_TRACE(log::codec(), "Column {} requires {} max_compressed_bytes, total {}", c, required, max_compressed_bytes);
        }
        if (in_mem_seg.has_string_pool()) {
            auto col = in_mem_seg.string_pool_data();
            arcticdb::proto::encoding::VariantCodec codec;
            codec.CopyFrom(codec_opts);
            const auto [uncompressed, required] = encoder.max_compressed_size(codec_opts, col);
            uncompressed_bytes += uncompressed;
            max_compressed_bytes += required;
            ARCTICDB_TRACE(log::codec(), "String pool requires {} max_compressed_bytes, total {}", required, max_compressed_bytes);
        }
    }
    ARCTICDB_TRACE(log::codec(), "Max compressed size {}", max_compressed_bytes);
    return std::make_pair(uncompressed_bytes, max_compressed_bytes);
}

Segment encode(SegmentInMemory&& s, const arcticdb::proto::encoding::VariantCodec &codec_opts) {
    /*
     * This takes an in memory segment with all the metadata, column tensors etc, loops through each column
     * and based on the type of the column, calls the typed block encoder for that column.
     */
    ARCTICDB_SAMPLE(EncodeSegment, 0)
    auto in_mem_seg = std::move(s);
    auto arena = std::make_unique<google::protobuf::Arena>();
    auto segment_header = google::protobuf::Arena::CreateMessage<arcticdb::proto::encoding::SegmentHeader>(arena.get());
    *segment_header->mutable_stream_descriptor() = std::move(in_mem_seg.descriptor().mutable_proto());
    segment_header->set_compacted(in_mem_seg.compacted());
    std::ptrdiff_t pos = 0;
    static auto block_to_header_ratio = ConfigsMap::instance()->get_int("Codec.EstimatedHeaderRatio", 75);
    const auto preamble = in_mem_seg.num_blocks() * block_to_header_ratio;
    auto [uncompressed_size, body_size] = max_compressed_size(in_mem_seg, codec_opts);
    ARCTICDB_TRACE(log::codec(), "Estimated max buffer requirement: {}", body_size);
    auto out_buffer = std::make_shared<Buffer>(body_size, preamble);
    ColumnEncoder encoder;

    ARCTICDB_TRACE(log::codec(), "Encoding descriptor: {}", segment_header->stream_descriptor().DebugString());
    auto *tsd = segment_header->mutable_stream_descriptor();
    tsd->set_in_bytes(uncompressed_size);

    if (in_mem_seg.metadata()) {
        const auto bytes_count = static_cast<shape_t>(in_mem_seg.metadata()->ByteSizeLong());
        ARCTICDB_TRACE(log::codec(), "Encoding {} bytes of metadata", bytes_count);
        auto *encoded_field = segment_header->mutable_metadata_field();
        arcticdb::proto::encoding::VariantCodec codec;
        codec.CopyFrom(codec_opts);

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

        using BytesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;
        using BytesEncoder = BlockEncoder<BytesTypeDescriptorTag>;
        auto typed_block = TypedBlockData<BytesTypeDescriptorTag>(
            meta_buffer.data(),
            &bytes_count,
            bytes_count,
            1u,
            meta_buffer.block_and_offset(0).block_);

        BytesEncoder::encode(codec_opts, typed_block, *encoded_field, *out_buffer, pos);
        ARCTICDB_DEBUG(log::codec(), "Encoded metadata to position {}", pos);
        if(malloced)
            free(meta_ptr);
    }

    if(in_mem_seg.row_count() > 0) {
        ARCTICDB_TRACE(log::codec(), "Encoding fields");
        for (std::size_t c = 0; c < in_mem_seg.num_columns(); ++c) {
            auto col = in_mem_seg.column_data(c);
            arcticdb::proto::encoding::VariantCodec codec;
            codec.CopyFrom(codec_opts);
            auto *encoded_field = segment_header->mutable_fields()->Add();
            encoder.encode(codec_opts, col, *encoded_field, *out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded column {}: ({}) to position {}", c, segment_header->stream_descriptor().fields(c).name(), pos);
        }
        if (in_mem_seg.has_string_pool()) {
            ARCTICDB_TRACE(log::codec(), "Encoding string pool to position {}", pos);
            auto *encoded_field = segment_header->mutable_string_pool_field();
            auto col = in_mem_seg.string_pool_data();
            arcticdb::proto::encoding::VariantCodec codec;
            codec.CopyFrom(codec_opts);
            encoder.encode(codec_opts, col, *encoded_field, *out_buffer, pos);
            ARCTICDB_TRACE(log::codec(), "Encoded string pool to position {}", pos);
        }
    }
    ARCTICDB_DEBUG(log::codec(), "Setting buffer bytes to {}", pos);
    out_buffer->set_bytes(pos);
    tsd->set_out_bytes(pos);
    ARCTICDB_DEBUG(log::codec(), "Encoded header: {}", tsd->DebugString());

    ARCTICDB_DEBUG(log::codec(), "Block count {} header size {} ratio {}",
        in_mem_seg.num_blocks(), segment_header->ByteSizeLong(),
        in_mem_seg.num_blocks() ? segment_header->ByteSizeLong() / in_mem_seg.num_blocks() : 0);
    return {std::move(arena), segment_header, std::move(out_buffer)};
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

  private:
    Buffer buff_;
    shape_t shape_ = 0;
};
}

size_t encode_bitmap(const util::BitMagic &sparse_map, Buffer &out, std::ptrdiff_t &pos) {
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

std::optional<google::protobuf::Any> decode_metadata(const Segment &segment) {
    auto &hdr = segment.header();
    if (!hdr.has_metadata_field())
        return std::nullopt;

    auto meta_type_desc = metadata_type_desc();
    MetaBuffer meta_buf;
    std::optional<util::BitMagic> bv;
    decode(meta_type_desc, hdr.metadata_field(), segment.buffer().data(), meta_buf, bv);
    google::protobuf::io::ArrayInputStream ais(meta_buf.buffer().data(),
                                               static_cast<int>(meta_buf.buffer().bytes()));

    google::protobuf::Any any;
    auto success = any.ParseFromZeroCopyStream(&ais);
    util::check(success, "Failed to parse metadata field in decode_metadata");
    return std::make_optional(std::move(any));
}

void decode(const Segment& segment,
            const arcticdb::proto::encoding::SegmentHeader& hdr,
            SegmentInMemory& res,
            const StreamDescriptor::Proto& desc)
{
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const uint8_t* data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = begin + segment.buffer().bytes();
    if (hdr.has_metadata_field()) {
        auto meta_type_desc = metadata_type_desc();

        MetaBuffer meta_buf;
        std::optional<util::BitMagic> bv;
        data += decode(meta_type_desc, hdr.metadata_field(), data, meta_buf, bv);
        ARCTICDB_TRACE(log::codec(), "Decoded metadata to position {}", data-begin);
        google::protobuf::io::ArrayInputStream ais(meta_buf.buffer().data(),
                                                   static_cast<int>(meta_buf.buffer().bytes()));
        google::protobuf::Any any;
        auto success = any.ParseFromZeroCopyStream(&ais);
        util::check(success, "Failed to parse metadata field in decode");
        res.set_metadata(std::move(any));
    }
    if (data!=end) {
        const auto fields_size = desc.fields().size();
        util::check(fields_size == hdr.fields_size(), "Mismatch between descriptor and header field size: {} != {}", fields_size, hdr.fields_size());
        const auto start_row = res.row_count();

        const auto seg_row_count = fields_size ? ssize_t(hdr.fields(0).ndarray().items_count()) : 0LL;
        res.init_column_map();

        for (std::size_t i = 0; i < static_cast<size_t>(fields_size); ++i) {
            const auto& field = hdr.fields(static_cast<int>(i));
            const auto& field_name = desc.fields(i).name();
            util::check(data!=end, "Reached end of input block with {} fields to decode", fields_size-i);
            if(auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode(type_desc_from_proto(res.field(*col_index).type_desc()), field, data, col, col.opt_sparse_map());

            } else
                data += encoding_size::compressed_size(field);

            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", i, data-begin);
        }

        if (hdr.has_string_pool_field()) {
            ARCTICDB_TRACE(log::codec(), "Decoding string pool");
            util::check(data!=end, "Reached end of input block with string pool fields to decode");
            std::optional<util::BitMagic> bv;
            data += decode(SegmentInMemory::string_pool_descriptor().type_desc(),
                           hdr.string_pool_field(),
                           data,
                           res.string_pool(),
                           bv);

            ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
        }

        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count-1));
        res.set_compacted(segment.header().compacted());
        auto is_sparse = res.is_sparse();
        if(is_sparse)
            log::version().debug("Brooop");
    }
}

SegmentInMemory decode(Segment&& s) {
    auto segment = std::move(s);
    auto &hdr = segment.header();
    ARCTICDB_TRACE(log::codec(), "Decoding descriptor: {}", segment.header().stream_descriptor().DebugString());
    StreamDescriptor descriptor(std::move(*segment.header().mutable_stream_descriptor()));

    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory res(std::move(descriptor));
    ARCTICDB_TRACE(log::codec(), "Decoding segment");
    decode(segment, hdr, res, res.descriptor().proto());
    ARCTICDB_TRACE(log::codec(), "Returning segment");
    return res;
}

} // namespace arcticdb
