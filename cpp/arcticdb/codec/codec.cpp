/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>


#include <string>
#include <google/protobuf/io/zero_copy_stream_impl.h>


#include <arcticdb/codec/encode_common.hpp>


namespace arcticdb {

Segment encode_v2(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts
);

Segment encode_v1(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec& codec_opts
);

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

Segment encode_dispatch(
    SegmentInMemory&& in_mem_seg,
    const arcticdb::proto::encoding::VariantCodec &codec_opts,
    EncodingVersion encoding_version) {
    if(encoding_version == EncodingVersion::V2) {
        return encode_v2(std::move(in_mem_seg), codec_opts);
    } else {
        return encode_v1(std::move(in_mem_seg), codec_opts);
    }
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
    const SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED
    ) {
    if (hdr.has_metadata_field()) {
        auto meta_type_desc = metadata_type_desc();
        MetaBuffer meta_buf;
        std::optional<util::BitMagic> bv;
        data += decode_field(meta_type_desc, hdr.metadata_field(), data, meta_buf, bv, hdr.encoding_version());
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
    const SegmentHeader& hdr,
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
    if(const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2; has_magic_numbers)
        util::check_magic<MetadataMagic>(data);

    return decode_metadata(hdr, data, begin);
}

Buffer decode_encoded_fields(
    const SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin ARCTICDB_UNUSED) {
        ARCTICDB_TRACE(log::codec(), "Decoding encoded fields");
        MetaBuffer meta_buffer;
        std::optional<util::BitMagic> bv;
        if(hdr.has_column_fields()) {
            constexpr auto type_desc = encoded_blocks_type_desc();
            decode_field(type_desc, hdr.column_fields(), data, meta_buffer, bv, hdr.encoding_version());
        }
        ARCTICDB_TRACE(log::codec(), "Decoded encoded fields at position {}", data-begin);
        return meta_buffer.detach_buffer();
}

std::optional<FieldCollection> decode_index_fields(
    const SegmentHeader& hdr,
    const uint8_t*& data,
    const uint8_t* begin ARCTICDB_UNUSED,
    const uint8_t* end) {
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
                       hdr.encoding_version());

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
    const SegmentHeader& hdr,
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
                       hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded descriptor fields to position {}", data-begin);
        return std::make_optional<FieldCollection>(std::move(fields));
    } else {
        return std::nullopt;
    }
}

std::optional<std::tuple<google::protobuf::Any, arcticdb::proto::descriptors::TimeSeriesDescriptor, FieldCollection>> decode_timeseries_descriptor(
    const SegmentHeader& hdr,
    const uint8_t* data,
    const uint8_t* begin,
    const uint8_t* end) {
    util::check(data != nullptr, "Got null data ptr from segment");
    const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;
    if(has_magic_numbers)
        util::check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if(!maybe_any)
        return std::nullopt;

    auto tsd = timeseries_descriptor_from_any(*maybe_any);

    if(has_magic_numbers)
        util::check_magic<DescriptorMagic>(data);

    if(hdr.has_descriptor_field() && hdr.descriptor_field().has_ndarray())
        data += encoding_sizes::ndarray_field_compressed_size(hdr.descriptor_field().ndarray());

    if(has_magic_numbers)
        util::check_magic<IndexMagic>(data);

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
    const auto &hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = data + segment.buffer().bytes();

    return decode_timeseries_descriptor(hdr, data, begin, end);
}

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(
    Segment& segment) {
    auto &hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        util::check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        util::check_magic<DescriptorMagic>(data);

    return std::make_pair(std::move(maybe_any), segment.descriptor());
}

void decode_string_pool( const SegmentHeader& hdr,
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
                       hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data-begin);
    }
}

void decode_v2(const Segment& segment,
           SegmentHeader& hdr,
           SegmentInMemory& res,
           const StreamDescriptor& desc)
           {
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const auto [begin, end] = get_segment_begin_end(segment, hdr);
    auto encoded_fields_ptr = end;
    auto data = begin;
    util::check_magic<MetadataMagic>(data);
    decode_metadata(hdr, data, begin, res);
    util::check(hdr.has_descriptor_field(), "Expected descriptor field in v2 encoding");
    util::check_magic<DescriptorMagic>(data);
    if(hdr.has_descriptor_field() && hdr.descriptor_field().has_ndarray())
        data += encoding_sizes::field_compressed_size(hdr.descriptor_field());

    util::check_magic<IndexMagic>(data);
    if(auto index_fields = decode_index_fields(hdr, data, begin, end); index_fields)
        res.set_index_fields(std::make_shared<FieldCollection>(std::move(*index_fields)));

    util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
    util::check_magic<EncodedMagic>(encoded_fields_ptr);
    if (data!=end) {
        auto encoded_fields_buffer = decode_encoded_fields(hdr, encoded_fields_ptr, begin);
        const auto fields_size = desc.fields().size();
        const auto start_row = res.row_count();
        EncodedFieldCollection encoded_fields(std::move(encoded_fields_buffer));
        const auto seg_row_count = fields_size ? ssize_t(encoded_fields.at(0).ndarray().items_count()) : 0L;
        res.init_column_map();

        for (std::size_t i = 0; i < fields_size; ++i) {
            const auto& encoded_field = encoded_fields.at(i);
#ifdef DUMP_BYTES
            log::version().debug("{}", dump_bytes(begin, (data - begin) + encoding_sizes::field_compressed_size(*encoded_field), 100u));
#endif
            const auto& field_name = desc.fields(i).name();
            util::check(data!=end, "Reached end of input block with {} fields to decode", fields_size-i);
            if(auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode_field(res.field(*col_index).type(), encoded_field, data, col, col.opt_sparse_map(), hdr.encoding_version());
            } else {
                data += encoding_sizes::field_compressed_size(encoded_field) + sizeof(ColumnMagic);
            }

            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", i, data-begin);
        }

        util::check_magic<StringPoolMagic>(data);
        decode_string_pool(hdr, data, begin, end, res);

        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count-1));
        res.set_compacted(segment.header().compacted());
    }}

void decode_v1(const Segment& segment,
            const SegmentHeader& hdr,
            SegmentInMemory& res,
            StreamDescriptor& desc)
{
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const uint8_t* data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = begin + segment.buffer().bytes();
    decode_metadata(hdr, data, begin, res);

    if (data!=end) {
        const auto fields_size = desc.fields().size();
        const auto& column_fields = hdr.body_fields();
        util::check(fields_size == segment.fields_size(), "Mismatch between descriptor and header field size: {} != {}", fields_size, column_fields.size());
        const auto start_row = res.row_count();
        const auto seg_row_count = fields_size ? ssize_t(column_fields.at(0).ndarray().items_count()) : 0LL;
        res.init_column_map();

        for (std::size_t i = 0; i < fields_size; ++i) {
            const auto& field = column_fields.at(i);
            const auto& field_name = desc.fields(i).name();
            util::check(data!=end, "Reached end of input block with {} fields to decode", fields_size-i);
            if(auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode_field(res.field(*col_index).type(), field, data, col, col.opt_sparse_map(), hdr.encoding_version());
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
    SegmentHeader& hdr,
    SegmentInMemory& res,
    StreamDescriptor& desc)
{
    if(EncodingVersion(segment.header().encoding_version()) == EncodingVersion::V2)
        decode_v2(segment, hdr, res, desc);
    else
        decode_v1(segment, hdr, res, desc);
}

SegmentInMemory decode_segment(Segment&& s) {
    auto segment = std::move(s);
    auto &hdr = segment.header();
    ARCTICDB_TRACE(log::codec(), "Decoding descriptor: {}", segment.header().stream_descriptor());
    auto descriptor = segment.descriptor();
    descriptor.fields().regenerate_offsets();
    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory res(std::move(descriptor));
    ARCTICDB_TRACE(log::codec(), "Decoding segment");
    decode_into_memory_segment(segment, hdr, res, res.descriptor());
    ARCTICDB_TRACE(log::codec(), "Returning segment");
    return res;
}

static void hash_field(const EncodedFieldImpl &field, HashAccum &accum) {
    auto &n = field.ndarray();
    for(auto i = 0; i < n.shapes_size(); ++i) {
        auto v = n.shapes(i).hash();
        accum(&v);
    }

    for(auto j = 0; j < n.values_size(); ++j) {
        auto v = n.values(j).hash();
        accum(&v);
    }
}

HashedValue hash_segment_header(const SegmentHeader &hdr) {
    HashAccum accum;
    if (hdr.has_metadata_field()) {
        hash_field(hdr.metadata_field(), accum);
    }
    if(hdr.has_string_pool_field()) {
        hash_field(hdr.string_pool_field(), accum);
    }

    return accum.digest();
}

void add_bitmagic_compressed_size(
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

/// @brief Write the sparse map to the out buffer
/// Bitmagic achieves the theoretical best compression for booleans. Adding additional encoding (lz4, zstd, etc...)
/// will not improve anything and in fact it might worsen the encoding.
[[nodiscard]] static size_t encode_bitmap(const util::BitMagic& sparse_map, Buffer& out, std::ptrdiff_t& pos) {
    ARCTICDB_DEBUG(log::version(), "Encoding sparse map of count: {}", sparse_map.count());
    bm::serializer<bm::bvector<> > bvs;
    bm::serializer<bm::bvector<> >::buffer sbuf;
    bvs.serialize(sparse_map, sbuf);
    auto sz = sbuf.size();
    auto total_sz = sz + util::combined_bit_magic_delimiters_size();
    out.assert_size(pos + total_sz);

    uint8_t* target = out.data() + pos;
    util::write_magic<util::BitMagicStart>(target);
    std::memcpy(target, sbuf.data(), sz);
    target += sz;
    util::write_magic<util::BitMagicEnd>(target);
    pos = pos + static_cast<ptrdiff_t>(total_sz);
    return total_sz;
}

void encode_sparse_map(
    ColumnData& column_data,
    std::variant<EncodedFieldImpl*, arcticdb::proto::encoding::EncodedField*> variant_field,
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
} // namespace arcticdb
