/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/codec/encode_common.hpp>
#include <arcticdb/codec/segment_identifier.hpp>

namespace arcticdb {

constexpr TypeDescriptor metadata_type_desc() { return TypeDescriptor{DataType::UINT8, Dimension::Dim1}; }

SizeResult max_compressed_size_dispatch(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts,
        EncodingVersion encoding_version
) {
    if (encoding_version == EncodingVersion::V2) {
        return max_compressed_size_v2(in_mem_seg, codec_opts);
    } else {
        return max_compressed_size_v1(in_mem_seg, codec_opts);
    }
}

Segment encode_dispatch(
        SegmentInMemory&& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts,
        EncodingVersion encoding_version
) {
    if (encoding_version == EncodingVersion::V2) {
        return encode_v2(std::move(in_mem_seg), codec_opts);
    } else {
        return encode_v1(std::move(in_mem_seg), codec_opts);
    }
}

namespace {
class MetaBuffer {
  public:
    MetaBuffer() = default;

    shape_t* allocate_shapes(std::size_t bytes) {
        util::check_arg(bytes == 8, "expected exactly one shape, actual {}", bytes / sizeof(shape_t));
        return &shape_;
    }

    uint8_t* allocate_data(std::size_t bytes) {
        buff_.ensure(bytes);
        return buff_.data();
    }

    void advance_data(std::size_t) const {
        // Not used
    }

    void advance_shapes(std::size_t) const {
        // Not used
    }

    void set_allow_sparse(Sparsity) const {
        // Not used
    }

    [[nodiscard]] const Buffer& buffer() const { return buff_; }

    Buffer&& detach_buffer() { return std::move(buff_); }

  private:
    Buffer buff_;
    shape_t shape_ = 0;
};
} // namespace

std::optional<google::protobuf::Any> decode_metadata(
        const SegmentHeader& hdr, const uint8_t*& data, const uint8_t* begin ARCTICDB_UNUSED
) {
    if (hdr.has_metadata_field()) {
        hdr.metadata_field().validate();
        auto meta_type_desc = metadata_type_desc();
        MetaBuffer meta_buf;
        std::optional<util::BitMagic> bv;
        ARCTICDB_DEBUG(log::codec(), "Decoding metadata at position {}: {}", data - begin, dump_bytes(data, 10));
        data += decode_ndarray(
                meta_type_desc, hdr.metadata_field().ndarray(), data, meta_buf, bv, hdr.encoding_version()
        );
        ARCTICDB_TRACE(log::codec(), "Decoded metadata to position {}", data - begin);
        google::protobuf::io::ArrayInputStream ais(
                meta_buf.buffer().data(), static_cast<int>(meta_buf.buffer().bytes())
        );
        google::protobuf::Any any;
        auto success = any.ParseFromZeroCopyStream(&ais);
        util::check(success, "Failed to parse metadata field in decode_metadata");
        return std::make_optional(std::move(any));
    } else {
        return std::nullopt;
    }
}

void decode_metadata(
        const SegmentHeader& hdr, const uint8_t*& data, const uint8_t* begin ARCTICDB_UNUSED, SegmentInMemory& res
) {
    auto maybe_any = decode_metadata(hdr, data, begin);
    if (maybe_any) {
        ARCTICDB_TRACE(log::version(), "Found metadata on segment");
        res.set_metadata(std::move(*maybe_any));
    } else {
        ARCTICDB_TRACE(log::version(), "No metadata on segment");
    }
}

std::optional<google::protobuf::Any> decode_metadata_from_segment(const Segment& segment) {
    auto& hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    const auto begin = data;
    if (const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;
        has_magic_numbers)
        util::check_magic<MetadataMagic>(data);

    return decode_metadata(hdr, data, begin);
}

EncodedFieldCollection decode_encoded_fields(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin ARCTICDB_UNUSED
) {
    ARCTICDB_TRACE(log::codec(), "Decoding encoded fields");

    util::check(
            hdr.has_column_fields() && hdr.column_fields().has_ndarray(),
            "Expected encoded field description to be set in header"
    );
    std::optional<util::BitMagic> bv;
    const auto uncompressed_size = encoding_sizes::uncompressed_size(hdr.column_fields());
    constexpr auto type_desc = encoded_fields_type_desc();
    Column encoded_column(type_desc, uncompressed_size, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    decode_ndarray(type_desc, hdr.column_fields().ndarray(), data, encoded_column, bv, hdr.encoding_version());

    ARCTICDB_TRACE(log::codec(), "Decoded encoded fields at position {}", data - begin);
    return {std::move(encoded_column.release_buffer()), std::move(encoded_column.release_shapes())};
}

std::shared_ptr<arcticdb::proto::descriptors::FrameMetadata> extract_frame_metadata(SegmentInMemory& res) {
    auto output = std::make_shared<arcticdb::proto::descriptors::FrameMetadata>();
    util::check(res.has_metadata(), "Cannot extract frame metadata as it is null");
    res.metadata()->UnpackTo(output.get());
    return output;
}

FrameDescriptorImpl read_frame_descriptor(const uint8_t*& data) {
    auto* frame_descriptor = reinterpret_cast<const FrameDescriptorImpl*>(data);
    data += sizeof(FrameDescriptorImpl);
    return *frame_descriptor;
}

SegmentDescriptorImpl read_segment_descriptor(const uint8_t*& data) {
    util::check_magic<SegmentDescriptorMagic>(data);
    auto* frame_descriptor = reinterpret_cast<const SegmentDescriptorImpl*>(data);
    data += sizeof(SegmentDescriptorImpl);
    return *frame_descriptor;
}

std::shared_ptr<FieldCollection> decode_index_fields(
        const SegmentHeader& hdr, const uint8_t*& data, const uint8_t* begin ARCTICDB_UNUSED, const uint8_t* end
) {
    auto fields = std::make_shared<FieldCollection>();
    if (hdr.has_index_descriptor_field() && hdr.index_descriptor_field().has_ndarray()) {
        ARCTICDB_TRACE(log::codec(), "Decoding index fields");
        util::check(data != end, "Reached end of input block with index descriptor fields to decode");
        std::optional<util::BitMagic> bv;

        data += decode_ndarray(
                FieldCollection::type(),
                hdr.index_descriptor_field().ndarray(),
                data,
                *fields,
                bv,
                hdr.encoding_version()
        );

        ARCTICDB_TRACE(log::codec(), "Decoded index descriptor to position {}", data - begin);
    }
    fields->regenerate_offsets();
    return fields;
}

namespace {
inline arcticdb::proto::descriptors::TimeSeriesDescriptor timeseries_descriptor_from_any(
        const google::protobuf::Any& any
) {
    arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
    any.UnpackTo(&tsd);
    return tsd;
}

inline arcticdb::proto::descriptors::FrameMetadata frame_metadata_from_any(const google::protobuf::Any& any) {
    arcticdb::proto::descriptors::FrameMetadata frame_meta;
    any.UnpackTo(&frame_meta);
    return frame_meta;
}
} // namespace

std::optional<FieldCollection> decode_descriptor_fields(
        const SegmentHeader& hdr, const uint8_t*& data, const uint8_t* begin ARCTICDB_UNUSED, const uint8_t* end
) {
    if (hdr.has_descriptor_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding index fields");
        util::check(data != end, "Reached end of input block with descriptor fields to decode");
        std::optional<util::BitMagic> bv;
        FieldCollection fields;
        data += decode_field(FieldCollection::type(), hdr.descriptor_field(), data, fields, bv, hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded descriptor fields to position {}", data - begin);
        return std::make_optional<FieldCollection>(std::move(fields));
    } else {
        return std::nullopt;
    }
}

TimeseriesDescriptor unpack_timeseries_descriptor_from_proto(
        const google::protobuf::Any& any, const StreamDescriptor& stream_desc, bool is_decoding_incompletes
) {

    auto tsd = timeseries_descriptor_from_any(any);
    if (is_decoding_incompletes) {
        // Prefer the stream descriptor on the segment header for incompletes.
        // See PR #1647.
        arcticc::pb2::descriptors_pb2::StreamDescriptor desc_proto;
        copy_stream_descriptor_to_proto(stream_desc, desc_proto);
        tsd.mutable_stream_descriptor()->CopyFrom(desc_proto);
    }

    auto frame_meta = std::make_shared<arcticdb::proto::descriptors::FrameMetadata>();
    exchange_timeseries_proto(tsd, *frame_meta);

    auto segment_desc =
            std::make_shared<SegmentDescriptorImpl>(segment_descriptor_from_proto((tsd.stream_descriptor())));
    auto frame_desc = std::make_shared<FrameDescriptorImpl>(frame_descriptor_from_proto(tsd));
    const auto& desc = tsd.stream_descriptor();
    auto old_fields = std::make_shared<FieldCollection>(fields_from_proto(tsd.stream_descriptor()));
    StreamId stream_id = desc.id_case() == desc.kNumId ? StreamId(desc.num_id()) : StreamId(desc.str_id());
    return {frame_desc, segment_desc, frame_meta, old_fields, stream_id};
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor_v1(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin, const StreamDescriptor& descriptor
) {
    auto maybe_any = decode_metadata(hdr, data, begin);
    if (!maybe_any)
        return std::nullopt;

    return unpack_timeseries_descriptor_from_proto(*maybe_any, descriptor, false);
}

void skip_descriptor(const uint8_t*& data, const SegmentHeader& hdr) {
    util::check_magic<SegmentDescriptorMagic>(data);
    data += sizeof(SegmentDescriptor);
    skip_identifier(data);
    util::check_magic<DescriptorFieldsMagic>(data);
    if (hdr.has_descriptor_field() && hdr.descriptor_field().has_ndarray())
        data += encoding_sizes::field_compressed_size(hdr.descriptor_field());
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor_v2(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin, const uint8_t* end
) {
    util::check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if (!maybe_any)
        return std::nullopt;

    auto frame_meta =
            std::make_shared<arcticdb::proto::descriptors::FrameMetadata>(frame_metadata_from_any(*maybe_any));

    skip_descriptor(data, hdr);

    util::check_magic<IndexMagic>(data);
    auto frame_desc = std::make_shared<FrameDescriptorImpl>(read_frame_descriptor(data));
    auto segment_desc = std::make_shared<SegmentDescriptorImpl>(read_segment_descriptor(data));
    auto segment_id = read_identifier(data);
    auto index_fields = decode_index_fields(hdr, data, begin, end);
    return std::make_optional<TimeseriesDescriptor>(
            frame_desc, segment_desc, frame_meta, std::move(index_fields), segment_id
    );
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor(
        const SegmentHeader& hdr, const uint8_t* data, const uint8_t* begin, const uint8_t* end,
        const StreamDescriptor& descriptor
) {
    util::check(data != nullptr, "Got null data ptr from segment");
    auto encoding_version = EncodingVersion(hdr.encoding_version());
    if (encoding_version == EncodingVersion::V1)
        return decode_timeseries_descriptor_v1(hdr, data, begin, descriptor);
    else
        return decode_timeseries_descriptor_v2(hdr, data, begin, end);
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor(Segment& segment) {
    const auto& hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = data + segment.buffer().bytes();

    return decode_timeseries_descriptor(hdr, data, begin, end, segment.descriptor());
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor_for_incompletes(
        const SegmentHeader& hdr, const StreamDescriptor& desc, const uint8_t* data, const uint8_t* begin,
        const uint8_t* end
) {
    util::check(data != nullptr, "Got null data ptr from segment");
    auto encoding_version = EncodingVersion(hdr.encoding_version());
    if (encoding_version == EncodingVersion::V1) {
        auto maybe_any = decode_metadata(hdr, data, begin);
        if (!maybe_any)
            return std::nullopt;

        return unpack_timeseries_descriptor_from_proto(*maybe_any, desc, true);
    } else {
        return decode_timeseries_descriptor_v2(hdr, data, begin, end);
    }
}

std::optional<TimeseriesDescriptor> decode_timeseries_descriptor_for_incompletes(Segment& segment) {
    auto& hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    const uint8_t* end = data + segment.buffer().bytes();

    return decode_timeseries_descriptor_for_incompletes(hdr, segment.descriptor(), data, begin, end);
}

std::pair<std::optional<google::protobuf::Any>, StreamDescriptor> decode_metadata_and_descriptor_fields(Segment& segment
) {
    auto& hdr = segment.header();
    const uint8_t* data = segment.buffer().data();

    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;
    if (EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        util::check_magic<MetadataMagic>(data);

    auto maybe_any = decode_metadata(hdr, data, begin);
    if (EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
        util::check_magic<DescriptorFieldsMagic>(data);

    return std::make_pair(std::move(maybe_any), segment.descriptor());
}

void decode_string_pool(
        const SegmentHeader& hdr, const uint8_t*& data, const uint8_t* begin ARCTICDB_UNUSED, const uint8_t* end,
        SegmentInMemory& res
) {
    if (hdr.has_string_pool_field()) {
        ARCTICDB_TRACE(log::codec(), "Decoding string pool");
        interval timer;
        timer.start();
        util::check(data != end, "Reached end of input block with string pool fields to decode");
        std::optional<util::BitMagic> bv;
        data += decode_ndarray(
                string_pool_descriptor().type(),
                hdr.string_pool_field(),
                data,
                res.string_pool(),
                bv,
                hdr.encoding_version()
        );
        timer.end();
        log::codec().debug("Decoded string pool to position {} in {}s", data - begin, timer.get_results_total());
    }
}

ssize_t calculate_last_row(const Column& col) {
    ssize_t last_row{0};
    if (col.opt_sparse_map().has_value()) {
        bm::bvector_size_type last_set_bit;
        if (col.sparse_map().find_reverse(last_set_bit)) {
            last_row = static_cast<ssize_t>(last_set_bit);
        }
    } else {
        last_row = static_cast<ssize_t>(col.row_count()) - 1;
    }
    return last_row;
}

void decode_v2(const Segment& segment, const SegmentHeader& hdr, SegmentInMemory& res, const StreamDescriptor& desc) {
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    if (segment.buffer().data() == nullptr) {
        ARCTICDB_DEBUG(log::codec(), "Segment contains no data in decode_v2");
        return;
    }

    const auto [begin, end] = get_segment_begin_end(segment, hdr);
    auto encoded_fields_ptr = end;
    auto data = begin;
    util::check_magic<MetadataMagic>(data);
    decode_metadata(hdr, data, begin, res);
    skip_descriptor(data, hdr);

    util::check_magic<IndexMagic>(data);
    if (hdr.has_index_descriptor_field()) {
        auto index_frame_descriptor = std::make_shared<FrameDescriptorImpl>(read_frame_descriptor(data));
        auto frame_metadata = extract_frame_metadata(res);
        auto index_segment_descriptor = std::make_shared<SegmentDescriptorImpl>(read_segment_descriptor(data));
        auto index_segment_identifier = read_identifier(data);
        auto index_fields = decode_index_fields(hdr, data, begin, end);
        TimeseriesDescriptor tsd{
                std::move(index_frame_descriptor),
                std::move(index_segment_descriptor),
                std::move(frame_metadata),
                std::move(index_fields),
                index_segment_identifier
        };
        res.set_timeseries_descriptor(tsd);
        res.reset_metadata();
    }

    if (data != end) {
        util::check(hdr.has_column_fields(), "Expected column fields in v2 encoding");
        util::check_magic<EncodedMagic>(encoded_fields_ptr);
        auto encoded_fields_buffer = decode_encoded_fields(hdr, encoded_fields_ptr, begin);
        const auto fields_size = desc.fields().size();
        const auto start_row = res.row_count();
        EncodedFieldCollection encoded_fields(std::move(encoded_fields_buffer));

        auto encoded_field = encoded_fields.begin();
        res.init_column_map();

        ssize_t seg_row_count = 0;
        for (std::size_t i = 0; i < fields_size; ++i) {
#ifdef DUMP_BYTES
            log::version().debug(
                    "{}",
                    dump_bytes(begin, (data - begin) + encoding_sizes::field_compressed_size(*encoded_field), 100u)
            );
#endif
            const auto& field_name = desc.fields(i).name();
            util::check(data != end, "Reached end of input block with {} fields to decode", fields_size - i);
            if (auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));

                data += decode_field(
                        res.field(*col_index).type(),
                        *encoded_field,
                        data,
                        col,
                        col.opt_sparse_map(),
                        hdr.encoding_version()
                );
                col.set_statistics(encoded_field->get_statistics());

                seg_row_count = std::max(seg_row_count, calculate_last_row(col));
            } else {
                data += encoding_sizes::field_compressed_size(*encoded_field) + sizeof(ColumnMagic);
            }
            ++encoded_field;
            ARCTICDB_TRACE(log::codec(), "V2 Decoded column {} to position {}", i, data - begin);
        }

        util::check_magic<StringPoolMagic>(data);
        decode_string_pool(hdr, data, begin, end, res);

        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count));
        res.set_compacted(segment.header().compacted());
    }
}

void decode_v1(
        const Segment& segment, const SegmentHeader& hdr, SegmentInMemory& res, const StreamDescriptor& desc,
        bool is_decoding_incompletes
) {
    ARCTICDB_SAMPLE(DecodeSegment, 0)
    const uint8_t* data = segment.buffer().data();
    if (data == nullptr) {
        ARCTICDB_DEBUG(log::codec(), "Segment contains no data in decode_v1");
        return;
    }

    const uint8_t* begin = data;
    const uint8_t* end = begin + segment.buffer().bytes();
    decode_metadata(hdr, data, begin, res);
    if (res.has_metadata() && res.metadata()->Is<arcticdb::proto::descriptors::TimeSeriesDescriptor>()) {
        ARCTICDB_DEBUG(log::version(), "Unpacking timeseries descriptor from metadata");
        auto tsd = unpack_timeseries_descriptor_from_proto(*res.metadata(), desc, is_decoding_incompletes);
        res.set_timeseries_descriptor(tsd);
        res.reset_metadata();
    }

    if (data != end) {
        const auto fields_size = desc.fields().size();
        const auto& column_fields = hdr.body_fields();
        util::check(
                fields_size == segment.fields_size(),
                "Mismatch between descriptor and header field size: {} != {}",
                fields_size,
                column_fields.size()
        );
        const auto start_row = res.row_count();

        res.init_column_map();

        ssize_t seg_row_count = 0;
        for (std::size_t i = 0; i < fields_size; ++i) {
            const auto& field = column_fields.at(i);
            const auto& desc_field = desc.fields(i);
            const auto& field_name = desc_field.name();
            util::check(
                    data != end || is_empty_type(desc_field.type().data_type()),
                    "Reached end of input block with {} fields to decode",
                    fields_size - i
            );
            if (auto col_index = res.column_index(field_name)) {
                auto& col = res.column(static_cast<position_t>(*col_index));
                data += decode_field(
                        res.field(*col_index).type(), field, data, col, col.opt_sparse_map(), hdr.encoding_version()
                );
                seg_row_count = std::max(seg_row_count, calculate_last_row(col));
                col.set_statistics(field.get_statistics());
                ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", i, data - begin);
            } else {
                data += encoding_sizes::field_compressed_size(field);
                ARCTICDB_TRACE(log::codec(), "Skipped column {}, at position {}", i, data - begin);
            }
        }
        decode_string_pool(hdr, data, begin, end, res);
        res.set_row_data(static_cast<ssize_t>(start_row + seg_row_count));
        res.set_compacted(segment.header().compacted());
    }
}

void decode_into_memory_segment(
        const Segment& segment, SegmentHeader& hdr, SegmentInMemory& res, const StreamDescriptor& desc
) {
    if (EncodingVersion(segment.header().encoding_version()) == EncodingVersion::V2)
        decode_v2(segment, hdr, res, desc);
    else
        decode_v1(segment, hdr, res, desc);
}

SegmentInMemory decode_segment(Segment& segment, AllocationType allocation_type) {
    auto& hdr = segment.header();
    ARCTICDB_TRACE(log::codec(), "Decoding descriptor: {}", segment.descriptor());
    auto descriptor = segment.descriptor();
    descriptor.fields().regenerate_offsets();
    ARCTICDB_TRACE(log::codec(), "Creating segment");
    SegmentInMemory res(std::move(descriptor), 0, allocation_type);
    ARCTICDB_TRACE(log::codec(), "Decoding segment");
    decode_into_memory_segment(segment, hdr, res, res.descriptor());
    ARCTICDB_TRACE(log::codec(), "Returning segment");
    return res;
}

template<typename EncodedFieldType>
void hash_field(const EncodedFieldType& field, HashAccum& accum) {
    auto& n = field.ndarray();
    for (auto i = 0; i < n.shapes_size(); ++i) {
        auto v = n.shapes(i).hash();
        accum(&v);
    }

    for (auto j = 0; j < n.values_size(); ++j) {
        auto v = n.values(j).hash();
        accum(&v);
    }
}

HashedValue get_segment_hash(Segment& seg) {
    HashAccum accum;
    const auto& fields = seg.fields_ptr();
    if (fields && !fields->empty()) {
        hash_buffer(fields->buffer(), accum);
    }

    const auto& hdr = seg.header();
    if (hdr.encoding_version() == EncodingVersion::V1) {
        // The hashes are part of the encoded fields protobuf in the v1 header, which is not
        // ideal but needs to be maintained for consistency
        const auto& proto = seg.generate_header_proto();
        if (proto.has_metadata_field()) {
            hash_field(proto.metadata_field(), accum);
        }
        for (int i = 0; i < proto.fields_size(); ++i) {
            hash_field(proto.fields(i), accum);
        }
        if (hdr.has_string_pool_field()) {
            hash_field(proto.string_pool_field(), accum);
        }
    } else {
        const auto& header_fields = hdr.header_fields();
        for (auto i = 0UL; i < header_fields.size(); ++i) {
            hash_field(header_fields.at(i), accum);
        }

        const auto& body_fields = hdr.body_fields();
        for (auto i = 0UL; i < body_fields.size(); ++i) {
            hash_field(body_fields.at(i), accum);
        }
    }

    return accum.digest();
}

void add_bitmagic_compressed_size(
        const ColumnData& column_data, size_t& max_compressed_bytes, size_t& uncompressed_bytes
) {
    if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0) {
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
    bm::serializer<bm::bvector<>> bvs; // TODO: It is inefficient to create the serializer every time.
    bm::bvector<>::statistics st;
    sparse_map.calc_stat(&st);
    auto total_max_size = st.max_serialize_mem + util::combined_bit_magic_delimiters_size();
    out.assert_size(pos + total_max_size);
    uint8_t* target = out.data() + pos;
    util::write_magic<util::BitMagicStart>(target);
    auto sz = bvs.serialize(sparse_map, target, st.max_serialize_mem);
    target += sz;
    util::write_magic<util::BitMagicEnd>(target);
    auto total_sz = sz + util::combined_bit_magic_delimiters_size();
    pos = pos + static_cast<ptrdiff_t>(total_sz);
    return total_sz;
}

void encode_sparse_map(ColumnData& column_data, EncodedFieldImpl& field, Buffer& out, std::ptrdiff_t& pos) {
    if (column_data.bit_vector() != nullptr && column_data.bit_vector()->count() > 0) {
        util::check(!is_empty_type(column_data.type().data_type()), "Empty typed columns should not have sparse maps");
        ARCTICDB_DEBUG(log::codec(), "Sparse map count = {} pos = {}", column_data.bit_vector()->count(), pos);
        const size_t sparse_bm_bytes = encode_bitmap(*column_data.bit_vector(), out, pos);
        field.mutable_ndarray()->set_sparse_map_bytes(static_cast<int>(sparse_bm_bytes));
    }
}
} // namespace arcticdb
