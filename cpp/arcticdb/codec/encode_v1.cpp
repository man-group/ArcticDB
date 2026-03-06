/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/codec/encode_common.hpp>
#include <arcticdb/codec/typed_block_encoder_impl.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {
void add_bitmagic_compressed_size(
        const ColumnData& column_data, size_t& max_compressed_bytes, size_t& uncompressed_bytes
);

void encode_sparse_map(ColumnData& column_data, EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos);

/// @brief Utility class used to encode and compute the max encoding size for regular data columns for V1 encoding
struct ColumnEncoderV1 {
    static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data
    );

    static void encode(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data,
            EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos
    );
};

std::pair<size_t, size_t> ColumnEncoderV1::max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data
) {
    return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
        size_t max_compressed_bytes = 0;
        size_t uncompressed_bytes = 0;
        using TDT = decltype(type_desc_tag);
        using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V1>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        while (auto block = column_data.next<TDT>()) {
            const auto nbytes = block->nbytes();
            if constexpr (must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(nbytes > 0, "Zero-sized block");
            }
            uncompressed_bytes += nbytes;
            // For the empty type the column will contain 0 size of user data however the encoder might need add some
            // encoder specific data to the buffer, thus the uncompressed size will be 0 but the max_compressed_bytes
            // might be non-zero.
            max_compressed_bytes += Encoder::max_compressed_size(codec_opts, *block);
        }
        add_bitmagic_compressed_size(column_data, max_compressed_bytes, uncompressed_bytes);
        return std::make_pair(uncompressed_bytes, max_compressed_bytes);
    });
}

void ColumnEncoderV1::encode(
        const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data, EncodedFieldImpl& field,
        Buffer& out, std::ptrdiff_t& pos
) {
    column_data.type().visit_tag([&codec_opts, &column_data, &field, &out, &pos](auto type_desc_tag) {
        using TDT = decltype(type_desc_tag);
        using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V1>;
        ARCTICDB_TRACE(log::codec(), "Column data has {} blocks", column_data.num_blocks());
        while (auto block = column_data.next<TDT>()) {
            if constexpr (must_contain_data(static_cast<TypeDescriptor>(type_desc_tag))) {
                util::check(block->nbytes() > 0, "Zero-sized block");
            }
            Encoder::encode(codec_opts, *block, field, out, pos);
        }
    });
    encode_sparse_map(column_data, field, out, pos);
}

using EncodingPolicyV1 = EncodingPolicyType<EncodingVersion::V1, ColumnEncoderV1>;

[[nodiscard]] SizeResult max_compressed_size_v1(
        const SegmentInMemory& in_mem_seg, const arcticdb::proto::encoding::VariantCodec& codec_opts
) {
    ARCTICDB_SAMPLE(GetSegmentCompressedSize, 0)
    SizeResult result{};
    calc_metadata_size<EncodingPolicyV1>(in_mem_seg, codec_opts, result);

    if (in_mem_seg.row_count() > 0) {
        calc_columns_size<EncodingPolicyV1>(in_mem_seg, codec_opts, result);
        calc_string_pool_size<EncodingPolicyV1>(in_mem_seg, codec_opts, result);
    }
    ARCTICDB_TRACE(log::codec(), "Max compressed size {}", result.max_compressed_bytes_);
    return result;
}

/*
 * This takes an in memory segment with all the metadata, column tensors etc., loops through each column
 * and based on the type of the column, calls the typed block encoder for that column.
 */
[[nodiscard]] Segment encode_v1(SegmentInMemory&& s, const arcticdb::proto::encoding::VariantCodec& codec_opts) {
    ARCTICDB_SAMPLE(EncodeSegment, 0)
    auto in_mem_seg = std::move(s);
    SegmentHeader segment_header{EncodingVersion::V1};
    segment_header.set_compacted(in_mem_seg.compacted());

    if (in_mem_seg.has_index_descriptor()) {
        ARCTICDB_TRACE(log::version(), "Memory segment has index descriptor, encoding to protobuf");
        util::check(!in_mem_seg.has_metadata(), "Metadata already set when trying to set index descriptor");
        auto proto = copy_time_series_descriptor_to_proto(in_mem_seg.index_descriptor());
        google::protobuf::Any any;
        any.PackFrom(proto);
        in_mem_seg.set_metadata(std::move(any));
    }

    std::ptrdiff_t pos = 0;
    static auto block_to_header_ratio = ConfigsMap::instance()->get_int("Codec.EstimatedHeaderRatio", 75);
    const auto preamble = in_mem_seg.num_blocks() * block_to_header_ratio;
    auto [max_compressed_size, uncompressed_size, encoded_buffer_size] = max_compressed_size_v1(in_mem_seg, codec_opts);
    ARCTICDB_TRACE(log::codec(), "Estimated max buffer requirement: {}", max_compressed_size);
    auto out_buffer = std::make_shared<Buffer>(max_compressed_size, preamble);
    ColumnEncoderV1 encoder;

    encode_metadata<EncodingPolicyV1>(in_mem_seg, segment_header, codec_opts, *out_buffer, pos);
    ARCTICDB_TRACE(log::codec(), "Encoding descriptor: {}", in_mem_seg.descriptor());
    auto descriptor_data = in_mem_seg.descriptor().data_ptr();
    descriptor_data->uncompressed_bytes_ = uncompressed_size;

    EncodedFieldCollection encoded_fields;
    if (in_mem_seg.row_count() > 0) {
        encoded_fields.reserve(encoded_buffer_size, in_mem_seg.num_columns());
        ARCTICDB_TRACE(log::codec(), "Encoding fields");
        for (std::size_t column_index = 0; column_index < in_mem_seg.num_columns(); ++column_index) {
            const auto& column = in_mem_seg.column(column_index);
            util::check(
                    !is_arrow_output_only_type(column.type()),
                    "Attempts to encode an output only type {}",
                    column.type()
            );
            auto column_data = column.data();
            auto* column_field = encoded_fields.add_field(column_data.num_blocks());
            if (column_data.num_blocks() > 0) {
                encoder.encode(codec_opts, column_data, *column_field, *out_buffer, pos);
                ARCTICDB_TRACE(
                        log::codec(),
                        "Encoded column {}: ({}) to position {}",
                        column_index,
                        in_mem_seg.descriptor().fields(column_index).name(),
                        pos
                );
            } else {
                util::check(
                        !must_contain_data(column_data.type()) || column_data.bit_vector() != nullptr,
                        "Column {} of type {} contains no blocks and is not sparse",
                        column_index,
                        column_data.type()
                );
                auto* ndarray = column_field->mutable_ndarray();
                ndarray->set_items_count(0);
                encode_sparse_map(column_data, *column_field, *out_buffer, pos);
            }
            column_field->set_statistics(column.get_statistics());
        }
        encode_string_pool<EncodingPolicyV1>(in_mem_seg, segment_header, codec_opts, *out_buffer, pos);
    }
    segment_header.set_body_fields(EncodedFieldCollection(std::move(encoded_fields)));
    ARCTICDB_TRACE(log::codec(), "Encode setting buffer bytes to {}", pos);
    out_buffer->set_bytes(pos);
    descriptor_data->compressed_bytes_ = pos;
    descriptor_data->row_count_ = in_mem_seg.row_count();
    return Segment::initialize(
            std::move(segment_header),
            std::move(out_buffer),
            descriptor_data,
            in_mem_seg.descriptor().fields_ptr(),
            in_mem_seg.descriptor().id()
    );
}
} // namespace arcticdb
