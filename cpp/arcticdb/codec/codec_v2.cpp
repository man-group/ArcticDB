/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec_v2.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/codec_utils.hpp>
#include <arcticdb/codec/typed_block_encoder_impl.hpp>
#include <arcticdb/column_store/column_data.hpp>

#include <utility>

namespace arcticdb {
    using ShapesBlockTDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;
    using ShapesBlock = TypedBlockData<ShapesBlockTDT>;

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

    void ColumnEncoderV2::encode(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    ) {
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
            ShapesBlock shapes_block = create_shapes_typed_block(column_data);
            util::variant_match(variant_field, [&](auto field){
                using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, ShapesBlockTDT, EncodingVersion::V2>;
                ShapesEncoder::encode_shapes(shapes_encoding_opts(), shapes_block, *field, out, pos_in_buffer);
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
                max_compressed_bytes += Encoder::max_compressed_size(codec_opts, block.value());
            }
            add_bitmagic_compressed_size(column_data, uncompressed_bytes, max_compressed_bytes);
            return std::make_pair(uncompressed_bytes, max_compressed_bytes);
        });
    }
}
