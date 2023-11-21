/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec_v1.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/codec_utils.hpp>
#include <arcticdb/codec/typed_block_encoder_impl.hpp>
#include <arcticdb/column_store/column_data.hpp>

#include <utility>
namespace arcticdb {
    std::pair<size_t, size_t> ColumnEncoderV1::max_compressed_size(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data
    ) {
        return column_data.type().visit_tag([&codec_opts, &column_data](auto type_desc_tag) {
            size_t max_compressed_bytes = 0;
            size_t uncompressed_bytes = 0;
            using TDT = decltype(type_desc_tag);
            using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V1>;
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
            add_bitmagic_compressed_size(column_data, uncompressed_bytes, max_compressed_bytes);
            return std::make_pair(uncompressed_bytes, max_compressed_bytes);
        });
    }

    void ColumnEncoderV1::encode(
        const arcticdb::proto::encoding::VariantCodec& codec_opts,
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    ) {
        column_data.type().visit_tag([&](auto type_desc_tag) {
            using TDT = decltype(type_desc_tag);
            using Encoder = TypedBlockEncoderImpl<TypedBlockData, TDT, EncodingVersion::V1>;
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
        encode_sparse_map(column_data, variant_field, out, pos);
    }
}
