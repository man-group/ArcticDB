/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec_utils.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/util/bitset.hpp>
#include <bitmagic/bmsparsevec_serial.h>


namespace arcticdb {
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
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
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
}
