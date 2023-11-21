/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/protobufs.hpp>
#include <cstddef>
#include <variant>

namespace std {
    template<typename, typename> class pair;
}

namespace arcticdb {
    class ColumnData;
    class Buffer;
    class EncodedField;

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
}
