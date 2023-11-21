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

    /// @brief Utility class used to encode and compute the max encoding size for regular data columns for V1 encoding
    struct ColumnEncoderV1 {
        static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts,
            ColumnData& column_data);

        static void encode(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
    };
}
