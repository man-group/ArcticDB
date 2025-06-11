#pragma once

#include <map>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/encoded_field.hpp>

namespace arcticdb {
struct ColumnEncoderV1 {
    static std::pair<size_t, size_t> max_compressed_size(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data);

    static void encode(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldImpl& variant_field,
        Buffer& out,
        std::ptrdiff_t& pos);
};

}