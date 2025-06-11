#pragma once

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>


#include <cstddef>
#include <cstdint>

namespace arcticdb {

struct ColumnEncoderV2 {
public:
    static void encode(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos,
        std::optional<EncodingScanResult> values_result,
        std::optional<EncodingScanResult> shapes_result);

    static std::pair<size_t, size_t> max_compressed_size(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data);


    static EncodedFieldImpl* add_field(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldCollection& fields);

private:
    static void encode_shapes(
        const ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos_in_buffer,
        std::optional<EncodingScanResult> scan_result);

    static void encode_blocks(
        const BlockCodecImpl& codec_opts,
        ColumnData& column_data,
        EncodedFieldImpl& field,
        Buffer& out,
        std::ptrdiff_t& pos,
        std::optional<EncodingScanResult> scan_result);
};
} // namespace arcticdb