/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#include <arrow/arrow_handlers.hpp>
#include <arcticdb/codec/slice_data_sink.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/sparse_utils.hpp>

namespace arcticdb {

void ArrowStringHandler::handle_type(
    const uint8_t *&data,
    Column& dest_column,
    const EncodedFieldImpl &field,
    const ColumnMapping& m,
    const DecodePathData& shared_data,
    std::any& handler_data,
    EncodingVersion encoding_version,
    const std::shared_ptr<StringPool>& string_pool) {
    ARCTICDB_SAMPLE(ArrowHandleString, 0)
    util::check(field.has_ndarray(), "String handler expected array");
    ARCTICDB_DEBUG(log::version(), "String handler got encoded field: {}", field.DebugString());
    const auto &ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);

    Column decoded_data{m.source_type_desc_, bytes / get_type_size(m.source_type_desc_.data_type()),
                                       AllocationType::DYNAMIC, Sparsity::PERMITTED};


    data += decode_field(m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version);

    convert_type(
        decoded_data,
        dest_column,
        m,
        shared_data,
        handler_data,
        string_pool);
}

void ArrowStringHandler::convert_type(
    const Column& source_column,
    Column& dest_column,
    const ColumnMapping& mapping,
    const DecodePathData&,
    std::any&,
    const std::shared_ptr<StringPool>& string_pool) const {
    size_t bytes = 0;
    using ArcticStringColumnTag = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;
    auto offset_ptr = reinterpret_cast<uint32_t*>(dest_column.bytes_at(mapping.offset_bytes_, source_column.row_count() * sizeof(uint32_t)));
    auto input_data = source_column.data();
    auto pos = input_data.cbegin<ArcticStringColumnTag>();
    const auto end = input_data.cend<ArcticStringColumnTag>();
    while(pos != end) {
        *offset_ptr = bytes;
        bytes += string_pool->get_view(*pos).size();
        ++pos;
        ++offset_ptr;
    }
    *offset_ptr = bytes;

    auto& buffer = dest_column.create_extra_buffer(mapping.offset_bytes_, bytes, AllocationType::DETACHABLE);

    input_data = source_column.data();
    pos = input_data.cbegin<ArcticStringColumnTag>();
    auto strv_ptr = buffer.data();
    while(pos != end) {
        const auto strv = string_pool->get_view(*pos);
        memcpy(strv_ptr, strv.data(), strv.size());
        strv_ptr += strv.size();
        ++pos;
    };
}

TypeDescriptor ArrowStringHandler::output_type(const TypeDescriptor&) const {
    return make_scalar_type(DataType::UTF_DYNAMIC32);
}

int ArrowStringHandler::type_size() const {
    return sizeof(uint32_t);
}

void ArrowStringHandler::default_initialize(
    ChunkedBuffer& /*buffer*/,
    size_t /*offset*/,
    size_t /*byte_size*/,
    const DecodePathData& /*shared_data*/,
    std::any& /*handler_data*/) const {

}

} // namespace arcticdb