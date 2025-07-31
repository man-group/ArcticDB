/* Copyright 2025 Man Group Operations Limited
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
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            m.source_type_desc_.data_type() == DataType::UTF_DYNAMIC64,
            "Cannot read column '{}' into Arrow output format as it is of unsupported type {} (only {} is supported)",
            m.frame_field_descriptor_.name(), m.source_type_desc_.data_type(), DataType::UTF_DYNAMIC64);
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
    using ArcticStringColumnTag = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;
    auto input_data = source_column.data();
    auto pos = input_data.cbegin<ArcticStringColumnTag>();
    const auto end = input_data.cend<ArcticStringColumnTag>();

    struct DictEntry {
        int32_t offset_buffer_pos_;
        int64_t string_buffer_pos_;
    };
    std::vector<StringPool::offset_t> unique_offsets_in_order;
    ankerl::unordered_dense::map<StringPool::offset_t, DictEntry> unique_offsets;
    int64_t bytes = 0;
    auto dest_ptr = reinterpret_cast<int32_t*>(dest_column.bytes_at(mapping.offset_bytes_, source_column.row_count() * sizeof(int32_t)));

    // First go through the source column once to compute the size of offset and string buffers.
    while(pos != end) {
        auto [entry, is_emplaced] = unique_offsets.try_emplace(*pos, DictEntry{static_cast<int32_t>(unique_offsets_in_order.size()), bytes});
        if(is_emplaced) {
            bytes += string_pool->get_const_view(*pos).size();
            unique_offsets_in_order.push_back(*pos);
        }
        ++pos;
        *dest_ptr = entry->second.offset_buffer_pos_;
        ++dest_ptr;
    }
    auto& string_buffer = dest_column.create_extra_buffer(mapping.offset_bytes_, ExtraBufferType::STRING, bytes, AllocationType::DETACHABLE);
    auto& offsets_buffer = dest_column.create_extra_buffer(mapping.offset_bytes_, ExtraBufferType::OFFSET, (unique_offsets_in_order.size() + 1) * sizeof(int64_t), AllocationType::DETACHABLE);

    // Then go through unique_offsets to fill up the offset and string buffers.
    auto offsets_ptr = reinterpret_cast<int64_t*>(offsets_buffer.data());
    auto string_ptr = reinterpret_cast<char*>(string_buffer.data());
    auto string_begin_ptr = string_ptr;
    for(auto i=0u; i<unique_offsets_in_order.size(); ++i) {
        auto string_pool_offset = unique_offsets_in_order[i];
        auto& entry = unique_offsets[string_pool_offset];
        util::check(static_cast<int32_t>(i) == entry.offset_buffer_pos_, "Mismatch in offset buffer pos");
        util::check(string_ptr - string_begin_ptr == entry.string_buffer_pos_, "Mismatch in string buffer pos");
        offsets_ptr[i] = entry.string_buffer_pos_;
        const auto strv = string_pool->get_const_view(string_pool_offset);
        memcpy(string_ptr, strv.data(), strv.size());
        string_ptr += strv.size();
    }
    offsets_ptr[unique_offsets_in_order.size()] = bytes;
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