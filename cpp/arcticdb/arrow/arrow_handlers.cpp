/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arrow/arrow_handlers.hpp>
#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/lambda_inlining.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>

namespace arcticdb {
ArrowOutputStringFormat ArrowStringHandler::output_string_format(
        std::string_view column_name, const ReadOptions& read_options
) const {
    const auto& arrow_config = read_options.arrow_output_config();
    if (auto it = arrow_config.per_column_string_format_.find(std::string{column_name});
        it != arrow_config.per_column_string_format_.end()) {
        return it->second;
    }
    return arrow_config.default_string_format_;
}

void ArrowStringHandler::handle_type(
        const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
        const DecodePathData& shared_data, std::any& handler_data, EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE(ArrowHandleString, 0)
    util::check(field.has_ndarray(), "String handler expected array");
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            m.source_type_desc_.data_type() == DataType::UTF_DYNAMIC64,
            "Cannot read column '{}' into Arrow output format as it is of unsupported type {} (only {} is supported)",
            m.frame_field_descriptor_.name(),
            m.source_type_desc_.data_type(),
            DataType::UTF_DYNAMIC64
    );
    ARCTICDB_DEBUG(log::version(), "String handler got encoded field: {}", field.DebugString());
    const auto& ndarray = field.ndarray();
    const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);

    Column decoded_data{
            m.source_type_desc_,
            bytes / get_type_size(m.source_type_desc_.data_type()),
            AllocationType::DYNAMIC,
            Sparsity::PERMITTED
    };

    data += decode_field(
            m.source_type_desc_, field, data, decoded_data, decoded_data.opt_sparse_map(), encoding_version
    );

    convert_type(decoded_data, dest_column, m, shared_data, handler_data, string_pool, read_options);
}

template<typename OffsetType>
void encode_variable_length(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
        const std::shared_ptr<StringPool>& string_pool
) {
    using ArcticStringColumnTag = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;

    int64_t bytes = 0u;
    auto last_idx = 0u;

    // `mapping.dest_bytes` does not count the extra offset value we need to store in the end.
    // Thus, we request one extra value to assert that the buffer has that extra value allocated.
    auto dest_ptr = reinterpret_cast<OffsetType*>(
            dest_column.bytes_at(mapping.offset_bytes_, mapping.dest_bytes_ + sizeof(OffsetType))
    );
    std::vector<std::string_view> strings;
    util::BitSet dest_bitset;
    util::BitSet::bulk_insert_iterator inserter(dest_bitset);
    bool populate_inverted_bitset = !source_column.opt_sparse_map().has_value();
    for_each_enumerated<ArcticStringColumnTag>(source_column, [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
        if (is_a_string(en.value())) {
            auto strv = string_pool->get_const_view(en.value());
            while (last_idx <= en.idx()) {
                // According to arrow spec offsets must be monotonic even for null values.
                // Hence, we fill missing values between `last_idx` and `en.idx` so that they contain 0 rows in
                // string buffer
                dest_ptr[last_idx++] = static_cast<OffsetType>(bytes);
            }
            bytes += strv.size();
            strings.emplace_back(std::move(strv));
            if (!populate_inverted_bitset) {
                inserter = en.idx();
            }
        } else if (populate_inverted_bitset) {
            inserter = en.idx();
        }
    });
    while (last_idx <= mapping.num_rows_) {
        dest_ptr[last_idx++] = static_cast<OffsetType>(bytes);
    }
    // The below assertion can only break for `SMALL_STRING` because `OffsetType=int32_t` and we can have more than 2^32
    // bytes in memory. The same could not happen for `LARGE_STRING` where `OffsetType=int64=typeof(bytes)`.
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            bytes <= std::numeric_limits<OffsetType>::max(),
            "The strings for column {} require {} bytes > {} bytes supported by requested string output format. "
            "Consider using LARGE_STRING or CATEGORICAL.",
            mapping.frame_field_descriptor_.name(),
            bytes,
            std::numeric_limits<OffsetType>::max()
    );
    inserter.flush();
    if (populate_inverted_bitset) {
        // For dense columns at this point bitset has ones where the source column is missing values.
        // We invert it back, so we can use it for the sparse map on the output column.
        dest_bitset.invert();
    }
    dest_bitset.resize(mapping.num_rows_);

    if (dest_bitset.count() > 0) {
        auto& string_buffer = dest_column.create_extra_buffer(
                mapping.offset_bytes_, ExtraBufferType::STRING, bytes, AllocationType::DETACHABLE
        );
        auto string_ptr = reinterpret_cast<char*>(string_buffer.data());
        for (auto strv : strings) {
            memcpy(string_ptr, strv.data(), strv.size());
            string_ptr += strv.size();
        }
    }

    // We explicitly truncate the bitset after creation of the string buffer, because in the case where the truncated
    // bitset is all False, we still should allocate a string buffer, so that the offsets for the nulls are valid.
    if (dest_bitset.count() != dest_bitset.size()) {
        handle_truncation(dest_bitset, mapping.truncate_);
        create_dense_bitmap(mapping.offset_bytes_, dest_bitset, dest_column, AllocationType::DETACHABLE);
    } // else there weren't any missing values
}

void encode_dictionary(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
        const std::shared_ptr<StringPool>& string_pool
) {
    using ArcticStringColumnTag = ScalarTagType<DataTypeTag<DataType::UTF_DYNAMIC64>>;
    struct DictEntry {
        int32_t offset_buffer_pos_;
        int64_t string_buffer_pos_;
        std::string_view strv;
    };
    std::vector<StringPool::offset_t> unique_offsets_in_order;
    ankerl::unordered_dense::map<StringPool::offset_t, DictEntry> unique_offsets;
    // Trade some memory for more performance
    // TODO: Use unique count column stat in V2 encoding
    unique_offsets_in_order.reserve(source_column.row_count());
    unique_offsets.reserve(source_column.row_count());
    int64_t bytes = 0;
    int32_t unique_offset_count = 0;
    auto dest_ptr = reinterpret_cast<int32_t*>(dest_column.bytes_at(mapping.offset_bytes_, mapping.dest_bytes_));

    util::BitSet dest_bitset;
    util::BitSet::bulk_insert_iterator inserter(dest_bitset);
    // For dense columns we populate an inverted bitmap (i.e. set the bits where values are missing)
    // This makes processing fully dense columns faster because it doesn't need to insert often into the bitmap.
    // Benchmarks in benchmark_arrow_reads.cpp show a 20% speedup with this approach for a dense string column with few
    // unique strings.
    bool populate_inverted_bitset = !source_column.opt_sparse_map().has_value();
    for_each_enumerated<ArcticStringColumnTag>(source_column, [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
        if (is_a_string(en.value())) {
            auto [entry, is_emplaced] = unique_offsets.try_emplace(
                    en.value(), DictEntry{unique_offset_count, bytes, string_pool->get_const_view(en.value())}
            );
            if (is_emplaced) {
                bytes += entry->second.strv.size();
                unique_offsets_in_order.push_back(en.value());
                ++unique_offset_count;
            }
            dest_ptr[en.idx()] = entry->second.offset_buffer_pos_;
            if (!populate_inverted_bitset) {
                inserter = en.idx();
            }
        } else if (populate_inverted_bitset) {
            inserter = en.idx();
        }
    });
    inserter.flush();
    if (populate_inverted_bitset) {
        // For dense columns at this point bitset has ones where the source column is missing values.
        // We invert it back, so we can use it for the sparse map on the output column.
        dest_bitset.invert();
    }
    dest_bitset.resize(mapping.num_rows_);

    if (dest_bitset.count() != dest_bitset.size()) {
        handle_truncation(dest_bitset, mapping.truncate_);
        create_dense_bitmap(mapping.offset_bytes_, dest_bitset, dest_column, AllocationType::DETACHABLE);
    } // else there weren't any missing values
    // bitset.count() == 0 is the special case where all the rows were missing. In this case, do not create
    // the extra string and offset buffers. string_dict_from_block will then do the right thing and call
    // minimal_strings_dict
    if (dest_bitset.count() > 0) {
        auto& string_buffer = dest_column.create_extra_buffer(
                mapping.offset_bytes_, ExtraBufferType::STRING, bytes, AllocationType::DETACHABLE
        );
        auto& offsets_buffer = dest_column.create_extra_buffer(
                mapping.offset_bytes_,
                ExtraBufferType::OFFSET,
                (unique_offsets_in_order.size() + 1) * sizeof(int64_t),
                AllocationType::DETACHABLE
        );
        // Then go through unique_offsets to fill up the offset and string buffers.
        auto offsets_ptr = reinterpret_cast<int64_t*>(offsets_buffer.data());
        auto string_ptr = reinterpret_cast<char*>(string_buffer.data());
        for (auto unique_offset : unique_offsets_in_order) {
            const auto& entry = unique_offsets[unique_offset];
            *offsets_ptr++ = entry.string_buffer_pos_;
            memcpy(string_ptr, entry.strv.data(), entry.strv.size());
            string_ptr += entry.strv.size();
        }
        *offsets_ptr = bytes;
    }
}

void ArrowStringHandler::convert_type(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping, const DecodePathData&,
        std::any&, const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
) const {
    auto string_format = output_string_format(mapping.frame_field_descriptor_.name(), read_options);
    switch (string_format) {
    case ArrowOutputStringFormat::CATEGORICAL:
        encode_dictionary(source_column, dest_column, mapping, string_pool);
        break;
    case ArrowOutputStringFormat::LARGE_STRING:
        encode_variable_length<int64_t>(source_column, dest_column, mapping, string_pool);
        break;
    case ArrowOutputStringFormat::SMALL_STRING:
        encode_variable_length<int32_t>(source_column, dest_column, mapping, string_pool);
        break;
    }
}

std::pair<TypeDescriptor, size_t> ArrowStringHandler::output_type_and_extra_bytes(
        const TypeDescriptor&, std::string_view column_name, const ReadOptions& read_options
) const {
    auto string_format = output_string_format(column_name, read_options);
    switch (string_format) {
    case ArrowOutputStringFormat::CATEGORICAL:
        return {make_scalar_type(DataType::UTF_DYNAMIC32), 0};
    case ArrowOutputStringFormat::LARGE_STRING:
        return {make_scalar_type(DataType::UTF_DYNAMIC64), sizeof(int64_t)};
    case ArrowOutputStringFormat::SMALL_STRING:
        return {make_scalar_type(DataType::UTF_DYNAMIC32), sizeof(int32_t)};
    default:
        util::raise_rte("Unknown arrow string output format {}", static_cast<int32_t>(string_format));
    }
}

void ArrowStringHandler::default_initialize(
        ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& /*shared_data*/,
        std::any& /*handler_data*/
) const {
    if (!buffer.has_extra_bytes_per_block()) {
        // For categorical string columns the extra_bytes_per_block won't be set.
        // Categorical string columns can contain any values as long as they are marked as 0.
        return;
    }
    // Large or small string columns must be default initialized because monotonicity is required even for null values.
    // `default_initialize` is called only on entire row slices so it is ok to memset buffers with 0s
    auto dest_ptrs_and_sizes = buffer.byte_blocks_at(offset, byte_size);
    for (auto& [dest_ptr, bytes] : dest_ptrs_and_sizes) {
        auto bytes_with_extra = bytes + buffer.extra_bytes_per_block();
        memset(dest_ptr, 0, bytes_with_extra);
    }
}

} // namespace arcticdb