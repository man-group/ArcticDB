/* Copyright 2026 Man Group Operations Limited
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
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {
ArrowOutputStringFormat ArrowStringHandler::output_string_format(
        std::string_view column_name, const ReadOptions& read_options
) const {
    const auto& arrow_config = read_options.arrow_output_config();
    if (auto it = arrow_config.per_column_string_format_.find(std::string{column_name});
        it != arrow_config.per_column_string_format_.end()) {
        return it->second;
    } else {
        // This could give the incorrect return type if:
        //  - The user has explicitly named a column "__idx__blah"
        //  - The user has NOT specified a string return type for column "__idx__blah"
        //  - The user HAS specified a string return type for column "blah"
        // This seems vanishingly unlikely, and to fix would require pushing knowledge of Pandas multiindex field counts
        // and fake field positions deep into the decoding pipeline, which is architecturally undesirable if it can be
        // avoided.
        // See test_explicit_string_format__idx__prefix and issue 10679807500
        auto multiindex_column_name = stream::demangled_name(column_name);
        if (multiindex_column_name.has_value()) {
            if (auto multiindex_it = arrow_config.per_column_string_format_.find(std::string(*multiindex_column_name));
                multiindex_it != arrow_config.per_column_string_format_.end()) {
                return multiindex_it->second;
            }
        }
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
    // ASCII would probably "just work", but it is impossible to test from the Python layer these days
    // Not sure how much data has been written with these types either, so prefer to see if anybody complains about it
    // not working before putting effort into making it work and tested
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            m.source_type_desc_.data_type() == DataType::UTF_DYNAMIC64 ||
                    m.source_type_desc_.data_type() == DataType::UTF_FIXED64,
            "Cannot read column '{}' into Arrow output format as it is of unsupported type {} (only {} and {} are "
            "supported)",
            m.frame_field_descriptor_.name(),
            m.source_type_desc_.data_type(),
            DataType::UTF_DYNAMIC64,
            DataType::UTF_FIXED64
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

struct PositionsAfterTruncation {
    std::optional<size_t> first_idx_after_truncation;
    std::optional<size_t> end_idx_after_truncation;
    size_t extra_buffer_position;
};

PositionsAfterTruncation get_positions_after_truncation(const ColumnMapping& mapping) {
    // If truncation is required, we need to calculate the starting index in the data we are writing after truncation.
    // `truncate_.start_` and `truncate_.end_` are in terms of the full segment, so we need to adjust them to indices in
    // the segment slice we are reading. For example, if we are reading rows 20-29 (mapping.offset_bytes_ = 20 *
    // dest_size_) and truncate_.start_ = 22, we need to start writing from index 2 in our data (22 - 20 = 2).
    std::optional<size_t> first_idx_after_truncation = std::nullopt;
    if (mapping.truncate_.start_.has_value()) {
        first_idx_after_truncation = mapping.truncate_.start_.value() - mapping.offset_bytes_ / mapping.dest_size_;
    }
    std::optional<size_t> end_idx_after_truncation = std::nullopt;
    if (mapping.truncate_.end_.has_value()) {
        end_idx_after_truncation = mapping.truncate_.end_.value() - mapping.offset_bytes_ / mapping.dest_size_;
    }
    // The extra buffer position needs to correspond to the byte offset of the first index after truncation with respect
    // to the full segment.
    auto extra_buffer_position = mapping.offset_bytes_ + first_idx_after_truncation.value_or(0) * mapping.dest_size_;
    return {first_idx_after_truncation, end_idx_after_truncation, extra_buffer_position};
}

template<typename OffsetType>
void encode_variable_length(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
        const std::shared_ptr<StringPool>& string_pool
) {
    const auto positions = get_positions_after_truncation(mapping);

    int64_t bytes = 0u;
    ssize_t last_idx = positions.first_idx_after_truncation.value_or(0);

    // `mapping.dest_bytes` does not count the extra offset value we need to store in the end.
    // Thus, we request one extra value to assert that the buffer has that extra value allocated.
    auto dest_ptr = reinterpret_cast<OffsetType*>(
            dest_column.bytes_at(mapping.offset_bytes_, mapping.dest_bytes_ + mapping.dest_size_)
    );
    util::BitSet dest_bitset;
    util::BitSet::bulk_insert_iterator inserter(dest_bitset);
    bool populate_inverted_bitset = !source_column.opt_sparse_map().has_value();
    auto strings = details::visit_type(
            source_column.type().data_type(),
            [&](auto source_tag) -> std::variant<std::vector<std::string_view>, std::vector<std::string>> {
                using source_type_info = ScalarTypeInfo<decltype(source_tag)>;
                if constexpr (is_sequence_type(source_type_info::data_type)) {
                    if constexpr (is_dynamic_string_type(source_type_info::data_type)) {
                        std::vector<std::string_view> strings;
                        for_each_enumerated<typename source_type_info::TDT>(
                                source_column,
                                [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
                                    if (is_a_string(en.value())) {
                                        while (last_idx <= en.idx()) {
                                            // According to arrow spec offsets must be monotonic even for null values.
                                            // Hence, we fill missing values between `last_idx` and `en.idx` so that
                                            // they contain 0 rows in string buffer
                                            dest_ptr[last_idx++] = static_cast<OffsetType>(bytes);
                                        }
                                        auto strv = string_pool->get_const_view(en.value());
                                        bytes += strv.size();
                                        strings.emplace_back(strv);
                                        if (!populate_inverted_bitset) {
                                            inserter = en.idx();
                                        }
                                    } else if (populate_inverted_bitset) {
                                        inserter = en.idx();
                                    }
                                },
                                positions.first_idx_after_truncation,
                                positions.end_idx_after_truncation
                        );
                        return strings;
                    } else { // fixed-width string type
                        std::vector<std::string> strings;
                        // Experimented with storing a hash map from stringpool offset to UTF-8 strings. Speeds up the
                        // "1 unique string" case by ~40%, but makes the "all strings unique" case ~x3 slower
                        for_each_enumerated<typename source_type_info::TDT>(
                                source_column,
                                [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
                                    while (last_idx <= en.idx()) {
                                        // According to arrow spec offsets must be monotonic even for null values.
                                        // Hence, we fill missing values between `last_idx` and `en.idx` so that
                                        // they contain 0 rows in string buffer
                                        dest_ptr[last_idx++] = static_cast<OffsetType>(bytes);
                                    }
                                    // Fixed-width string columns don't support None/NaN values, so is_a_string check
                                    // not required
                                    auto str = util::utf32_to_u8(string_pool->get_const_view(en.value()));
                                    bytes += str.size();
                                    strings.emplace_back(std::move(str));
                                    if (!populate_inverted_bitset) {
                                        inserter = en.idx();
                                    }
                                },
                                positions.first_idx_after_truncation,
                                positions.end_idx_after_truncation
                        );
                        return strings;
                    }
                } else {
                    util::raise_rte("Unexpected non-string type {} in Arrow string handler");
                }
            }
    );
    while (last_idx <= static_cast<ssize_t>(positions.end_idx_after_truncation.value_or(mapping.num_rows_))) {
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

    handle_truncation(dest_bitset, mapping.truncate_);
    handle_truncation(dest_column, mapping.truncate_);

    if (dest_bitset.count() != dest_bitset.size()) {
        create_dense_bitmap(positions.extra_buffer_position, dest_bitset, dest_column, AllocationType::DETACHABLE);
    } // else there weren't any missing values

    if (dest_bitset.count() > 0) {
        auto& string_buffer = dest_column.create_extra_buffer(
                positions.extra_buffer_position, ExtraBufferType::STRING, bytes, AllocationType::DETACHABLE
        );
        util::variant_match(strings, [&string_buffer](const auto& strings_or_views) {
            auto string_ptr = reinterpret_cast<char*>(string_buffer.data());
            for (auto string_or_view : strings_or_views) {
                memcpy(string_ptr, string_or_view.data(), string_or_view.size());
                string_ptr += string_or_view.size();
            }
        });
    }
}

void encode_dictionary(
        const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
        const std::shared_ptr<StringPool>& string_pool
) {
    // view for dynamic string columns, owning for fixed-width
    struct DictEntryView {
        int32_t offset_buffer_pos_;
        int64_t string_buffer_pos_;
        std::string_view str;
    };
    struct DictEntryOwning {
        int32_t offset_buffer_pos_;
        int64_t string_buffer_pos_;
        std::string str;
    };
    std::vector<StringPool::offset_t> unique_offsets_in_order;
    std::variant<
            ankerl::unordered_dense::map<StringPool::offset_t, DictEntryView>,
            ankerl::unordered_dense::map<StringPool::offset_t, DictEntryOwning>>
            unique_offsets;

    const auto positions = get_positions_after_truncation(mapping);
    const auto row_count_after_truncation = positions.end_idx_after_truncation.value_or(mapping.num_rows_) -
                                            positions.first_idx_after_truncation.value_or(0);
    // Trade some memory for more performance
    // TODO: Use unique count column stat in V2 encoding
    unique_offsets_in_order.reserve(row_count_after_truncation);
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
    unique_offsets = details::visit_type(
            source_column.type().data_type(),
            [&](auto source_tag) -> std::variant<
                                         ankerl::unordered_dense::map<StringPool::offset_t, DictEntryView>,
                                         ankerl::unordered_dense::map<StringPool::offset_t, DictEntryOwning>> {
                using source_type_info = ScalarTypeInfo<decltype(source_tag)>;
                if constexpr (is_sequence_type(source_type_info::data_type)) {
                    if constexpr (is_dynamic_string_type(source_type_info::data_type)) {
                        ankerl::unordered_dense::map<StringPool::offset_t, DictEntryView> unique_offsets_view;
                        unique_offsets_view.reserve(row_count_after_truncation);
                        for_each_enumerated<typename source_type_info::TDT>(
                                source_column,
                                [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
                                    if (is_a_string(en.value())) {
                                        if (auto it = unique_offsets_view.find(en.value());
                                            it == unique_offsets_view.end()) {
                                            auto entry = unique_offsets_view
                                                                 .emplace(
                                                                         en.value(),
                                                                         DictEntryView{
                                                                                 unique_offset_count,
                                                                                 bytes,
                                                                                 string_pool->get_const_view(en.value())
                                                                         }
                                                                 )
                                                                 .first;
                                            bytes += entry->second.str.size();
                                            unique_offsets_in_order.push_back(en.value());
                                            ++unique_offset_count;
                                            dest_ptr[en.idx()] = entry->second.offset_buffer_pos_;
                                        } else {
                                            dest_ptr[en.idx()] = it->second.offset_buffer_pos_;
                                        }
                                        if (!populate_inverted_bitset) {
                                            inserter = en.idx();
                                        }
                                    } else if (populate_inverted_bitset) {
                                        inserter = en.idx();
                                    }
                                },
                                positions.first_idx_after_truncation,
                                positions.end_idx_after_truncation
                        );
                        return unique_offsets_view;
                    } else { // fixed-width string type
                        ankerl::unordered_dense::map<StringPool::offset_t, DictEntryOwning> unique_offsets_owning;
                        unique_offsets_owning.reserve(row_count_after_truncation);
                        for_each_enumerated<typename source_type_info::TDT>(
                                source_column,
                                [&] ARCTICDB_LAMBDA_INLINE(const auto& en) {
                                    // Fixed-width string columns don't support None/NaN values, so is_a_string check
                                    // not required
                                    if (auto it = unique_offsets_owning.find(en.value());
                                        it == unique_offsets_owning.end()) {
                                        auto str = util::utf32_to_u8(string_pool->get_const_view(en.value()));
                                        auto entry = unique_offsets_owning
                                                             .emplace(
                                                                     en.value(),
                                                                     DictEntryOwning{
                                                                             unique_offset_count, bytes, std::move(str)
                                                                     }
                                                             )
                                                             .first;
                                        bytes += entry->second.str.size();
                                        unique_offsets_in_order.push_back(en.value());
                                        ++unique_offset_count;
                                        dest_ptr[en.idx()] = entry->second.offset_buffer_pos_;
                                    } else {
                                        dest_ptr[en.idx()] = it->second.offset_buffer_pos_;
                                    }
                                    if (!populate_inverted_bitset) {
                                        inserter = en.idx();
                                    }
                                },
                                positions.first_idx_after_truncation,
                                positions.end_idx_after_truncation
                        );
                        return unique_offsets_owning;
                    }
                } else {
                    util::raise_rte("Unexpected non-string type {} in Arrow string handler");
                }
            }
    );
    inserter.flush();
    if (populate_inverted_bitset) {
        // For dense columns at this point bitset has ones where the source column is missing values.
        // We invert it back, so we can use it for the sparse map on the output column.
        dest_bitset.invert();
    }
    dest_bitset.resize(mapping.num_rows_);

    handle_truncation(dest_bitset, mapping.truncate_);
    handle_truncation(dest_column, mapping);

    if (dest_bitset.count() != dest_bitset.size()) {
        create_dense_bitmap(positions.extra_buffer_position, dest_bitset, dest_column, AllocationType::DETACHABLE);
    } // else there weren't any missing values

    // bitset.count() == 0 is the special case where all the rows were missing. In this case, do not create
    // the extra string and offset buffers. string_dict_from_block will then do the right thing and call
    // minimal_strings_dict
    if (dest_bitset.count() > 0) {

        auto& string_buffer = dest_column.create_extra_buffer(
                positions.extra_buffer_position, ExtraBufferType::STRING, bytes, AllocationType::DETACHABLE
        );
        auto& offsets_buffer = dest_column.create_extra_buffer(
                positions.extra_buffer_position,
                ExtraBufferType::OFFSET,
                (unique_offsets_in_order.size() + 1) * sizeof(int64_t),
                AllocationType::DETACHABLE
        );
        // Then go through unique_offsets to fill up the offset and string buffers.
        auto offsets_ptr = reinterpret_cast<int64_t*>(offsets_buffer.data());
        auto string_ptr = reinterpret_cast<char*>(string_buffer.data());
        util::variant_match(
                unique_offsets,
                [&unique_offsets_in_order, &offsets_ptr, &string_ptr](auto& unique_offsets) {
                    for (auto unique_offset : unique_offsets_in_order) {
                        const auto& entry = unique_offsets[unique_offset];
                        *offsets_ptr++ = entry.string_buffer_pos_;
                        memcpy(string_ptr, entry.str.data(), entry.str.size());
                        string_ptr += entry.str.size();
                    }
                }
        );
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