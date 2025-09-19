/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/array_from_block.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <sparrow/record_batch.hpp>

namespace arcticdb {

sparrow::array empty_arrow_array_from_type(const TypeDescriptor& type, std::string_view name) {
    auto res = type.visit_tag([](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        using DataTagType = typename TagType::DataTypeTag;
        using RawType = typename DataTagType::raw_type;
        std::optional<sparrow::validity_bitmap> validity_bitmap;
        if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
            sparrow::u8_buffer<int32_t> dict_keys_buffer{nullptr, 0};
            auto dict_values_array = minimal_strings_dict();
            return sparrow::array{
                create_dict_array<int32_t>(
                    sparrow::array{std::move(dict_values_array)},
                    std::move(dict_keys_buffer),
                    std::move(validity_bitmap)
            )};
        } else if constexpr (is_time_type(TagType::DataTypeTag::data_type)) {
            return sparrow::array{create_timestamp_array<RawType>(nullptr, 0, std::move(validity_bitmap))};
        } else {
            return sparrow::array{create_primitive_array<RawType>(nullptr, 0, std::move(validity_bitmap))};
        }
    });
    res.set_name(name);
    return res;
}

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name) {
    std::vector<sparrow::array> vec;
    auto column_data = column.data();
    vec.reserve(column.num_blocks());
    column.type().visit_tag([&vec, &column_data, &column, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        if (column_data.num_blocks() == 0) {
            // For empty columns we want to return one empty array instead of no arrays.
            vec.emplace_back(empty_arrow_array_from_type(column.type(), name));
        }
        while (auto block = column_data.next<TagType>()) {
            auto bitmap = create_validity_bitmap(block->offset(), column, block->row_count());
            if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
                vec.emplace_back(string_dict_from_block<TagType>(*block, column, name, std::move(bitmap)));
            } else {
                vec.emplace_back(arrow_array_from_block<TagType>(*block, name, std::move(bitmap)));
            }
        }
    });
    return vec;
}

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment) {
    const auto total_blocks = segment.num_blocks();
    const auto num_columns = segment.num_columns();
    // Zero columns implies zero record batches were written, this just allows us to roundtrip zero record batch tables
    if (num_columns == 0) {
        return std::make_shared<std::vector<sparrow::record_batch>>();
    }
    const auto column_blocks = segment.column(0).num_blocks();
    util::check(total_blocks == column_blocks * num_columns, "Expected regular block size");

    // column_blocks == 0 is a special case where we are returning a zero-row structure (e.g. if date_range is
    // provided outside of the time range covered by the symbol)
    auto output = std::make_shared<std::vector<sparrow::record_batch>>(column_blocks == 0 ? 1 : column_blocks, sparrow::record_batch{});
    for (auto i = 0UL; i < num_columns; ++i) {
        auto& column = segment.column(static_cast<position_t>(i));
        util::check(column.num_blocks() == column_blocks, "Non-standard column block number: {} != {}", column.num_blocks(), column_blocks);

        auto column_arrays = arrow_arrays_from_column(column, segment.field(i).name());
        util::check(column_arrays.size() == output->size(), "Unexpected number of arrow arrays returned: {} != {}", column_arrays.size(), output->size());

        for (auto block_idx = 0UL; block_idx < column_arrays.size(); ++block_idx) {
            util::check(block_idx < output->size(), "Block index overflow {} > {}", block_idx, output->size());
            (*output)[block_idx].add_column(static_cast<std::string>(segment.field(i).name()),
                                            std::move(column_arrays[block_idx]));
        }
    }
    return output;
}

DataType arcticdb_type_from_arrow_type(sparrow::data_type arrow_type) {
    switch (arrow_type) {
    case sparrow::data_type::BOOL: return DataType::BOOL8;
    case sparrow::data_type::UINT8: return DataType::UINT8;
    case sparrow::data_type::UINT16: return DataType::UINT16;
    case sparrow::data_type::UINT32: return DataType::UINT32;
    case sparrow::data_type::UINT64: return DataType::UINT64;
    case sparrow::data_type::INT8: return DataType::INT8;
    case sparrow::data_type::INT16: return DataType::INT16;
    case sparrow::data_type::INT32: return DataType::INT32;
    case sparrow::data_type::INT64: return DataType::INT64;
    case sparrow::data_type::FLOAT: return DataType::FLOAT32;
    case sparrow::data_type::DOUBLE: return DataType::FLOAT64;
    case sparrow::data_type::TIMESTAMP_SECONDS: return DataType::SECONDS_UTC64;
    case sparrow::data_type::TIMESTAMP_MILLISECONDS: return DataType::MILLISECONDS_UTC64;
    case sparrow::data_type::TIMESTAMP_MICROSECONDS: return DataType::MICROSECONDS_UTC64;
    case sparrow::data_type::TIMESTAMP_NANOSECONDS: return DataType::NANOSECONDS_UTC64;
    default: schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>("Unsupported Arrow data type provided `{}`", std::format("{}", arrow_type));
    }
}

std::pair<std::vector<DataType>, std::optional<size_t>> find_data_types_and_index_position(const sparrow::record_batch& record_batch,
                                                                                           const std::optional<std::string>& index_name) {
    std::vector<DataType> data_types;
    data_types.reserve(record_batch.nb_columns());
    std::optional<size_t> index_column_position;
    for (size_t idx = 0; idx < record_batch.nb_columns(); ++idx) {
        const auto& array = record_batch.get_column(idx);
        if (index_name.has_value() && array.name().has_value() && *array.name() == *index_name) {
            index_column_position = idx;
        }
        data_types.emplace_back(arcticdb_type_from_arrow_type(array.data_type()));
    }
    if (index_name.has_value()) {
        schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(index_column_position.has_value(),
                                                        "Specified index column named '{}' not present in data", *index_name);
    }
    return {std::move(data_types), index_column_position};
}

std::pair<SegmentInMemory, std::optional<size_t>> arrow_data_to_segment(const std::vector<sparrow::record_batch>& record_batches,
                                                                        const std::optional<std::string>& index_name) {
    SegmentInMemory seg;
    if (record_batches.empty()) {
        return {seg, std::nullopt};
    }
    auto record_batch = record_batches.cbegin();
    auto column_names = record_batch->names();
    auto [data_types, index_column_position] = find_data_types_and_index_position(*record_batch, index_name);
    uint64_t total_rows = std::accumulate(record_batches.cbegin(), record_batches.cend(), uint64_t(0),[](const uint64_t& accum, const sparrow::record_batch& record_batch) {
        return accum + record_batch.nb_rows();
    });
    std::vector<ChunkedBuffer> chunked_buffers(record_batch->nb_columns());
    uint64_t start_row{0};
    for (; record_batch != record_batches.cend(); ++record_batch) {
        for (size_t idx = 0; idx < record_batch->nb_columns(); ++idx) {
            const auto& array = record_batch->get_column(idx);
            auto [arrow_array, arrow_schema] = sparrow::get_arrow_structures(array);
            auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_array, *arrow_schema);
            // arrow_array_buffers.at(0) seems to be the validity bitmap, and may be NULL if there are no null values
            // need to handle it being non-NULL and all 1s though
            const auto* data = arrow_array_buffers.at(1).data<uint8_t>();
            if (is_bool_type(data_types.at(idx))) {
                // Arrow bool columns are packed bitsets
                // It would be more idiomatic if this unpacking happened in WriteToSegmentTask, which would also naturally
                // parallelise the process, but this is quite complicated, particularly when there are multiple record batches.
                // This approach is much simpler, and BM_packed_bits_to_buffer show a rate of 10 billion rows per second
                // handled in serial. If this proves to be a bottleneck in some situations, it will probably still be
                // easier to do the unpacking here in parallel than to try and push this down to WriteToSegmentTask
                if (record_batch == record_batches.cbegin()) {
                    chunked_buffers.at(idx).ensure(total_rows);
                }
                packed_bits_to_buffer(data, array.size(), chunked_buffers.at(idx).bytes_at(start_row, array.size()));
            } else {
                const auto bytes = array.size() * get_type_size(data_types.at(idx));
                chunked_buffers.at(idx).add_external_block(data, bytes);
            }
        }
        start_row += record_batch->nb_rows();
    }
    if (index_column_position.has_value()) {
        auto idx = *index_column_position;
        seg.add_column(scalar_field(data_types.at(idx), column_names[idx]),
                       std::make_shared<Column>(make_scalar_type(data_types.at(idx)), Sparsity::NOT_PERMITTED, std::move(chunked_buffers.at(idx))));
    }
    for (size_t idx = 0; idx < column_names.size(); ++idx) {
        if (!index_column_position.has_value() || idx != *index_column_position) {
            // The Arrow data may be semantically sparse, but this buffer is still dense, hence Sparsity::NOT_PERMITTED
            seg.add_column(scalar_field(data_types.at(idx), column_names[idx]),
                           std::make_shared<Column>(make_scalar_type(data_types.at(idx)), Sparsity::NOT_PERMITTED, std::move(chunked_buffers.at(idx))));
        }
    }
    seg.set_row_data(static_cast<ssize_t>(total_rows) - 1);
    return {seg, index_column_position};
}

} // namespace arcticdb
