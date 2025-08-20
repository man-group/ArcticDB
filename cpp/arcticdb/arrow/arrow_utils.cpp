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
#include <span>

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
    // TODO: Include string repr of data type in error message
    // TODO: Work out if TIMESTAMP_NANOSECONDS is the right time type
    // TODO: Add support for other time types
    // TODO: Add support for strings
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
    case sparrow::data_type::TIMESTAMP_NANOSECONDS: return DataType::NANOSECONDS_UTC64;
    default: schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>("Unsupported Arrow data type provided");
    }
}

SegmentInMemory arrow_data_to_segment(const std::vector<sparrow::record_batch>& record_batches) {
    // TODO: See if it is possible to produce something with zero record batches in Python
    // Probably same as pandas empty df case, where the write/append/update should be a no-op and return the previous
    // versioned item, although if no previous versions might be more ambiguous
    util::check(!record_batches.empty(), "Zero record batches provided");
    auto record_batch = record_batches.cbegin();
    auto num_columns = record_batch->nb_columns();
    auto column_names = record_batch->names();
    std::vector<DataType> data_types;
    std::vector<ChunkedBuffer> chunked_buffers(num_columns);
    data_types.reserve(num_columns);
    for (size_t idx = 0; idx < num_columns; ++idx) {
        const auto& array = record_batch->get_column(idx);
        const auto arrow_data_type = array.data_type();
        const auto arcticdb_data_type = arcticdb_type_from_arrow_type(arrow_data_type);
        data_types.emplace_back(arcticdb_data_type);
    }
    uint64_t total_rows{0};;
    for (; record_batch != record_batches.cend(); ++record_batch) {
        // Could skip this check on first iteration
        util::check(std::ranges::equal(column_names, record_batch->names()),
                "Record batches do not contain the same column names: {} != {}", column_names, record_batch->names());
        std::optional<size_t> num_rows;
        for (size_t idx = 0; idx < num_columns; ++idx) {
            const auto& array = record_batch->get_column(idx);
            const auto arrow_data_type = array.data_type();
            const auto arcticdb_data_type = arcticdb_type_from_arrow_type(arrow_data_type);
            util::check(arcticdb_data_type == data_types.at(idx), "Mismatching column types in record batches {} != {}",
                        arcticdb_data_type, data_types.at(idx));
            if (num_rows.has_value()) {
                util::check(*num_rows == array.size(), "Mismatched number of rows in record batch {} != {}", *num_rows, array.size());
            } else {
                num_rows = array.size();
            }
            // TODO: Remove const-cast when sparrow github issue #528 fixed
            auto arrow_structures = sparrow::get_arrow_structures(const_cast<sparrow::array&>(array));
            auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_structures.first, *arrow_structures.second);
            // arrow_array_buffers.at(1) seems to be the validity bitmap, and may be NULL if there are no null values
            // need to handle it being non-NULL and all 1s though
            const auto* data = arrow_array_buffers.at(1).data<uint8_t>();
            const auto& block_offsets = chunked_buffers.at(idx).block_offsets();
            const auto bytes = *num_rows * get_type_size(data_types.at(idx));
            const auto offset = block_offsets.empty() ? 0 : block_offsets.back();
            chunked_buffers.at(idx).add_external_block(data, bytes, offset);
        }
        total_rows += *num_rows;
    }
    SegmentInMemory seg;
    for (size_t idx = 0; idx < num_columns; ++idx) {
        // The Arrow data may be semantically sparse, but this buffer is still dense, hence Sparsity::NOT_PERMITTED
        seg.add_column(scalar_field(data_types.at(idx), column_names[idx]),
                       std::make_shared<Column>(make_scalar_type(data_types.at(idx)), Sparsity::NOT_PERMITTED, std::move(chunked_buffers.at(idx))));
    }
    seg.set_row_data(static_cast<ssize_t>(total_rows) - 1);
    return seg;
}

} // namespace arcticdb
