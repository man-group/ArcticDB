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
                    validity_bitmap
            )};
        } else if constexpr (is_time_type(TagType::DataTypeTag::data_type)) {
            return sparrow::array{create_timestamp_array<RawType>(nullptr, 0, validity_bitmap)};
        } else {
            return sparrow::array{create_primitive_array<RawType>(nullptr, 0, validity_bitmap)};
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
                vec.emplace_back(string_dict_from_block<TagType>(*block, column, name, bitmap));
            } else {
                vec.emplace_back(arrow_array_from_block<TagType>(*block, name, bitmap));
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

} // namespace arcticdb
