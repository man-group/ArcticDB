/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/array_from_block.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

#include <span>

namespace arcticdb {

void arrow_arrays_from_column(const Column& column, std::vector<sparrow::array>& vec, std::string_view name) {
    auto column_data = column.data();
    vec.reserve(column.num_blocks());

    column.type().visit_tag([&vec, &column_data, &column, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        while (auto block = column_data.next<TagType>()) {
            auto bitmap = create_validity_bitmap(block->offset(), column);
            if constexpr(is_sequence_type(TagType::DataTypeTag::data_type)) {
                vec.emplace_back(string_dict_from_block<TagType>(*block, column, name, bitmap));
            } else {
                vec.emplace_back(arrow_array_from_block<TagType>(*block, name, bitmap));
            }
        }
    });
}

std::vector<std::string> names_from_segment(const SegmentInMemory& segment) {
    std::vector<std::string> output;
    output.reserve(segment.fields().size());
    for(auto i = 0UL; i < segment.fields().size(); ++i) {
        const auto ref = segment.field(i).ref();
        output.emplace_back(ref.name_);
    }
    return output;
}
std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment) {
    const auto total_blocks = segment.num_blocks();
    const auto num_columns = segment.num_columns();
    const auto column_blocks = segment.column(0).num_blocks();

    auto output = std::make_shared<std::vector<sparrow::record_batch>>();
    output->reserve(total_blocks);

    for(auto i = 0UL; i < column_blocks; ++i)
        output->emplace_back(sparrow::record_batch{});

    util::check(total_blocks == column_blocks * num_columns, "Expected regular block size");

    for (auto i = 0UL; i < num_columns; ++i) {
        std::vector<sparrow::array> column_arrays;
        auto& column = segment.column(static_cast<position_t>(i));
        util::check(column.num_blocks() == column_blocks, "Non-standard column block number: {} != {}", column.num_blocks(), column_blocks);

        arrow_arrays_from_column(column, column_arrays, segment.field(i).name());

        for(auto block_idx = 0UL; block_idx < column_blocks; ++block_idx) {
            util::check(block_idx < output->size(), "Block index overflow {} > {}", block_idx, output->size());
            (*output)[block_idx].add_column(static_cast<std::string>(segment.field(i).name()), std::move(column_arrays[block_idx]));
        }
    }

    return output;
}

} // namespace arcticdb
