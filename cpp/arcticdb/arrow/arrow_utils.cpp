/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/array_from_block.hpp>
#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

#include <span>

namespace arcticdb {

using keys_type = uint32_t;
using layout_type = sparrow::dictionary_encoded_array<keys_type>;

sparrow::nullable<std::string_view> get_dict_value(layout_type::const_reference r) {
    return std::get<sparrow::nullable<std::string_view>>(r);
}



void arrow_arrays_from_column(const Column& column, std::vector<sparrow::array>& vec, std::string_view name) {
    auto column_data = column.data();
    vec.reserve(column.num_blocks());
    column.type().visit_tag([&vec, &column_data, &column, name](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        if constexpr(is_sequence_type(TagType::DataTypeTag::data_type)) {
            while (auto block = column_data.next<TagType>()) {
                const auto offset = block->offset();
                auto& dict_keys = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
                const auto offset_buffer_size = dict_keys.block(0)->bytes() / sizeof(uint32_t);
                sparrow::u8_buffer<uint32_t> offset_buffer(reinterpret_cast<uint32_t *>(dict_keys.block(0)->release()), offset_buffer_size);

                auto& dict_values =  column.get_extra_buffer(offset, ExtraBufferType::STRING);
                const auto data_buffer_size = dict_values.block(0)->bytes();
                sparrow::u8_buffer<char> data_buffer(reinterpret_cast<char*>(dict_values.block(0)->release()), data_buffer_size);

                const auto block_size = block->row_count();
                sparrow::u8_buffer<uint32_t> string_offsets(reinterpret_cast<uint32_t*>(block->release()), block_size);

                sparrow::string_array arrow_dict(
                    std::move(data_buffer),
                    std::move(offset_buffer)
                );
                sparrow::array dict_array(std::move(arrow_dict));

                sparrow::dictionary_encoded_array<uint32_t> dict_encoded(
                    sparrow::dictionary_encoded_array<uint32_t>::keys_buffer_type(std::move(string_offsets)),
                    std::move(dict_array),
                    std::vector<size_t>{}
                );

                vec.emplace_back(std::move(dict_encoded));
            }
        } else {
            while (auto block = column_data.next<TagType>()) {
                vec.emplace_back(arrow_array_from_block<TagType>(*block, name));
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

template<std::ranges::random_access_range R>
auto strided_range(R&& range, size_t stride, size_t start) {
    return std::views::iota(start, std::ranges::size(range) / stride)
        | std::views::transform([&range, stride](auto i) { return range[i * stride]; });
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
ArrowReadResult create_arrow_read_result(
    const VersionedItem& version,
    FrameAndDescriptor&& fd) {
    auto result = std::move(fd);
    auto arrow_frame = ArrowOutputFrame{segment_to_arrow_data(result.frame_), names_from_segment(result.frame_)};

    const auto& desc_proto = result.desc_.proto();
    return {version, std::move(arrow_frame), desc_proto.user_meta()};
}

} // namespace arcticdb
