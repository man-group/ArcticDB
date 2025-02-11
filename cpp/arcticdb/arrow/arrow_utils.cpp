/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

#include <span>

namespace arcticdb {


template <typename TagType>
sparrow::array arrow_array_from_block(TypedBlockData<TagType>& block) {
    using DataType = TagType::DataTypeTag;
    using RawType = DataType::raw_type;
    auto *tmp = const_cast<RawType*>(block.release());
    sparrow::primitive_array<RawType> raw_array(std::span(tmp, block.row_count()), std::nullopt, std::nullopt);
    return sparrow::array{std::move(raw_array)};
}


void arrow_arrays_from_column(const Column& column, std::vector<sparrow::array>& vec) {
    auto column_data = column.data();
    vec.reserve(column.num_blocks());
    column.type().visit_tag([&vec, &column_data](auto &&impl) {
        using TagType = std::decay_t<decltype(impl)>;
        while (auto block = column_data.next<TagType>()) {
            vec.emplace_back(arrow_array_from_block<TagType>(*block));
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
    std::vector<sparrow::array> arrays;
    const auto num_blocks = segment.num_blocks();
    const auto num_columns = segment.num_columns();
    arrays.reserve(segment.num_columns() * num_blocks);

    for (auto i = 0UL; i < num_columns; ++i) {
        auto& column = segment.column(static_cast<position_t>(i));
        if (is_sequence_type(column.type().data_type())) {
            //output.emplace_back(arrow_data_from_string_column(column, segment.field(i).name()));
        } else {
            arrow_arrays_from_column(segment.column(static_cast<position_t>(i)), arrays);
        }
    }

    auto output = std::make_shared<std::vector<sparrow::record_batch>>();
    output->reserve(num_columns);

    for(auto i = 0UL; i < num_blocks; ++i) {
        auto batch_data = strided_range(std::span(arrays), num_columns, i);
        output->emplace_back(sparrow::record_batch(names_from_segment(segment), std::move(batch_data)));
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
