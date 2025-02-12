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
template <class T>
void release_external_common_arrow(T* t)
{
    if (t->dictionary)
    {
        delete t->dictionary;
        t->dictionary = nullptr;
    }

    for (std::int64_t i = 0; i < t->n_children; ++i)
    {
        t->children[i]->release(t->children[i]);
        delete t->children[i];
    }
    delete[] t->children;
    t->children = nullptr;

    t->release = nullptr;
}

template <typename TagType>
sparrow::array arrow_array_from_block(TypedBlockData<TagType>& block) {
    using DataType = typename TagType::DataTypeTag;
    using RawType = typename DataType::raw_type;

    auto* data_ptr = block.release();
    const auto size = block.row_count();

    std::span<const RawType> data_span(
        reinterpret_cast<const RawType*>(data_ptr),
        size
    );

    sparrow::primitive_array<RawType> prim_array(
        data_span,
        std::nullopt,
        std::nullopt
    );

    return sparrow::array{std::move(prim_array)};
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
    const auto num_blocks = segment.num_blocks();
    const auto num_columns = segment.num_columns();

    auto output = std::make_shared<std::vector<sparrow::record_batch>>();
    output->reserve(num_blocks);

    for(auto i = 0UL; i < num_blocks; ++i) {
        output->emplace_back(sparrow::record_batch{});
    }

    for (auto i = 0UL; i < num_columns; ++i) {
        std::vector<sparrow::array> column_arrays;
        auto& column = segment.column(static_cast<position_t>(i));

        if (is_sequence_type(column.type().data_type())) {
            // Handle sequence types if needed
            //output.emplace_back(arrow_data_from_string_column(column, segment.field(i).name()));
        } else {
            arrow_arrays_from_column(column, column_arrays);

            for(auto block_idx = 0UL; block_idx < num_blocks; ++block_idx) {
                (*output)[block_idx].add_column(static_cast<std::string>(segment.field(i).name()), std::move(column_arrays[block_idx]));
            }
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
