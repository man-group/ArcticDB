#pragma once

#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

namespace arcticdb {
template <typename TagType>
sparrow::array arrow_array_from_block(TypedBlockData<TagType>& block, std::string_view name) {
    using DataTagType = typename TagType::DataTypeTag;
    using RawType = typename DataTagType::raw_type;
        auto *data_ptr = block.release();
        const auto data_size = block.row_count();

        sparrow::u8_buffer<RawType> buffer(data_ptr, data_size);
        sparrow::primitive_array<RawType> primitive_array{std::move(buffer), name};
        return sparrow::array{std::move(primitive_array)};
}


}