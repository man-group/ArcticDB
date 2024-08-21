/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <sparrow/array/array_data.hpp>
#include <sparrow/array.hpp>
#include <sparrow/arrow_interface/array_data_to_arrow_array_converters.hpp>

#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

template <typename T>
struct ArrowTypeMapper {
    using type = T;
};

template <>
struct ArrowTypeMapper<uint8_t> {
    using type = std::uint8_t;
};

template <>
struct ArrowTypeMapper<int8_t> {
    using type = std::int8_t;
};

template <>
struct ArrowTypeMapper<uint16_t> {
    using type = std::uint16_t;
};

template <>
struct ArrowTypeMapper<int16_t> {
    using type = std::int16_t;
};

template <>
struct ArrowTypeMapper<uint32_t> {
    using type = std::uint32_t;
};

template <>
struct ArrowTypeMapper<int32_t> {
    using type = std::int32_t;
};


template <>
struct ArrowTypeMapper<uint64_t> {
    using type = std::uint64_t;
};

template <>
struct ArrowTypeMapper<int64_t> {
    using type = std::int64_t;
};

// Helper alias template for easier use
template <typename T>
using TypeMapper_t = typename ArrowTypeMapper<T>::type;

sparrow::arrow_array_unique_ptr arrow_data_from_column(const Column& column) {
    return column.type().visit_tag([&](auto && impl) -> sparrow::arrow_array_unique_ptr {
        using TagType = std::decay_t<decltype(impl)>;
        using DataType = TagType::DataTypeTag;
        using RawType = DataType::raw_type;
        if constexpr(std::is_same_v<RawType, uint64_t>) {
        //if constexpr (!is_sequence_type(DataType::data_type)) {
            sparrow::array_data data;
            data.type = sparrow::data_descriptor(sparrow::arrow_traits<RawType>::type_id);

            using ArrowType = ArrowTypeMapper<RawType>::type;
            auto column_data = column.data();
            util::check(column_data.num_blocks() == 1, "Expected single block in arrow conversion");
            auto block = column_data.next<TagType>().value();
            sparrow::buffer<RawType> buffer(const_cast<ArrowType*>(block.data()), block.row_count());

            data.buffers.push_back(buffer);
            data.length = static_cast<std::int64_t>(block.row_count());
            data.offset = static_cast<std::int64_t>(0);
            data.child_data.emplace_back();

            return to_arrow_array_unique_ptr(std::move(data));
        } else {
            util::raise_rte("Sequence types not implemented");
        }
    });
};
/*
std::vector<sparrow::arrow_array_unique_ptr> segment_to_arrow_arrays(SegmentInMemory& segment) {
    std::vector<sparrow::arrow_array_unique_ptr> output;
    for(auto& column : segment.column()) {
        output.emplace_back(arrow_data_from_column(column));
    }
}
*/
} // namespace arcticdb